/*
 * Modified by ScyllaDB
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/gate.hh>
#include "db/config.hh"
#include "db/hints/manager.hh"
#include "seastarx.hh"
#include "converting_mutation_partition_applier.hh"
#include "disk-error-handler.hh"
#include "lister.hh"

namespace db {
namespace hints {

static logging::logger manager_logger("hints_manager");
const std::string manager::FILENAME_PREFIX("HintsLog" + commitlog::descriptor::SEPARATOR);

const std::chrono::seconds manager::hint_file_write_timeout = std::chrono::seconds(2);
const std::chrono::seconds manager::hints_timer_period = std::chrono::seconds(10);

manager::manager(sstring hints_directory, std::vector<sstring> hinted_dcs, int64_t max_hint_window_ms, distributed<database>& db)
    : _hints_dir(boost::filesystem::path(hints_directory) / format("{:d}", engine().cpu_id()).c_str())
    , _hinted_dcs(hinted_dcs.begin(), hinted_dcs.end())
    , _timer([this] { on_timer(); })
    , _local_snitch_ptr(locator::i_endpoint_snitch::get_local_snitch_ptr())
    , _max_hint_window_us(max_hint_window_ms * 1000)
    , _local_db(db.local())
    , _buckets(_initial_buckets_count)
    , _ep_managers(bi_set_bucket_traits(_buckets.data(), _buckets.size()))
{
    namespace sm = seastar::metrics;

    _metrics.add_group("hints_manager", {
        sm::make_gauge("total_hints_in_progress", _stats.total_hints_in_progress,
                        sm::description("Number of hints that are scheduled to be written.")),

        sm::make_derive("written", _stats.written,
                        sm::description("Number of successfully written hints.")),

        sm::make_derive("errors", _stats.errors,
                        sm::description("Number of errors during hints writes.")),

        sm::make_derive("dropped", _stats.dropped,
                        sm::description("Number of dropped hints.")),

        sm::make_derive("sent", _stats.sent,
                        sm::description("Number of sent hints.")),
    });
}

manager::~manager() {
    assert(_ep_managers.empty());
}

future<> manager::create(sstring hints_dir, std::vector<sstring> hinted_dcs, int64_t max_hint_window_ms, distributed<database>& db) {
    manager_logger.trace("hinted DCs: {}", hinted_dcs);
    return manager_instance().start(std::move(hints_dir), std::move(hinted_dcs), max_hint_window_ms, std::ref(db));
}

future<> manager::start() {
    return rebalance().then([] {
        return manager_instance().invoke_on_all([] (manager& local_manager) {
            return local_manager.start_one();
        });
    });
}

future<> manager::start_one() {
    _proxy_anchor = service::get_local_storage_proxy().shared_from_this();
    _gossiper_anchor = gms::get_local_gossiper().shared_from_this();
    return lister::scan_dir(_hints_dir, { directory_entry_type::directory }, [this] (lister::path datadir, directory_entry de) {
        ep_key_type ep = ep_key_type(de.name);
        if (!check_dc_for(ep)) {
            return make_ready_future<>();
        }
        return get_ep_manager(ep).populate_segments_to_replay();
    }).then([this] {
        // we are ready to store new hints...
        _proxy_anchor->allow_hints();
        _timer.arm(timer_clock_type::now());
    });
}

future<> manager::stop() {
    manager_logger.info("Asked to stop");
    if (_proxy_anchor) {
        _proxy_anchor->forbid_hints();
    }
    return _hints_timer_gate.close().then([this] {
        _timer.cancel();
        return _hints_write_gate.close().then([this] {
            return parallel_for_each(_ep_managers, [] (end_point_hints_manager& ep_map) {
                return ep_map.stop();
            });
        }).then([this] {
            _ep_managers.clear_and_dispose(destroy_ep_manager);
            manager_logger.info("Stopped");
        });
    });
}

future<> manager::end_point_hints_manager::store_hint(schema_ptr s, lw_shared_ptr<const frozen_mutation> fm) {
    return with_shared(_write_mutex, [this, fm = std::move(fm), s = std::move(s)] () mutable -> future<> {
        return get_or_load().then([this, fm = std::move(fm), s = std::move(s)] (hints_store_ptr log_ptr) mutable {
            ++_hints_in_progress;
            commitlog_entry_writer cew(s, *fm);
            return log_ptr->add_entry(s->id(), cew, commitlog::timeout_clock::now() + _shard_manager.hint_file_write_timeout);
        }).then([] (db::rp_handle rh) {
            rh.release();
            return make_ready_future<>();
        }).finally([this] {
            --_hints_in_progress;
        });
    });
}

future<> manager::end_point_hints_manager::populate_segments_to_replay() {
    return with_lock(_write_mutex, [this] {
        // this will move all existing hints files to the "upload" and populate the _segments_to_replay list
        return get_or_load().discard_result();
    });
}

future<> manager::end_point_hints_manager::stop() noexcept {
    auto func = [this] {
        return with_lock(_write_mutex, [this] {
            if (_hints_store_anchor) {
                hints_store_ptr tmp = std::move(_hints_store_anchor);
                _hints_store_anchor = nullptr;
                return tmp->shutdown().finally([tmp] {});
            }
            return make_ready_future<>();
        });
    };
    _stopping = true;
    return futurize_apply(std::move(func));
}

manager::end_point_hints_manager::end_point_hints_manager(const key_type& key, manager& shard_manager)
    : _key(key)
    , _shard_manager(shard_manager)
    , _db(_shard_manager.local_db())
    , _proxy(_shard_manager.local_storage_proxy())
    , _gossiper(_shard_manager.local_gossiper())
    , _hints_dir(_shard_manager.hints_dir() / format("{}", _key).c_str())
    , _upload_dir(_hints_dir / "upload")
{}

future<hints_store_ptr> manager::end_point_hints_manager::get_or_load() {
    if (!_hints_store_anchor) {
        return _shard_manager.store_factory().get_or_load(_key, [this] (const key_type&) noexcept {
            return add_store();
        }).then([this] (hints_store_ptr log_ptr) {
            _hints_store_anchor = log_ptr;
            return make_ready_future<hints_store_ptr>(std::move(log_ptr));
        });
    }

    return make_ready_future<hints_store_ptr>(_hints_store_anchor);
}

manager::end_point_hints_manager& manager::get_ep_manager(ep_key_type ep) {
    auto it = _ep_managers.find(ep, std::hash<ep_key_type>(), typename end_point_hints_manager::key_eq());
    if (it == _ep_managers.end()) {
        end_point_hints_manager* new_ep_man = new end_point_hints_manager(ep, *this);
        rehash_before_insert();
        _ep_managers.insert(*new_ep_man);
        return *new_ep_man;
    }
    return *it;
}

inline bool manager::have_ep_manager(ep_key_type ep) const noexcept {
    return _ep_managers.find(ep, std::hash<ep_key_type>(), typename end_point_hints_manager::key_eq()) != _ep_managers.end();
}

bool manager::store_hint(ep_key_type ep, schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept {
    if (!can_hint_for(ep)) {
        manager_logger.trace("Can't store a hint to {}", ep);
        ++_stats.dropped;

        return false;
    }

    manager_logger.trace("Going to store a hint to {}", ep);
    tracing::trace(tr_state, "Going to store a hint to {}", ep);
    try {
        with_gate(_hints_write_gate, [this, ep, s, fm, tr_state] () mutable -> future<> {
            ++_stats.total_hints_in_progress;
            return get_ep_manager(ep).store_hint(std::move(s), std::move(fm)).then([this, tr_state = std::move(tr_state), ep] {
                manager_logger.trace("Hint to {} was stored", ep);
                tracing::trace(tr_state, "Hint to {} was stored", ep);
                ++_stats.written;
            });
        }).handle_exception([this, tr_state, ep] (auto eptr) {
            try {
                std::rethrow_exception(eptr);
                ++_stats.errors;
            } catch (std::exception& e) {
                manager_logger.debug("Got exception: {}", e.what());
                tracing::trace(tr_state, "Failed to store a hint to {}: {}", ep, e.what());
            } catch (...) {
                manager_logger.debug("Got unknown exception");
                tracing::trace(tr_state, "Failed to store a hint to {}: unknown exception", ep);
            }
        }).finally([s, fm, this] {
            --_stats.total_hints_in_progress;
            manager_logger.trace("store_hint() - complete");
        });
    } catch (seastar::gate_closed_exception& e) {
        manager_logger.trace("Failed to store a hint to {}: write gate is closed", ep);
        tracing::trace(tr_state, "Failed to store a hint to {}: write gate is closed", ep);
        ++_stats.dropped;

        return false;
    } catch (...) {
        manager_logger.trace("Failed to store a hint to {}: unexpected exception", ep);
        tracing::trace(tr_state, "Failed to store a hint to {}: unexpected exception", ep);
        ++_stats.errors;

        return false;
    }

    return true;
}

future<db::commitlog> manager::end_point_hints_manager::add_store() noexcept {
    using namespace boost::filesystem;
    manager_logger.trace("Going to add a store to {}", _hints_dir.c_str());

    return io_check(recursive_touch_directory, _upload_dir.c_str()).then([this] () {
        lw_shared_ptr<commitlog::config> cfg_ptr = make_lw_shared<commitlog::config>();

        cfg_ptr->commit_log_location = _hints_dir.c_str();
        cfg_ptr->commitlog_total_space_in_mb = _max_hints_per_ep_size_mb;
        cfg_ptr->fname_prefix = manager::FILENAME_PREFIX;

        return commitlog::create_commitlog(*cfg_ptr, false).then([this, cfg_ptr] (commitlog l) {
            if (!_segments_to_replay.empty()) {
                return make_ready_future<commitlog>(std::move(l));
            }

            // Move all pre-existing segments to "upload" directory
            std::vector<sstring> segs(l.get_segments_to_replay());
            return parallel_for_each(segs, [this] (sstring& seg_path){
                path old_seg_path(seg_path);
                path new_seg_path(_upload_dir / old_seg_path.filename());

                manager_logger.trace("mv {} -> {}", old_seg_path.c_str(), new_seg_path.c_str());

                return io_check(rename_file, old_seg_path.c_str(), new_seg_path.c_str());
            }).then([this, l = std::move(l), cfg_ptr = std::move(cfg_ptr)] () mutable {
                // Create a helper commitlog object to enumerate the segments in the "upload" sub-directory. We don't use the
                // lister::scan_dir() for that because we want the segments to be listed in a particular order only commitlog
                // "knows" - the order from the oldest segment to the newest.
                cfg_ptr->commit_log_location = _upload_dir.c_str();

                return commitlog::create_commitlog(*cfg_ptr, false).then([this, l = std::move(l)] (commitlog upload_cl) mutable {
                    std::vector<sstring> segs_vec = upload_cl.get_segments_to_replay();

                    std::for_each(segs_vec.begin(), segs_vec.end(), [this] (sstring& seg) {
                        _segments_to_replay.emplace_back(std::move(seg));
                    });

                    // shutdown the helper commitlog object
                    return do_with(std::move(upload_cl), [l = std::move(l)] (commitlog& upload_cl) mutable {
                        return upload_cl.shutdown().then([l = std::move(l)] () mutable  {
                            return make_ready_future<commitlog>(std::move(l));
                        });
                    });
                });
                return make_ready_future<commitlog>(std::move(l));
            });
        });
    });
}

future<> manager::end_point_hints_manager::flush_current_hints() {
    // flush the currently created hints to disk
    if (_hints_store_anchor) {
        return with_lock(_write_mutex, [this] () -> future<> {
            return get_or_load().then([] (hints_store_ptr cptr) {
                return cptr->shutdown();
            }).then([this] {
                // Un-hold the commitlog object. Since we are under the exclusive _write_mutex lock there are no
                // other hints_store_ptr copies and this would destroy the commitlog shared value.
                _hints_store_anchor = nullptr;

                // Re-create the commitlog instance - this will re-populate the _segments_to_replay if needed.
                return get_or_load().discard_result();
            });
        });
    }

    return make_ready_future<>();
}

class host_is_down_error : public std::exception {};
class sending_for_too_long_error : public std::exception {};
class stop_called_error : public std::exception {};
class no_column_mapping : public std::exception {
private:
    sstring _msg;

public:
    no_column_mapping(const utils::UUID& id) : _msg(format("column mapping for CF {} is missing", id)) {}
    const char* what() const noexcept override {
        return _msg.c_str();
    }
};

// TODO: rename send_hints() -> on_timer(), send_hints() -> on_timer()
future<> manager::end_point_hints_manager::on_timer() {
    return flush_current_hints().then([this] {
        if (_gossiper.is_alive(end_point_key())) {
            return send_hints();
        } else {
            manager_logger.trace("{} is still DOWN", end_point_key());
            return make_ready_future<>();
        }
    });
}

future<timespec> manager::end_point_hints_manager::get_last_file_modification(const sstring& fname) {
    return open_file_dma(fname, open_flags::ro).then([] (file f) {
        return do_with(std::move(f), [] (file& f) {
            return f.stat();
        });
    }).then([] (struct stat st) {
        return make_ready_future<timespec>(st.st_mtim);
    });
}

future<> manager::end_point_hints_manager::send_hints() {
    manager_logger.trace("Going to send hints to {}, we have {} segment to replay", end_point_key(), _segments_to_replay.size());

    _replayed_segments_count = 0;
    timer_clock_type::time_point sending_began_at = timer_clock_type::now();

    return do_for_each(_segments_to_replay, [this, sending_began_at] (sstring& fname) {
        return get_last_file_modification(fname).then([this, &fname, sending_began_at] (timespec last_mod) {
            return commitlog::read_log_file(fname, [this, last_mod, &fname] (temporary_buffer<char> buf, db::replay_position rp) {

                // Check that the destination is still UP
                if (!_gossiper.is_alive(end_point_key())) {
                    return make_exception_future(host_is_down_error());
                }

                try {
                    mutation m = _shard_manager.get_mutation(buf);
                    auto& keyspace_name = m.schema()->ks_name();
                    keyspace& ks = _db.find_keyspace(keyspace_name);
                    auto& rs = ks.get_replication_strategy();
                    std::vector<gms::inet_address> natural_endpoints = rs.get_natural_endpoints(m.token());

                    gc_clock::duration gc_grace_sec = m.schema()->gc_grace_seconds();
                    gc_clock::duration secs_since_file_mod = std::chrono::seconds(last_mod.tv_sec);

                    // The hint is too old - drop it.
                    //
                    // Files are aggregated for at most manager::hints_timer_period therefore the oldest hint there is
                    // (last_modification - manager::hints_timer_period) old.
                    if (gc_clock::now().time_since_epoch() - secs_since_file_mod > gc_grace_sec - manager::hints_timer_period) {
                        return make_ready_future<>();
                    }

                    // Break early if stop() was called - we will re-send this segment next time.
                    if (_stopping) {
                        return make_exception_future(stop_called_error());
                    }

                    ++_shard_manager._stats.sent;

                    if (boost::range::find(natural_endpoints, end_point_key()) != natural_endpoints.end()) {
                        manager_logger.trace("Sending directly to {}", end_point_key());
                        return _proxy.send_to_endpoint(std::move(m), end_point_key(), write_type::SIMPLE);
                    } else {
                        manager_logger.trace("Endpoints set has changed and {} is no longer a replica. Mutating from scratch...", end_point_key());
                        return _proxy.mutate({std::move(m)}, consistency_level::ANY, nullptr);
                    }
                // ignore these errors and move on - probably this hint is too old and the KS/CF has been deleted...
                } catch (no_such_column_family& e) {
                    manager_logger.debug("no_such_column_family: {}", e.what());
                } catch (no_such_keyspace& e) {
                    manager_logger.debug("no_such_keyspace: {}", e.what());
                } catch (no_column_mapping& e) {
                    manager_logger.debug("{}: {}", fname, e.what());
                }

                return make_ready_future<>();
            }).then([] (auto s) {
                return do_with(std::move(s), [](auto& s) {
                    return s->done();
                });
            }).then([&fname] {
                return io_check(remove_file, fname);
            }).then([this, &fname, sending_began_at] {
                ++_replayed_segments_count;
                manager_logger.trace("Segment {} was sent in full and deleted", fname);

                // Check that we are not sending for too long. If we have too many hints to send we want to let the shard manager
                // timer to restart to let it send hints to other end points too.
                // Let's not preempt in the middle of the file however.
                if (timer_clock_type::now() > sending_began_at + hints_timer_period) {
                    manager_logger.trace("Sent for too long - preempting", fname);
                    return make_exception_future(sending_for_too_long_error());
                }

                return make_ready_future<>();
            });
        });
    }).handle_exception([] (auto eptr) {
        // ignore the exceptions, we will re-try to send this file the next time...
    }).finally([this] {
        manager_logger.trace("We handled {} segments", _replayed_segments_count);

        if (_replayed_segments_count == 0) {
            return;
        }

        auto start = _segments_to_replay.begin();
        auto end = std::next(start, _replayed_segments_count);
        _segments_to_replay.erase(start, end);
    });
}

mutation manager::get_mutation(temporary_buffer<char>& buf) {
    hint_entry_reader hr(buf);
    auto& fm = hr.mutation();
    auto& cm = get_column_mapping(fm, hr);
    auto& cf = _local_db.find_column_family(fm.column_family_id());

    if (cf.schema()->version() != fm.schema_version()) {
        mutation m(fm.decorated_key(*cf.schema()), cf.schema());
        converting_mutation_partition_applier v(cm, *cf.schema(), m.partition());
        fm.partition().accept(cm, v);

        return std::move(m);
    } else {
        return fm.unfreeze(cf.schema());
    }
}

const column_mapping& manager::get_column_mapping(const frozen_mutation& fm, const hint_entry_reader& hr) {
    auto cm_it = _schema_ver_to_column_mapping.find(fm.schema_version());
    if (cm_it == _schema_ver_to_column_mapping.end()) {
        if (!hr.get_column_mapping()) {
            throw no_column_mapping(fm.schema_version());
        }

        manager_logger.debug("new schema version {}", fm.schema_version());
        cm_it = _schema_ver_to_column_mapping.emplace(fm.schema_version(), *std::move(hr.get_column_mapping())).first;
    }

    return cm_it->second;
}

void manager::on_timer() {
    manager_logger.trace("on_timer: start");
    auto timer_start_tp = timer_clock_type::now();
    with_gate(_hints_timer_gate, [this, timer_start_tp] {
        _schema_ver_to_column_mapping.clear();
        return parallel_for_each(_ep_managers, [this] (end_point_hints_manager& ep_manager) {
            return ep_manager.on_timer();
        }).finally([this, timer_start_tp] {
            _timer.arm(timer_start_tp + hints_timer_period);
        });
    });
}

void manager::rehash_before_insert() noexcept {
    try {
        size_t new_buckets_count = 0;
        size_t new_size = _ep_managers.size() + 1;

        // Try to keep the load factor below 0.75.
        if (new_size > 3 * buckets_count() / 4) {
            new_buckets_count = buckets_count() * 2;
        }

        if (new_buckets_count < _initial_buckets_count) {
            return;
        }

        std::vector<typename ep_managers_set_type::bucket_type> new_buckets(new_buckets_count);
        _ep_managers.rehash(bi_set_bucket_traits(new_buckets.data(), new_buckets.size()));
        _buckets.swap(new_buckets);
    } catch (...) {
        // if rehashing fails - continue with the current buckets array
    }
}

bool manager::too_many_in_flight_hints_for(ep_key_type ep) const noexcept {
    // There is no need to check the DC here because if there is an in-flight hint for this end point then this means that
    // its DC has already been checked and found to be ok.
    return _stats.total_hints_in_progress > _max_hints_in_progress && hints_in_progress_for(ep) > 0 && _gossiper_anchor->get_endpoint_downtime(ep) <= _max_hint_window_us;
}

bool manager::can_hint_for(ep_key_type ep) const noexcept {
    // Don't allow more than one in-flight (to the store) hint to a specific destination when the total amount of in-flight
    // hints is more than the maximum allowed value.
    //
    // In the worst case there's going to be (_max_hints_in_progress + N - 1) in-flight hints, where N is the total number Nodes in the cluster.
    if (_stats.total_hints_in_progress > _max_hints_in_progress && hints_in_progress_for(ep) > 0) {
        manager_logger.trace("total_hints_in_progress {} hints_in_progress_for({}) {}", _stats.total_hints_in_progress, ep, hints_in_progress_for({}));
        return false;
    }

    // check that the destination DC is "hintable"
    if (!check_dc_for(ep)) {
        manager_logger.trace("{}'s DC is not hintable", ep);
        return false;
    }

    // check if the end point has been down for too long
    if (_gossiper_anchor->get_endpoint_downtime(ep) > _max_hint_window_us) {
        manager_logger.trace("{} is down for {}, not hinting", ep, _gossiper_anchor->get_endpoint_downtime(ep));
        return false;
    }

    return true;
}

bool manager::check_dc_for(ep_key_type ep) const noexcept {
    try {
        // If target's DC is not a "hintable" DCs - don't hint.
        // If there is an end point manager then DC has already been checked and found to be ok.
        return _hinted_dcs.empty() || have_ep_manager(ep) ||
               _hinted_dcs.find(_local_snitch_ptr->get_datacenter(ep)) != _hinted_dcs.end();
    } catch (...) {
        // if we failed to check the DC - block this hint
        return false;
    }
}

}
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "partition_range_compat.hh"
#include "db/consistency_level.hh"
#include "db/commitlog/commitlog.hh"
#include "storage_proxy.hh"
#include "unimplemented.hh"
#include "frozen_mutation.hh"
#include "supervisor.hh"
#include "query_result_merger.hh"
#include <seastar/core/do_with.hh>
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "storage_service.hh"
#include <seastar/core/future-util.hh>
#include "db/read_repair_decision.hh"
#include "db/config.hh"
#include "db/batchlog_manager.hh"
#include "db/hints/manager.hh"
#include "db/system_keyspace.hh"
#include "exceptions/exceptions.hh"
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/cxx11/none_of.hpp>
#include <boost/algorithm/cxx11/partition_copy.hpp>
#include <boost/range/algorithm/count_if.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/numeric.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/empty.hpp>
#include <boost/range/algorithm/min_element.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/intrusive/list.hpp>
#include "utils/latency.hh"
#include "schema.hh"
#include "schema_registry.hh"
#include "utils/joinpoint.hh"
#include <seastar/util/lazy.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/execution_stage.hh>
#include "db/timeout_clock.hh"
#include "multishard_mutation_query.hh"
#include "database.hh"

namespace bi = boost::intrusive;

namespace service {

static logging::logger slogger("storage_proxy");
static logging::logger qlogger("query_result");
static logging::logger mlogger("mutation_data");

const sstring storage_proxy::COORDINATOR_STATS_CATEGORY("storage_proxy_coordinator");
const sstring storage_proxy::REPLICA_STATS_CATEGORY("storage_proxy_replica");

distributed<service::storage_proxy> _the_storage_proxy;

using namespace exceptions;
using fbu = utils::fb_utilities;

static inline
const dht::token& start_token(const dht::partition_range& r) {
    static const dht::token min_token = dht::minimum_token();
    return r.start() ? r.start()->value().token() : min_token;
}

static inline
const dht::token& end_token(const dht::partition_range& r) {
    static const dht::token max_token = dht::maximum_token();
    return r.end() ? r.end()->value().token() : max_token;
}

static inline
sstring get_dc(gms::inet_address ep) {
    auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
    return snitch_ptr->get_datacenter(ep);
}

static inline
sstring get_local_dc() {
    auto local_addr = utils::fb_utilities::get_broadcast_address();
    return get_dc(local_addr);
}

class mutation_holder {
protected:
    size_t _size = 0;
    schema_ptr _schema;
public:
    virtual ~mutation_holder() {}
    // Can return a nullptr
    virtual lw_shared_ptr<const frozen_mutation> get_mutation_for(gms::inet_address ep) = 0;
    virtual bool is_shared() = 0;
    size_t size() const {
        return _size;
    }
    const schema_ptr& schema() {
        return _schema;
    }
    virtual void release_mutation() = 0;
};

// different mutation for each destination (for read repairs)
class per_destination_mutation : public mutation_holder {
    std::unordered_map<gms::inet_address, lw_shared_ptr<const frozen_mutation>> _mutations;
    dht::token _token;
public:
    per_destination_mutation(const std::unordered_map<gms::inet_address, std::optional<mutation>>& mutations) {
        for (auto&& m : mutations) {
            lw_shared_ptr<const frozen_mutation> fm;
            if (m.second) {
                _schema = m.second.value().schema();
                _token = m.second.value().token();
                fm = make_lw_shared<const frozen_mutation>(freeze(m.second.value()));
                _size += fm->representation().size();
            }
            _mutations.emplace(m.first, std::move(fm));
        }
    }
    virtual lw_shared_ptr<const frozen_mutation> get_mutation_for(gms::inet_address ep) override {
        return _mutations[ep];
    }
    virtual bool is_shared() override {
        return false;
    }
    virtual void release_mutation() override {
        for (auto&& m : _mutations) {
            if (m.second) {
                m.second.release();
            }
        }
    }
    dht::token& token() {
        return _token;
    }
};

// same mutation for each destination
class shared_mutation : public mutation_holder {
    lw_shared_ptr<const frozen_mutation> _mutation;
public:
    explicit shared_mutation(frozen_mutation_and_schema&& fm_a_s)
            : _mutation(make_lw_shared<const frozen_mutation>(std::move(fm_a_s.fm))) {
        _size = _mutation->representation().size();
        _schema = std::move(fm_a_s.s);
    }
    explicit shared_mutation(const mutation& m) : shared_mutation(frozen_mutation_and_schema{freeze(m), m.schema()}) {
    }
    lw_shared_ptr<const frozen_mutation> get_mutation_for(gms::inet_address ep) override {
        return _mutation;
    }
    virtual bool is_shared() override {
        return true;
    }
    virtual void release_mutation() override {
        _mutation.release();
    }
};

class abstract_write_response_handler : public seastar::enable_shared_from_this<abstract_write_response_handler> {
protected:
    storage_proxy::response_id_type _id;
    promise<> _ready; // available when cl is achieved
    shared_ptr<storage_proxy> _proxy;
    tracing::trace_state_ptr _trace_state;
    db::consistency_level _cl;
    size_t _total_block_for = 0;
    db::write_type _type;
    std::unique_ptr<mutation_holder> _mutation_holder;
    std::unordered_set<gms::inet_address> _targets; // who we sent this mutation to
    // added dead_endpoints as a memeber here as well. This to be able to carry the info across
    // calls in helper methods in a convinient way. Since we hope this will be empty most of the time
    // it should not be a huge burden. (flw)
    std::vector<gms::inet_address> _dead_endpoints;
    size_t _cl_acks = 0;
    bool _cl_achieved = false;
    bool _throttled = false;
    enum class error : uint8_t {
        NONE,
        TIMEOUT,
        FAILURE,
    };
    error _error = error::NONE;
    size_t _failed = 0; // only failures that may impact consistency
    size_t _all_failures = 0; // total amount of failures
    size_t _total_endpoints = 0;
    storage_proxy::write_stats& _stats;
    timer<storage_proxy::clock_type> _expire_timer;

protected:
    virtual bool waited_for(gms::inet_address from) = 0;
    void signal(gms::inet_address from) {
        if (waited_for(from)) {
            signal();
        }
    }

public:
    abstract_write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, std::unordered_set<gms::inet_address> targets, tracing::trace_state_ptr trace_state,
            storage_proxy::write_stats& stats, size_t pending_endpoints = 0, std::vector<gms::inet_address> dead_endpoints = {})
            : _id(p->get_next_response_id()), _proxy(std::move(p)), _trace_state(trace_state), _cl(cl), _type(type), _mutation_holder(std::move(mh)), _targets(std::move(targets)),
              _dead_endpoints(std::move(dead_endpoints)), _stats(stats), _expire_timer([this] { timeout_cb(); }) {
        // original comment from cassandra:
        // during bootstrap, include pending endpoints in the count
        // or we may fail the consistency level guarantees (see #833, #8058)
        _total_block_for = db::block_for(ks, _cl) + pending_endpoints;
        ++_stats.writes;
    }
    virtual ~abstract_write_response_handler() {
        --_stats.writes;
        if (_cl_achieved) {
            if (_throttled) {
                _ready.set_value();
            } else {
                _stats.background_writes--;
                _stats.background_write_bytes -= _mutation_holder->size();
                _proxy->unthrottle();
            }
        } else if (_error == error::TIMEOUT) {
            _ready.set_exception(mutation_write_timeout_exception(get_schema()->ks_name(), get_schema()->cf_name(), _cl, _cl_acks, _total_block_for, _type));
        } else if (_error == error::FAILURE) {
            _ready.set_exception(mutation_write_failure_exception(get_schema()->ks_name(), get_schema()->cf_name(), _cl, _cl_acks, _failed, _total_block_for, _type));
        }
    }
    bool is_counter() const {
        return _type == db::write_type::COUNTER;
    }
    // While delayed, a request is not throttled.
    void unthrottle() {
        _stats.background_writes++;
        _stats.background_write_bytes += _mutation_holder->size();
        _throttled = false;
        _ready.set_value();
    }
    void signal(size_t nr = 1) {
        _cl_acks += nr;
        if (!_cl_achieved && _cl_acks >= _total_block_for) {
             _cl_achieved = true;
            delay(get_trace_state(), [] (abstract_write_response_handler* self) {
                if (self->_proxy->need_throttle_writes()) {
                    self->_throttled = true;
                    self->_proxy->_throttled_writes.push_back(self->_id);
                    ++self->_stats.throttled_writes;
                } else {
                    self->unthrottle();
                }
            });
        }
    }
    virtual bool failure(gms::inet_address from, size_t count) {
        if (waited_for(from)) {
            _failed += count;
            if (_total_block_for + _failed > _total_endpoints) {
                _error = error::FAILURE;
                delay(get_trace_state(), [] (abstract_write_response_handler*) { });
                return true;
            }
        }
        return false;
    }
    void on_timeout() {
        if (_cl_achieved) {
            slogger.trace("Write is not acknowledged by {} replicas after achieving CL", get_targets());
        }
        _error = error::TIMEOUT;
        // We don't delay request completion after a timeout, but its possible we are currently delaying.
    }
    // return true on last ack
    bool response(gms::inet_address from) {
        auto it = _targets.find(from);
        if (it != _targets.end()) {
            signal(from);
            _targets.erase(it);
        } else {
            slogger.warn("Receive outdated write ack from {}", from);
        }
        return _targets.size() == 0;
    }
    // return true if handler is no longer needed because
    // CL cannot be reached
    bool failure_response(gms::inet_address from, size_t count) {
        auto it = _targets.find(from);
        if (it == _targets.end()) {
            // There is a little change we can get outdated reply
            // if the coordinator was restarted after sending a request and
            // getting reply back. The chance is low though since initial
            // request id is initialized to server starting time
            slogger.warn("Receive outdated write failure from {}", from);
            return false;
        }
        _all_failures += count;
        // we should not fail CL=ANY requests since they may succeed after
        // writing hints
        return _cl != db::consistency_level::ANY && failure(from, count);
    }
    void check_for_early_completion() {
        if (_all_failures == _targets.size()) {
            // leftover targets are all reported error, so nothing to wait for any longer
            timeout_cb();
        }
    }
    void expire_at(storage_proxy::clock_type::time_point timeout) {
        _expire_timer.arm(timeout);
    }
    void on_released() {
        _expire_timer.cancel();
        _mutation_holder->release_mutation();
    }
    void timeout_cb() {
        if (_cl_achieved || _cl == db::consistency_level::ANY) {
            // we are here because either cl was achieved, but targets left in the handler are not
            // responding, so a hint should be written for them, or cl == any in which case
            // hints are counted towards consistency, so we need to write hints and count how much was written
            auto hints = _proxy->hint_to_dead_endpoints(_mutation_holder, get_targets(), _type, get_trace_state());
            signal(hints);
            if (_cl == db::consistency_level::ANY && hints) {
                slogger.trace("Wrote hint to satisfy CL.ANY after no replicas acknowledged the write");
            }
            if (_cl_achieved) { // For CL=ANY this can still be false
                for (auto&& ep : get_targets()) {
                    ++stats().background_replica_writes_failed.get_ep_stat(ep);
                }
                stats().background_writes_failed += int(!_targets.empty());
            }
        }

        on_timeout();
        _proxy->remove_response_handler(_id);
    }
    db::view::update_backlog max_backlog() {
        return boost::accumulate(
                get_targets() | boost::adaptors::transformed([this] (gms::inet_address ep) {
                    return _proxy->get_backlog_of(ep);
                }),
                db::view::update_backlog::no_backlog(),
                [] (const db::view::update_backlog& lhs, const db::view::update_backlog& rhs) {
                    return std::max(lhs, rhs);
                });
    }
    std::chrono::microseconds calculate_delay(db::view::update_backlog backlog) {
        constexpr auto delay_limit_us = 1000000;
        auto adjust = [] (float x) { return x * x * x; };
        auto budget = std::max(std::chrono::microseconds(0), std::chrono::microseconds(_expire_timer.get_timeout() - storage_proxy::clock_type::now()));
        return std::min(
                budget,
                std::chrono::microseconds(uint32_t(adjust(backlog.relative_size()) * delay_limit_us)));
    }
    // Calculates how much to delay completing the request. The delay adds to the request's inherent latency.
    template<typename Func>
    void delay(tracing::trace_state_ptr trace, Func&& on_resume) {
        auto backlog = max_backlog();
        auto delay = calculate_delay(backlog);
        stats().last_mv_flow_control_delay = delay;
        if (delay.count() == 0) {
            tracing::trace(trace, "Delay decision due to throttling: do not delay, resuming now");
            on_resume(this);
        } else {
            ++stats().throttled_base_writes;
            tracing::trace(trace, "Delaying user write due to view update backlog {}/{} by {}us",
                          backlog.current, backlog.max, delay.count());
            sleep_abortable<seastar::steady_clock_type>(delay).finally([self = shared_from_this(), on_resume = std::forward<Func>(on_resume)] {
                --self->stats().throttled_base_writes;
                on_resume(self.get());
            }).handle_exception_type([] (const seastar::sleep_aborted& ignored) { });
        }
    }
    future<> wait() {
        return _ready.get_future();
    }
    const std::unordered_set<gms::inet_address>& get_targets() const {
        return _targets;
    }
    const std::vector<gms::inet_address>& get_dead_endpoints() const {
        return _dead_endpoints;
    }
    lw_shared_ptr<const frozen_mutation> get_mutation_for(gms::inet_address ep) {
        return _mutation_holder->get_mutation_for(ep);
    }
    const schema_ptr& get_schema() const {
        return _mutation_holder->schema();
    }
    storage_proxy::response_id_type id() const {
      return _id;
    }
    bool read_repair_write() {
        return !_mutation_holder->is_shared();
    }
    const tracing::trace_state_ptr& get_trace_state() const {
        return _trace_state;
    }
    storage_proxy::write_stats& stats() {
        return _stats;
    }
    friend storage_proxy;
};

class datacenter_write_response_handler : public abstract_write_response_handler {
    bool waited_for(gms::inet_address from) override {
        return fbu::is_me(from) || db::is_local(from);
    }

public:
    datacenter_write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, std::unordered_set<gms::inet_address> targets,
            const std::vector<gms::inet_address>& pending_endpoints, std::vector<gms::inet_address> dead_endpoints, tracing::trace_state_ptr tr_state,
            storage_proxy::write_stats& stats) :
                abstract_write_response_handler(std::move(p), ks, cl, type, std::move(mh),
                        std::move(targets), std::move(tr_state), stats, db::count_local_endpoints(pending_endpoints), std::move(dead_endpoints)) {
        _total_endpoints = db::count_local_endpoints(_targets);
    }
};

class write_response_handler : public abstract_write_response_handler {
    bool waited_for(gms::inet_address from) override {
        return true;
    }
public:
    write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, std::unordered_set<gms::inet_address> targets,
            const std::vector<gms::inet_address>& pending_endpoints, std::vector<gms::inet_address> dead_endpoints, tracing::trace_state_ptr tr_state,
            storage_proxy::write_stats& stats) :
                abstract_write_response_handler(std::move(p), ks, cl, type, std::move(mh),
                        std::move(targets), std::move(tr_state), stats, pending_endpoints.size(), std::move(dead_endpoints)) {
        _total_endpoints = _targets.size();
    }
};

class view_update_write_response_handler : public write_response_handler, public bi::list_base_hook<bi::link_mode<bi::auto_unlink>> {
public:
    view_update_write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl,
            std::unique_ptr<mutation_holder> mh, std::unordered_set<gms::inet_address> targets,
            const std::vector<gms::inet_address>& pending_endpoints, std::vector<gms::inet_address> dead_endpoints, tracing::trace_state_ptr tr_state,
            storage_proxy::write_stats& stats):
                write_response_handler(p, ks, cl, db::write_type::VIEW, std::move(mh),
                        std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), stats) {
        register_in_intrusive_list(*p);
    }
private:
    void register_in_intrusive_list(storage_proxy& p);
};

class storage_proxy::view_update_handlers_list : public bi::list<view_update_write_response_handler, bi::base_hook<view_update_write_response_handler>, bi::constant_time_size<false>> {
};

void view_update_write_response_handler::register_in_intrusive_list(storage_proxy& p) {
    p.get_view_update_handlers_list().push_back(*this);
}

class datacenter_sync_write_response_handler : public abstract_write_response_handler {
    struct dc_info {
        size_t acks;
        size_t total_block_for;
        size_t total_endpoints;
        size_t failures;
    };
    std::unordered_map<sstring, dc_info> _dc_responses;
    bool waited_for(gms::inet_address from) override {
        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
        sstring data_center = snitch_ptr->get_datacenter(from);
        auto dc_resp = _dc_responses.find(data_center);

        if (dc_resp->second.acks < dc_resp->second.total_block_for) {
            ++dc_resp->second.acks;
            return true;
        }
        return false;
    }
public:
    datacenter_sync_write_response_handler(shared_ptr<storage_proxy> p, keyspace& ks, db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, std::unordered_set<gms::inet_address> targets, const std::vector<gms::inet_address>& pending_endpoints,
            std::vector<gms::inet_address> dead_endpoints, tracing::trace_state_ptr tr_state, storage_proxy::write_stats& stats) :
        abstract_write_response_handler(std::move(p), ks, cl, type, std::move(mh), targets, std::move(tr_state), stats, 0, dead_endpoints) {
        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();

        for (auto& target : targets) {
            auto dc = snitch_ptr->get_datacenter(target);

            if (_dc_responses.find(dc) == _dc_responses.end()) {
                auto pending_for_dc = boost::range::count_if(pending_endpoints, [&snitch_ptr, &dc] (const gms::inet_address& ep){
                    return snitch_ptr->get_datacenter(ep) == dc;
                });
                size_t total_endpoints_for_dc = boost::range::count_if(targets, [&snitch_ptr, &dc] (const gms::inet_address& ep){
                    return snitch_ptr->get_datacenter(ep) == dc;
                });
                _dc_responses.emplace(dc, dc_info{0, db::local_quorum_for(ks, dc) + pending_for_dc, total_endpoints_for_dc, 0});
                _total_block_for += pending_for_dc;
            }
        }
    }
    bool failure(gms::inet_address from, size_t count) override {
        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
        const sstring& dc = snitch_ptr->get_datacenter(from);
        auto dc_resp = _dc_responses.find(dc);

        dc_resp->second.failures += count;
        _failed += count;
        if (dc_resp->second.total_block_for + dc_resp->second.failures > dc_resp->second.total_endpoints) {
            _error = error::FAILURE;
            return true;
        }
        return false;
    }
};

static std::vector<gms::inet_address>
replica_ids_to_endpoints(const std::vector<utils::UUID>& replica_ids) {
    const auto& tm = get_local_storage_service().get_token_metadata();

    std::vector<gms::inet_address> endpoints;
    endpoints.reserve(replica_ids.size());

    for (const auto& replica_id : replica_ids) {
        if (auto endpoint_opt = tm.get_endpoint_for_host_id(replica_id)) {
            endpoints.push_back(*endpoint_opt);
        }
    }

    return endpoints;
}

static std::vector<utils::UUID>
endpoints_to_replica_ids(const std::vector<gms::inet_address>& endpoints) {
    const auto& tm = get_local_storage_service().get_token_metadata();

    std::vector<utils::UUID> replica_ids;
    replica_ids.reserve(endpoints.size());

    for (const auto& endpoint : endpoints) {
        if (auto replica_id_opt = tm.get_host_id_if_known(endpoint)) {
            replica_ids.push_back(*replica_id_opt);
        }
    }

    return replica_ids;
}

bool storage_proxy::need_throttle_writes() const {
    return _stats.background_write_bytes > _background_write_throttle_threahsold || _stats.queued_write_bytes > 6*1024*1024;
}

void storage_proxy::unthrottle() {
   while(!need_throttle_writes() && !_throttled_writes.empty()) {
       auto id = _throttled_writes.front();
       _throttled_writes.pop_front();
       auto it = _response_handlers.find(id);
       if (it != _response_handlers.end()) {
           it->second->unthrottle();
       }
   }
}

storage_proxy::response_id_type storage_proxy::register_response_handler(shared_ptr<abstract_write_response_handler>&& h) {
    auto id = h->id();
    auto e = _response_handlers.emplace(id, std::move(h));
    assert(e.second);
    return id;
}

void storage_proxy::remove_response_handler(storage_proxy::response_id_type id) {
    auto entry = _response_handlers.find(id);
    entry->second->on_released();
    _response_handlers.erase(std::move(entry));
}


void storage_proxy::got_response(storage_proxy::response_id_type id, gms::inet_address from, std::optional<db::view::update_backlog> backlog) {
    auto it = _response_handlers.find(id);
    if (it != _response_handlers.end()) {
        tracing::trace(it->second->get_trace_state(), "Got a response from /{}", from);
        if (it->second->response(from)) {
            remove_response_handler(id); // last one, remove entry. Will cancel expiration timer too.
        } else {
            it->second->check_for_early_completion();
        }
    }
    maybe_update_view_backlog_of(std::move(from), std::move(backlog));
}

void storage_proxy::got_failure_response(storage_proxy::response_id_type id, gms::inet_address from, size_t count, std::optional<db::view::update_backlog> backlog) {
    auto it = _response_handlers.find(id);
    if (it != _response_handlers.end()) {
        tracing::trace(it->second->get_trace_state(), "Got {} failures from /{}", count, from);
        if (it->second->failure_response(from, count)) {
            remove_response_handler(id);
        } else {
            it->second->check_for_early_completion();
        }
    }
    maybe_update_view_backlog_of(std::move(from), std::move(backlog));
}

void storage_proxy::maybe_update_view_backlog_of(gms::inet_address replica, std::optional<db::view::update_backlog> backlog) {
    if (backlog) {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        _view_update_backlogs[replica] = {std::move(*backlog), now};
    }
}

db::view::update_backlog storage_proxy::get_view_update_backlog() const {
    return _max_view_update_backlog.add_fetch(engine().cpu_id(), get_db().local().get_view_update_backlog());
}

db::view::update_backlog storage_proxy::get_backlog_of(gms::inet_address ep) const {
    auto it = _view_update_backlogs.find(ep);
    if (it == _view_update_backlogs.end()) {
        return db::view::update_backlog::no_backlog();
    }
    return it->second.backlog;
}

future<> storage_proxy::response_wait(storage_proxy::response_id_type id, clock_type::time_point timeout) {
    auto& handler = _response_handlers.find(id)->second;
    handler->expire_at(timeout);
    return handler->wait();
}

::shared_ptr<abstract_write_response_handler>& storage_proxy::get_write_response_handler(storage_proxy::response_id_type id) {
        return _response_handlers.find(id)->second;
}

storage_proxy::response_id_type storage_proxy::create_write_response_handler(keyspace& ks, db::consistency_level cl, db::write_type type, std::unique_ptr<mutation_holder> m,
                             std::unordered_set<gms::inet_address> targets, const std::vector<gms::inet_address>& pending_endpoints, std::vector<gms::inet_address> dead_endpoints, tracing::trace_state_ptr tr_state,
                             storage_proxy::write_stats& stats)
{
    shared_ptr<abstract_write_response_handler> h;
    auto& rs = ks.get_replication_strategy();

    if (db::is_datacenter_local(cl)) {
        h = ::make_shared<datacenter_write_response_handler>(shared_from_this(), ks, cl, type, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), stats);
    } else if (cl == db::consistency_level::EACH_QUORUM && rs.get_type() == locator::replication_strategy_type::network_topology){
        h = ::make_shared<datacenter_sync_write_response_handler>(shared_from_this(), ks, cl, type, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), stats);
    } else if (type == db::write_type::VIEW) {
        h = ::make_shared<view_update_write_response_handler>(shared_from_this(), ks, cl, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), stats);
    } else {
        h = ::make_shared<write_response_handler>(shared_from_this(), ks, cl, type, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), stats);
    }
    return register_response_handler(std::move(h));
}

seastar::metrics::label storage_proxy_stats::split_stats::datacenter_label("datacenter");
seastar::metrics::label storage_proxy_stats::split_stats::op_type_label("op_type");

storage_proxy_stats::split_stats::split_stats(const sstring& category, const sstring& short_description_prefix, const sstring& long_description_prefix, const sstring& op_type, bool auto_register_metrics)
        : _short_description_prefix(short_description_prefix)
        , _long_description_prefix(long_description_prefix)
        , _category(category)
        , _op_type(op_type)
        , _auto_register_metrics(auto_register_metrics) { }

storage_proxy_stats::write_stats::write_stats()
: writes_attempts(storage_proxy::COORDINATOR_STATS_CATEGORY, "total_write_attempts", "total number of write requests", "mutation_data")
, writes_errors(storage_proxy::COORDINATOR_STATS_CATEGORY, "write_errors", "number of write requests that failed", "mutation_data")
, background_replica_writes_failed(storage_proxy::COORDINATOR_STATS_CATEGORY, "background_replica_writes_failed", "number of replica writes that timed out or failed after CL was reached", "mutation_data")
, read_repair_write_attempts(storage_proxy::COORDINATOR_STATS_CATEGORY, "read_repair_write_attempts", "number of write operations in a read repair context", "mutation_data") { }

storage_proxy_stats::write_stats::write_stats(const sstring& category, bool auto_register_stats)
        : writes_attempts(category, "total_write_attempts", "total number of write requests", "mutation_data", auto_register_stats)
        , writes_errors(category, "write_errors", "number of write requests that failed", "mutation_data", auto_register_stats)
        , background_replica_writes_failed(category, "background_replica_writes_failed", "number of replica writes that timed out or failed after CL was reached", "mutation_data", auto_register_stats)
        , read_repair_write_attempts(category, "read_repair_write_attempts", "number of write operations in a read repair context", "mutation_data", auto_register_stats) { }

void storage_proxy_stats::write_stats::register_metrics_local() {
    writes_attempts.register_metrics_local();
    writes_errors.register_metrics_local();
    background_replica_writes_failed.register_metrics_local();
    read_repair_write_attempts.register_metrics_local();
}

void storage_proxy_stats::write_stats::register_metrics_for(gms::inet_address ep) {
    writes_attempts.register_metrics_for(ep);
    writes_errors.register_metrics_for(ep);
    background_replica_writes_failed.register_metrics_for(ep);
    read_repair_write_attempts.register_metrics_for(ep);
}

storage_proxy_stats::stats::stats()
        : write_stats()
        , data_read_attempts(storage_proxy::COORDINATOR_STATS_CATEGORY, "reads", "number of data read requests", "data")
        , data_read_completed(storage_proxy::COORDINATOR_STATS_CATEGORY, "completed_reads", "number of data read requests that completed", "data")
        , data_read_errors(storage_proxy::COORDINATOR_STATS_CATEGORY, "read_errors", "number of data read requests that failed", "data")
        , digest_read_attempts(storage_proxy::COORDINATOR_STATS_CATEGORY, "reads", "number of digest read requests", "digest")
        , digest_read_completed(storage_proxy::COORDINATOR_STATS_CATEGORY, "completed_reads", "number of digest read requests that completed", "digest")
        , digest_read_errors(storage_proxy::COORDINATOR_STATS_CATEGORY, "read_errors", "number of digest read requests that failed", "digest")
        , mutation_data_read_attempts(storage_proxy::COORDINATOR_STATS_CATEGORY, "reads", "number of mutation data read requests", "mutation_data")
        , mutation_data_read_completed(storage_proxy::COORDINATOR_STATS_CATEGORY, "completed_reads", "number of mutation data read requests that completed", "mutation_data")
        , mutation_data_read_errors(storage_proxy::COORDINATOR_STATS_CATEGORY, "read_errors", "number of mutation data read requests that failed", "mutation_data") { }

void storage_proxy_stats::stats::register_metrics_local() {
    write_stats::register_metrics_local();

    data_read_attempts.register_metrics_local();
    data_read_completed.register_metrics_local();
    data_read_errors.register_metrics_local();
    digest_read_attempts.register_metrics_local();
    digest_read_completed.register_metrics_local();
    mutation_data_read_attempts.register_metrics_local();
    mutation_data_read_completed.register_metrics_local();
    mutation_data_read_errors.register_metrics_local();
}

void storage_proxy_stats::stats::register_metrics_for(gms::inet_address ep) {
    write_stats::register_metrics_for(ep);

    data_read_attempts.register_metrics_for(ep);
    data_read_completed.register_metrics_for(ep);
    data_read_errors.register_metrics_for(ep);
    digest_read_attempts.register_metrics_for(ep);
    digest_read_completed.register_metrics_for(ep);
    mutation_data_read_attempts.register_metrics_for(ep);
    mutation_data_read_completed.register_metrics_for(ep);
    mutation_data_read_errors.register_metrics_for(ep);
}

inline uint64_t& storage_proxy_stats::split_stats::get_ep_stat(gms::inet_address ep) {
    if (fbu::is_me(ep)) {
        return _local.val;
    }

    sstring dc = get_dc(ep);
    if (_auto_register_metrics) {
        register_metrics_for(ep);
    }
    return _dc_stats[dc].val;
}

void storage_proxy_stats::split_stats::register_metrics_local() {
    namespace sm = seastar::metrics;

    _metrics.add_group(_category, {
        sm::make_derive(_short_description_prefix + sstring("_local_node"), [this] { return _local.val; },
                       sm::description(_long_description_prefix + "on a local Node"), {op_type_label(_op_type)})
    });
}

void storage_proxy_stats::split_stats::register_metrics_for(gms::inet_address ep) {
    namespace sm = seastar::metrics;

    sstring dc = get_dc(ep);
    // if this is the first time we see an endpoint from this DC - add a
    // corresponding collectd metric
    if (_dc_stats.find(dc) == _dc_stats.end()) {
        _metrics.add_group(_category, {
            sm::make_derive(_short_description_prefix + sstring("_remote_node"), [this, dc] { return _dc_stats[dc].val; },
                            sm::description(seastar::format("{} when communicating with external Nodes in DC {}", _long_description_prefix, dc)), {datacenter_label(dc), op_type_label(_op_type)})
        });
    }
}

using namespace std::literals::chrono_literals;

storage_proxy::~storage_proxy() {}
storage_proxy::storage_proxy(distributed<database>& db, storage_proxy::config cfg, db::view::node_update_backlog& max_view_update_backlog)
    : _db(db)
    , _next_response_id(std::chrono::system_clock::now().time_since_epoch()/1ms)
    , _hints_resource_manager(cfg.available_memory / 10)
    , _hints_for_views_manager(_db.local().get_config().view_hints_directory(), {}, _db.local().get_config().max_hint_window_in_ms(), _hints_resource_manager, _db)
    , _background_write_throttle_threahsold(cfg.available_memory / 10)
    , _mutate_stage{"storage_proxy_mutate", &storage_proxy::do_mutate}
    , _max_view_update_backlog(max_view_update_backlog)
    , _view_update_handlers_list(std::make_unique<view_update_handlers_list>()) {
    namespace sm = seastar::metrics;
    _metrics.add_group(COORDINATOR_STATS_CATEGORY, {
        sm::make_histogram("read_latency", sm::description("The general read latency histogram"), [this]{ return _stats.estimated_read.get_histogram(16, 20);}),
        sm::make_histogram("write_latency", sm::description("The general write latency histogram"), [this]{return _stats.estimated_write.get_histogram(16, 20);}),
        sm::make_queue_length("foreground_writes", [this] { return _stats.writes - _stats.background_writes; },
                       sm::description("number of currently pending foreground write requests")),

        sm::make_queue_length("background_writes", [this] { return _stats.background_writes; },
                       sm::description("number of currently pending background write requests")),

        sm::make_queue_length("current_throttled_writes", [this] { return _throttled_writes.size(); },
                       sm::description("number of currently throttled write requests")),

        sm::make_queue_length("current_throttled_base_writes", [this] { return _stats.throttled_base_writes; },
                       sm::description("number of currently throttled base replica write requests")),

        sm::make_gauge("last_mv_flow_control_delay", [this] { return std::chrono::duration<float>(_stats.last_mv_flow_control_delay).count(); },
                                      sm::description("delay (in seconds) added for MV flow control in the last request")),

        sm::make_total_operations("throttled_writes", [this] { return _stats.throttled_writes; },
                                  sm::description("number of throttled write requests")),

        sm::make_current_bytes("queued_write_bytes", [this] { return _stats.queued_write_bytes; },
                       sm::description("number of bytes in pending write requests")),

        sm::make_current_bytes("background_write_bytes", [this] { return _stats.background_write_bytes; },
                       sm::description("number of bytes in pending background write requests")),

        sm::make_queue_length("foreground_reads", [this] { return _stats.foreground_reads; },
                       sm::description("number of currently pending foreground read requests")),

        sm::make_queue_length("background_reads", [this] { return _stats.reads - _stats.foreground_reads; },
                       sm::description("number of currently pending background read requests")),

        sm::make_total_operations("read_retries", [this] { return _stats.read_retries; },
                       sm::description("number of read retry attempts")),

        sm::make_total_operations("canceled_read_repairs", [this] { return _stats.global_read_repairs_canceled_due_to_concurrent_write; },
                       sm::description("number of global read repairs canceled due to a concurrent write")),

        sm::make_total_operations("foreground_read_repair", [this] { return _stats.read_repair_repaired_blocking; },
                      sm::description("number of foreground read repairs")),

        sm::make_total_operations("background_read_repairs", [this] { return _stats.read_repair_repaired_background; },
                       sm::description("number of background read repairs")),

        sm::make_total_operations("write_timeouts", [this] { return _stats.write_timeouts._count; },
                       sm::description("number of write request failed due to a timeout")),

        sm::make_total_operations("write_unavailable", [this] { return _stats.write_unavailables._count; },
                       sm::description("number write requests failed due to an \"unavailable\" error")),

        sm::make_total_operations("read_timeouts", [this] { return _stats.read_timeouts._count; },
                       sm::description("number of read request failed due to a timeout")),

        sm::make_total_operations("read_unavailable", [this] { return _stats.read_unavailables._count; },
                       sm::description("number read requests failed due to an \"unavailable\" error")),

        sm::make_total_operations("range_timeouts", [this] { return _stats.range_slice_timeouts._count; },
                       sm::description("number of range read operations failed due to a timeout")),

        sm::make_total_operations("range_unavailable", [this] { return _stats.range_slice_unavailables._count; },
                       sm::description("number of range read operations failed due to an \"unavailable\" error")),

        sm::make_total_operations("speculative_digest_reads", [this] { return _stats.speculative_digest_reads; },
                       sm::description("number of speculative digest read requests that were sent")),

        sm::make_total_operations("speculative_data_reads", [this] { return _stats.speculative_data_reads; },
                       sm::description("number of speculative data read requests that were sent")),

        sm::make_total_operations("background_writes_failed", [this] { return _stats.background_writes_failed; },
                       sm::description("number of write requests that failed after CL was reached")),
    });

    _metrics.add_group(REPLICA_STATS_CATEGORY, {
        sm::make_total_operations("received_counter_updates", _stats.received_counter_updates,
                       sm::description("number of counter updates received by this node acting as an update leader")),

        sm::make_total_operations("received_mutations", _stats.received_mutations,
                       sm::description("number of mutations received by a replica Node")),

        sm::make_total_operations("forwarded_mutations", _stats.forwarded_mutations,
                       sm::description("number of mutations forwarded to other replica Nodes")),

        sm::make_total_operations("forwarding_errors", _stats.forwarding_errors,
                       sm::description("number of errors during forwarding mutations to other replica Nodes")),

        sm::make_total_operations("reads", _stats.replica_data_reads,
                       sm::description("number of remote data read requests this Node received"), {storage_proxy_stats::split_stats::op_type_label("data")}),

        sm::make_total_operations("reads", _stats.replica_mutation_data_reads,
                       sm::description("number of remote mutation data read requests this Node received"), {storage_proxy_stats::split_stats::op_type_label("mutation_data")}),

        sm::make_total_operations("reads", _stats.replica_digest_reads,
                       sm::description("number of remote digest read requests this Node received"), {storage_proxy_stats::split_stats::op_type_label("digest")}),

        sm::make_total_operations("cross_shard_ops", _stats.replica_cross_shard_ops,
                       sm::description("number of operations that crossed a shard boundary")),

    });

    _stats.register_metrics_local();

    if (cfg.hinted_handoff_enabled) {
        const db::config& dbcfg = _db.local().get_config();
        supervisor::notify("creating hints manager");
        slogger.trace("hinted DCs: {}", *cfg.hinted_handoff_enabled);
        _hints_manager.emplace(dbcfg.hints_directory(), *cfg.hinted_handoff_enabled, dbcfg.max_hint_window_in_ms(), _hints_resource_manager, _db);
        _hints_manager->register_metrics("hints_manager");
        _hints_resource_manager.register_manager(*_hints_manager);
    }

    _hints_for_views_manager.register_metrics("hints_for_views_manager");
    _hints_resource_manager.register_manager(_hints_for_views_manager);
}

storage_proxy::unique_response_handler::unique_response_handler(storage_proxy& p_, response_id_type id_) : id(id_), p(p_) {}
storage_proxy::unique_response_handler::unique_response_handler(unique_response_handler&& x) : id(x.id), p(x.p) { x.id = 0; };
storage_proxy::unique_response_handler::~unique_response_handler() {
    if (id) {
        p.remove_response_handler(id);
    }
}
storage_proxy::response_id_type storage_proxy::unique_response_handler::release() {
    auto r = id;
    id = 0;
    return r;
}

future<>
storage_proxy::mutate_locally(const mutation& m, clock_type::time_point timeout) {
    auto shard = _db.local().shard_of(m);
    _stats.replica_cross_shard_ops += shard != engine().cpu_id();
    return _db.invoke_on(shard, [s = global_schema_ptr(m.schema()), m = freeze(m), timeout] (database& db) -> future<> {
        return db.apply(s, m, timeout);
    });
}

future<>
storage_proxy::mutate_locally(const schema_ptr& s, const frozen_mutation& m, clock_type::time_point timeout) {
    auto shard = _db.local().shard_of(m);
    _stats.replica_cross_shard_ops += shard != engine().cpu_id();
    return _db.invoke_on(shard, [&m, gs = global_schema_ptr(s), timeout] (database& db) -> future<> {
        return db.apply(gs, m, timeout);
    });
}

future<>
storage_proxy::mutate_locally(std::vector<mutation> mutations, clock_type::time_point timeout) {
    return do_with(std::move(mutations), [this, timeout] (std::vector<mutation>& pmut){
        return parallel_for_each(pmut.begin(), pmut.end(), [this, timeout] (const mutation& m) {
            return mutate_locally(m, timeout);
        });
    });
}

future<>
storage_proxy::mutate_counters_on_leader(std::vector<frozen_mutation_and_schema> mutations, db::consistency_level cl, clock_type::time_point timeout,
                                         tracing::trace_state_ptr trace_state) {
    _stats.received_counter_updates += mutations.size();
    return do_with(std::move(mutations), [this, cl, timeout, trace_state = std::move(trace_state)] (std::vector<frozen_mutation_and_schema>& update_ms) mutable {
        return parallel_for_each(update_ms, [this, cl, timeout, trace_state] (frozen_mutation_and_schema& fm_a_s) {
            return mutate_counter_on_leader_and_replicate(fm_a_s.s, std::move(fm_a_s.fm), cl, timeout, trace_state);
        });
    });
}

future<>
storage_proxy::mutate_counter_on_leader_and_replicate(const schema_ptr& s, frozen_mutation fm, db::consistency_level cl, clock_type::time_point timeout,
                                                      tracing::trace_state_ptr trace_state) {
    auto shard = _db.local().shard_of(fm);
    _stats.replica_cross_shard_ops += shard != engine().cpu_id();
    return _db.invoke_on(shard, [gs = global_schema_ptr(s), fm = std::move(fm), cl, timeout, gt = tracing::global_trace_state_ptr(std::move(trace_state))] (database& db) {
        auto trace_state = gt.get();
        return db.apply_counter_update(gs, fm, timeout, trace_state).then([cl, timeout, trace_state] (mutation m) mutable {
            return service::get_local_storage_proxy().replicate_counter_from_leader(std::move(m), cl, std::move(trace_state), timeout);
        });
    });
}

future<>
storage_proxy::mutate_streaming_mutation(const schema_ptr& s, utils::UUID plan_id, const frozen_mutation& m, bool fragmented) {
    auto shard = _db.local().shard_of(m);
    _stats.replica_cross_shard_ops += shard != engine().cpu_id();
    return _db.invoke_on(shard, [&m, plan_id, fragmented, gs = global_schema_ptr(s)] (database& db) mutable -> future<> {
        return db.apply_streaming_mutation(gs, plan_id, m, fragmented);
    });
}


/**
 * Helper for create_write_response_handler, shared across mutate/mutate_atomically.
 * Both methods do roughly the same thing, with the latter intermixing batch log ops
 * in the logic.
 * Since ordering is (maybe?) significant, we need to carry some info across from here
 * to the hint method below (dead nodes).
 */
storage_proxy::response_id_type
storage_proxy::create_write_response_handler(const mutation& m, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state) {
    auto keyspace_name = m.schema()->ks_name();
    keyspace& ks = _db.local().find_keyspace(keyspace_name);
    auto& rs = ks.get_replication_strategy();
    std::vector<gms::inet_address> natural_endpoints = rs.get_natural_endpoints(m.token());
    std::vector<gms::inet_address> pending_endpoints =
        get_local_storage_service().get_token_metadata().pending_endpoints_for(m.token(), keyspace_name);

    slogger.trace("creating write handler for token: {} natural: {} pending: {}", m.token(), natural_endpoints, pending_endpoints);
    tracing::trace(tr_state, "Creating write handler for token: {} natural: {} pending: {}", m.token(), natural_endpoints ,pending_endpoints);

    // filter out natural_endpoints and dead nodes from pending_endpoint if later is not yet updated during node join
    gms::gossiper& local_gossiper = gms::get_local_gossiper();
    auto itend = boost::range::remove_if(pending_endpoints, [&natural_endpoints, &local_gossiper] (gms::inet_address& p) {
        return !local_gossiper.is_alive(p) || boost::range::find(natural_endpoints, p) != natural_endpoints.end();
    });
    pending_endpoints.erase(itend, pending_endpoints.end());

    auto all = boost::range::join(natural_endpoints, pending_endpoints);

    if (cannot_hint(all, type)) {
        // avoid OOMing due to excess hints.  we need to do this check even for "live" nodes, since we can
        // still generate hints for those if it's overloaded or simply dead but not yet known-to-be-dead.
        // The idea is that if we have over maxHintsInProgress hints in flight, this is probably due to
        // a small number of nodes causing problems, so we should avoid shutting down writes completely to
        // healthy nodes.  Any node with no hintsInProgress is considered healthy.
        throw overloaded_exception(_hints_manager->size_of_hints_in_progress());
    }

    // filter live endpoints from dead ones
    std::unordered_set<gms::inet_address> live_endpoints;
    std::vector<gms::inet_address> dead_endpoints;
    live_endpoints.reserve(all.size());
    dead_endpoints.reserve(all.size());
    std::partition_copy(all.begin(), all.end(), std::inserter(live_endpoints, live_endpoints.begin()), std::back_inserter(dead_endpoints),
            std::bind1st(std::mem_fn(&gms::gossiper::is_alive), &gms::get_local_gossiper()));

    slogger.trace("creating write handler with live: {} dead: {}", live_endpoints, dead_endpoints);
    tracing::trace(tr_state, "Creating write handler with live: {} dead: {}", live_endpoints, dead_endpoints);

    db::assure_sufficient_live_nodes(cl, ks, live_endpoints, pending_endpoints);

    return create_write_response_handler(ks, cl, type, std::make_unique<shared_mutation>(m), std::move(live_endpoints), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), _stats);
}

storage_proxy::response_id_type
storage_proxy::create_write_response_handler(const std::unordered_map<gms::inet_address, std::optional<mutation>>& m, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state) {
    std::unordered_set<gms::inet_address> endpoints(m.size());
    boost::copy(m | boost::adaptors::map_keys, std::inserter(endpoints, endpoints.begin()));
    auto mh = std::make_unique<per_destination_mutation>(m);

    slogger.trace("creating write handler for read repair token: {} endpoint: {}", mh->token(), endpoints);
    tracing::trace(tr_state, "Creating write handler for read repair token: {} endpoint: {}", mh->token(), endpoints);

    auto keyspace_name = mh->schema()->ks_name();
    keyspace& ks = _db.local().find_keyspace(keyspace_name);

    return create_write_response_handler(ks, cl, type, std::move(mh), std::move(endpoints), std::vector<gms::inet_address>(), std::vector<gms::inet_address>(), std::move(tr_state), _stats);
}

void
storage_proxy::hint_to_dead_endpoints(response_id_type id, db::consistency_level cl) {
    auto& h = *get_write_response_handler(id);

    size_t hints = hint_to_dead_endpoints(h._mutation_holder, h.get_dead_endpoints(), h._type, h.get_trace_state());

    if (cl == db::consistency_level::ANY) {
        // for cl==ANY hints are counted towards consistency
        h.signal(hints);
    }
}

template<typename Range, typename CreateWriteHandler>
future<std::vector<storage_proxy::unique_response_handler>> storage_proxy::mutate_prepare(Range&& mutations, db::consistency_level cl, db::write_type type, CreateWriteHandler create_handler) {
    // apply is used to convert exceptions to exceptional future
    return futurize<std::vector<storage_proxy::unique_response_handler>>::apply([this] (Range&& mutations, db::consistency_level cl, db::write_type type, CreateWriteHandler create_handler) {
        std::vector<unique_response_handler> ids;
        ids.reserve(std::distance(std::begin(mutations), std::end(mutations)));
        for (auto& m : mutations) {
            ids.emplace_back(*this, create_handler(m, cl, type));
        }
        return make_ready_future<std::vector<unique_response_handler>>(std::move(ids));
    }, std::forward<Range>(mutations), cl, type, std::move(create_handler));
}

template<typename Range>
future<std::vector<storage_proxy::unique_response_handler>> storage_proxy::mutate_prepare(Range&& mutations, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state) {
    return mutate_prepare<>(std::forward<Range>(mutations), cl, type, [this, tr_state = std::move(tr_state)] (const typename std::decay_t<Range>::value_type& m, db::consistency_level cl, db::write_type type) mutable {
        return create_write_response_handler(m, cl, type, tr_state);
    });
}

future<> storage_proxy::mutate_begin(std::vector<unique_response_handler> ids, db::consistency_level cl,
                                     std::optional<clock_type::time_point> timeout_opt) {
    return parallel_for_each(ids, [this, cl, timeout_opt] (unique_response_handler& protected_response) {
        auto response_id = protected_response.id;
        // it is better to send first and hint afterwards to reduce latency
        // but request may complete before hint_to_dead_endpoints() is called and
        // response_id handler will be removed, so we will have to do hint with separate
        // frozen_mutation copy, or manage handler live time differently.
        hint_to_dead_endpoints(response_id, cl);

        auto timeout = timeout_opt.value_or(clock_type::now() + std::chrono::milliseconds(_db.local().get_config().write_request_timeout_in_ms()));
        // call before send_to_live_endpoints() for the same reason as above
        auto f = response_wait(response_id, timeout);
        send_to_live_endpoints(protected_response.release(), timeout); // response is now running and it will either complete or timeout
        return f;
    });
}

// this function should be called with a future that holds result of mutation attempt (usually
// future returned by mutate_begin()). The future should be ready when function is called.
future<> storage_proxy::mutate_end(future<> mutate_result, utils::latency_counter lc, write_stats& stats, tracing::trace_state_ptr trace_state) {
    assert(mutate_result.available());
    stats.write.mark(lc.stop().latency());
    if (lc.is_start()) {
        stats.estimated_write.add(lc.latency(), stats.write.hist.count);
    }
    try {
        mutate_result.get();
        tracing::trace(trace_state, "Mutation successfully completed");
        return make_ready_future<>();
    } catch (no_such_keyspace& ex) {
        tracing::trace(trace_state, "Mutation failed: write to non existing keyspace: {}", ex.what());
        slogger.trace("Write to non existing keyspace: {}", ex.what());
        return make_exception_future<>(std::current_exception());
    } catch(mutation_write_timeout_exception& ex) {
        // timeout
        tracing::trace(trace_state, "Mutation failed: write timeout; received {:d} of {:d} required replies", ex.received, ex.block_for);
        slogger.debug("Write timeout; received {} of {} required replies", ex.received, ex.block_for);
        stats.write_timeouts.mark();
        return make_exception_future<>(std::current_exception());
    } catch (exceptions::unavailable_exception& ex) {
        tracing::trace(trace_state, "Mutation failed: unavailable");
        stats.write_unavailables.mark();
        slogger.trace("Unavailable");
        return make_exception_future<>(std::current_exception());
    }  catch(overloaded_exception& ex) {
        tracing::trace(trace_state, "Mutation failed: overloaded");
        stats.write_unavailables.mark();
        slogger.trace("Overloaded");
        return make_exception_future<>(std::current_exception());
    } catch (...) {
        tracing::trace(trace_state, "Mutation failed: unknown reason");
        throw;
    }
}

gms::inet_address storage_proxy::find_leader_for_counter_update(const mutation& m, db::consistency_level cl) {
    auto& ks = _db.local().find_keyspace(m.schema()->ks_name());
    auto live_endpoints = get_live_endpoints(ks, m.token());

    if (live_endpoints.empty()) {
        throw exceptions::unavailable_exception(cl, block_for(ks, cl), 0);
    }

    auto local_endpoints = boost::copy_range<std::vector<gms::inet_address>>(live_endpoints | boost::adaptors::filtered([&] (auto&& ep) {
        return db::is_local(ep);
    }));
    if (local_endpoints.empty()) {
        // FIXME: O(n log n) to get maximum
        auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();
        snitch->sort_by_proximity(utils::fb_utilities::get_broadcast_address(), live_endpoints);
        return live_endpoints[0];
    } else {
        // FIXME: favour ourselves to avoid additional hop?
        static thread_local std::default_random_engine re{std::random_device{}()};
        std::uniform_int_distribution<> dist(0, local_endpoints.size() - 1);
        return local_endpoints[dist(re)];
    }
}

template<typename Range>
future<> storage_proxy::mutate_counters(Range&& mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state, clock_type::time_point timeout) {
    if (boost::empty(mutations)) {
        return make_ready_future<>();
    }

    slogger.trace("mutate_counters cl={}", cl);
    mlogger.trace("counter mutations={}", mutations);


    // Choose a leader for each mutation
    std::unordered_map<gms::inet_address, std::vector<frozen_mutation_and_schema>> leaders;
    for (auto& m : mutations) {
        auto leader = find_leader_for_counter_update(m, cl);
        leaders[leader].emplace_back(frozen_mutation_and_schema { freeze(m), m.schema() });
        // FIXME: check if CL can be reached
    }

    // Forward mutations to the leaders chosen for them
    auto my_address = utils::fb_utilities::get_broadcast_address();
    return parallel_for_each(leaders, [this, cl, timeout, tr_state = std::move(tr_state), my_address] (auto& endpoint_and_mutations) {
        auto endpoint = endpoint_and_mutations.first;

        // The leader receives a vector of mutations and processes them together,
        // so if there is a timeout we don't really know which one is to "blame"
        // and what to put in ks and cf fields of write timeout exception.
        // Let's just use the schema of the first mutation in a vector.
        auto handle_error = [this, sp = this->shared_from_this(), s = endpoint_and_mutations.second[0].s, cl] (std::exception_ptr exp) {
            auto& ks = _db.local().find_keyspace(s->ks_name());
            try {
                std::rethrow_exception(std::move(exp));
            } catch (rpc::timeout_error&) {
                return make_exception_future<>(mutation_write_timeout_exception(s->ks_name(), s->cf_name(), cl, 0, db::block_for(ks, cl), db::write_type::COUNTER));
            } catch (timed_out_error&) {
                return make_exception_future<>(mutation_write_timeout_exception(s->ks_name(), s->cf_name(), cl, 0, db::block_for(ks, cl), db::write_type::COUNTER));
            }
        };

        auto f = make_ready_future<>();
        if (endpoint == my_address) {
            f = this->mutate_counters_on_leader(std::move(endpoint_and_mutations.second), cl, timeout, tr_state);
        } else {
            auto& mutations = endpoint_and_mutations.second;
            auto fms = boost::copy_range<std::vector<frozen_mutation>>(mutations | boost::adaptors::transformed([] (auto& m) {
                return std::move(m.fm);
            }));

            auto& ms = netw::get_local_messaging_service();
            auto msg_addr = netw::messaging_service::msg_addr{ endpoint_and_mutations.first, 0 };
            tracing::trace(tr_state, "Enqueuing counter update to {}", msg_addr);
            f = ms.send_counter_mutation(msg_addr, timeout, std::move(fms), cl, tracing::make_trace_info(tr_state));
        }
        return f.handle_exception(std::move(handle_error));
    });
}

/**
 * Use this method to have these Mutations applied
 * across all replicas. This method will take care
 * of the possibility of a replica being down and hint
 * the data across to some other replica.
 *
 * @param mutations the mutations to be applied across the replicas
 * @param consistency_level the consistency level for the operation
 * @param tr_state trace state handle
 */
future<> storage_proxy::mutate(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, bool raw_counters) {
    return _mutate_stage(this, std::move(mutations), cl, timeout, std::move(tr_state), raw_counters);
}

future<> storage_proxy::do_mutate(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, bool raw_counters) {
    auto mid = raw_counters ? mutations.begin() : boost::range::partition(mutations, [] (auto&& m) {
        return m.schema()->is_counter();
    });
    return seastar::when_all_succeed(
        mutate_counters(boost::make_iterator_range(mutations.begin(), mid), cl, tr_state, timeout),
        mutate_internal(boost::make_iterator_range(mid, mutations.end()), cl, false, tr_state, timeout)
    );
}

future<> storage_proxy::replicate_counter_from_leader(mutation m, db::consistency_level cl, tracing::trace_state_ptr tr_state,
                                                      clock_type::time_point timeout) {
    // FIXME: do not send the mutation to itself, it has already been applied (it is not incorrect to do so, though)
    return mutate_internal(std::array<mutation, 1>{std::move(m)}, cl, true, std::move(tr_state), timeout);
}

/*
 * Range template parameter can either be range of 'mutation' or a range of 'std::unordered_map<gms::inet_address, mutation>'.
 * create_write_response_handler() has specialization for both types. The one for the former uses keyspace to figure out
 * endpoints to send mutation to, the one for the late uses enpoints that are used as keys for the map.
 */
template<typename Range>
future<>
storage_proxy::mutate_internal(Range mutations, db::consistency_level cl, bool counters, tracing::trace_state_ptr tr_state,
                               std::optional<clock_type::time_point> timeout_opt) {
    if (boost::empty(mutations)) {
        return make_ready_future<>();
    }

    slogger.trace("mutate cl={}", cl);
    mlogger.trace("mutations={}", mutations);

    // If counters is set it means that we are replicating counter shards. There
    // is no need for special handling anymore, since the leader has already
    // done its job, but we need to return correct db::write_type in case of
    // a timeout so that client doesn't attempt to retry the request.
    auto type = counters ? db::write_type::COUNTER
                         : (std::next(std::begin(mutations)) == std::end(mutations) ? db::write_type::SIMPLE : db::write_type::UNLOGGED_BATCH);
    utils::latency_counter lc;
    lc.start();

    return mutate_prepare(mutations, cl, type, tr_state).then([this, cl, timeout_opt] (std::vector<storage_proxy::unique_response_handler> ids) {
        return mutate_begin(std::move(ids), cl, timeout_opt);
    }).then_wrapped([this, p = shared_from_this(), lc, tr_state] (future<> f) mutable {
        return p->mutate_end(std::move(f), lc, _stats, std::move(tr_state));
    });
}

future<>
storage_proxy::mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl,
    clock_type::time_point timeout,
    bool should_mutate_atomically, tracing::trace_state_ptr tr_state, bool raw_counters) {
    warn(unimplemented::cause::TRIGGERS);
    if (should_mutate_atomically) {
        assert(!raw_counters);
        return mutate_atomically(std::move(mutations), cl, timeout, std::move(tr_state));
    }
    return mutate(std::move(mutations), cl, timeout, std::move(tr_state), raw_counters);
}

/**
 * See mutate. Adds additional steps before and after writing a batch.
 * Before writing the batch (but after doing availability check against the FD for the row replicas):
 *      write the entire batch to a batchlog elsewhere in the cluster.
 * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
 *
 * @param mutations the Mutations to be applied across the replicas
 * @param consistency_level the consistency level for the operation
 */
future<>
storage_proxy::mutate_atomically(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state) {

    utils::latency_counter lc;
    lc.start();

    class context {
        storage_proxy& _p;
        std::vector<mutation> _mutations;
        db::consistency_level _cl;
        clock_type::time_point _timeout;
        tracing::trace_state_ptr _trace_state;
        storage_proxy::stats& _stats;

        const utils::UUID _batch_uuid;
        const std::unordered_set<gms::inet_address> _batchlog_endpoints;

    public:
        context(storage_proxy & p, std::vector<mutation>&& mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state)
                : _p(p)
                , _mutations(std::move(mutations))
                , _cl(cl)
                , _timeout(timeout)
                , _trace_state(std::move(tr_state))
                , _stats(p._stats)
                , _batch_uuid(utils::UUID_gen::get_time_UUID())
                , _batchlog_endpoints(
                        [this]() -> std::unordered_set<gms::inet_address> {
                            auto local_addr = utils::fb_utilities::get_broadcast_address();
                            auto& topology = service::get_storage_service().local().get_token_metadata().get_topology();
                            auto& local_endpoints = topology.get_datacenter_racks().at(get_local_dc());
                            auto local_rack = locator::i_endpoint_snitch::get_local_snitch_ptr()->get_rack(local_addr);
                            auto chosen_endpoints = db::get_batchlog_manager().local().endpoint_filter(local_rack, local_endpoints);

                            if (chosen_endpoints.empty()) {
                                if (_cl == db::consistency_level::ANY) {
                                    return {local_addr};
                                }
                                throw exceptions::unavailable_exception(db::consistency_level::ONE, 1, 0);
                            }
                            return chosen_endpoints;
                        }()) {
                tracing::trace(_trace_state, "Created a batch context");
                tracing::set_batchlog_endpoints(_trace_state, _batchlog_endpoints);
        }

        future<> send_batchlog_mutation(mutation m, db::consistency_level cl = db::consistency_level::ONE) {
            return _p.mutate_prepare<>(std::array<mutation, 1>{std::move(m)}, cl, db::write_type::BATCH_LOG, [this] (const mutation& m, db::consistency_level cl, db::write_type type) {
                auto& ks = _p._db.local().find_keyspace(m.schema()->ks_name());
                return _p.create_write_response_handler(ks, cl, type, std::make_unique<shared_mutation>(m), _batchlog_endpoints, {}, {}, _trace_state, _stats);
            }).then([this, cl] (std::vector<unique_response_handler> ids) {
                return _p.mutate_begin(std::move(ids), cl, _timeout);
            });
        }
        future<> sync_write_to_batchlog() {
            auto m = db::get_batchlog_manager().local().get_batch_log_mutation_for(_mutations, _batch_uuid, netw::messaging_service::current_version);
            tracing::trace(_trace_state, "Sending a batchlog write mutation");
            return send_batchlog_mutation(std::move(m));
        };
        future<> async_remove_from_batchlog() {
            // delete batch
            auto schema = _p._db.local().find_schema(db::system_keyspace::NAME, db::system_keyspace::BATCHLOG);
            auto key = partition_key::from_exploded(*schema, {uuid_type->decompose(_batch_uuid)});
            auto now = service::client_state(service::client_state::internal_tag()).get_timestamp();
            mutation m(schema, key);
            m.partition().apply_delete(*schema, clustering_key_prefix::make_empty(), tombstone(now, gc_clock::now()));

            tracing::trace(_trace_state, "Sending a batchlog remove mutation");
            return send_batchlog_mutation(std::move(m), db::consistency_level::ANY).handle_exception([] (std::exception_ptr eptr) {
                slogger.error("Failed to remove mutations from batchlog: {}", eptr);
            });
        };

        future<> run() {
            return _p.mutate_prepare(_mutations, _cl, db::write_type::BATCH, _trace_state).then([this] (std::vector<unique_response_handler> ids) {
                return sync_write_to_batchlog().then([this, ids = std::move(ids)] () mutable {
                    tracing::trace(_trace_state, "Sending batch mutations");
                    return _p.mutate_begin(std::move(ids), _cl, _timeout);
                }).then(std::bind(&context::async_remove_from_batchlog, this));
            });
        }
    };

    auto mk_ctxt = [this, tr_state, timeout] (std::vector<mutation> mutations, db::consistency_level cl) mutable {
      try {
          return make_ready_future<lw_shared_ptr<context>>(make_lw_shared<context>(*this, std::move(mutations), cl, timeout, std::move(tr_state)));
      } catch(...) {
          return make_exception_future<lw_shared_ptr<context>>(std::current_exception());
      }
    };

    return mk_ctxt(std::move(mutations), cl).then([this] (lw_shared_ptr<context> ctxt) {
        return ctxt->run().finally([ctxt]{});
    }).then_wrapped([p = shared_from_this(), lc, tr_state = std::move(tr_state)] (future<> f) mutable {
        return p->mutate_end(std::move(f), lc, p->_stats, std::move(tr_state));
    });
}

template<typename Range>
bool storage_proxy::cannot_hint(const Range& targets, db::write_type type) {
    // if hints are disabled we "can always hint" since there's going to be no hint generated in this case
    return hints_enabled(type) && boost::algorithm::any_of(targets, std::bind(&db::hints::manager::too_many_in_flight_hints_for, &*_hints_manager, std::placeholders::_1));
}

future<> storage_proxy::send_to_endpoint(
        std::unique_ptr<mutation_holder> m,
        gms::inet_address target,
        std::vector<gms::inet_address> pending_endpoints,
        db::write_type type,
        write_stats& stats,
        allow_hints allow_hints) {
    utils::latency_counter lc;
    lc.start();

    std::optional<clock_type::time_point> timeout;
    db::consistency_level cl = allow_hints ? db::consistency_level::ANY : db::consistency_level::ONE;
    if (type == db::write_type::VIEW) {
        // View updates have a near-infinite timeout to avoid incurring the extra work of writting hints
        // and to apply backpressure.
        timeout = clock_type::now() + 5min;
    }
    return mutate_prepare(std::array{std::move(m)}, cl, type,
            [this, target = std::array{target}, pending_endpoints = std::move(pending_endpoints), &stats] (
                std::unique_ptr<mutation_holder>& m,
                db::consistency_level cl,
                db::write_type type) mutable {
        std::unordered_set<gms::inet_address> targets;
        targets.reserve(pending_endpoints.size() + 1);
        std::vector<gms::inet_address> dead_endpoints;
        boost::algorithm::partition_copy(
                boost::range::join(pending_endpoints, target),
                std::inserter(targets, targets.begin()),
                std::back_inserter(dead_endpoints),
                [] (gms::inet_address ep) { return gms::get_local_gossiper().is_alive(ep); });
        auto& ks = _db.local().find_keyspace(m->schema()->ks_name());
        slogger.trace("Creating write handler with live: {}; dead: {}", targets, dead_endpoints);
        db::assure_sufficient_live_nodes(cl, ks, targets, pending_endpoints);
        return create_write_response_handler(
            ks,
            cl,
            type,
            std::move(m),
            std::move(targets),
            pending_endpoints,
            std::move(dead_endpoints),
            nullptr,
            stats);
    }).then([this, cl, timeout = std::move(timeout)] (std::vector<unique_response_handler> ids) mutable {
        return mutate_begin(std::move(ids), cl, std::move(timeout));
    }).then_wrapped([p = shared_from_this(), lc, &stats] (future<>&& f) {
        return p->mutate_end(std::move(f), lc, stats, nullptr);
    });
}

future<> storage_proxy::send_to_endpoint(
        frozen_mutation_and_schema fm_a_s,
        gms::inet_address target,
        std::vector<gms::inet_address> pending_endpoints,
        db::write_type type,
        allow_hints allow_hints) {
    return send_to_endpoint(
            std::make_unique<shared_mutation>(std::move(fm_a_s)),
            std::move(target),
            std::move(pending_endpoints),
            type,
            _stats,
            allow_hints);
}

future<> storage_proxy::send_to_endpoint(
        frozen_mutation_and_schema fm_a_s,
        gms::inet_address target,
        std::vector<gms::inet_address> pending_endpoints,
        db::write_type type,
        write_stats& stats,
        allow_hints allow_hints) {
    return send_to_endpoint(
            std::make_unique<shared_mutation>(std::move(fm_a_s)),
            std::move(target),
            std::move(pending_endpoints),
            type,
            stats,
            allow_hints);
}

/**
 * Send the mutations to the right targets, write it locally if it corresponds or writes a hint when the node
 * is not available.
 *
 * Note about hints:
 *
 * | Hinted Handoff | Consist. Level |
 * | on             |       >=1      | --> wait for hints. We DO NOT notify the handler with handler.response() for hints;
 * | on             |       ANY      | --> wait for hints. Responses count towards consistency.
 * | off            |       >=1      | --> DO NOT fire hints. And DO NOT wait for them to complete.
 * | off            |       ANY      | --> DO NOT fire hints. And DO NOT wait for them to complete.
 *
 * @throws OverloadedException if the hints cannot be written/enqueued
 */
 // returned future is ready when sent is complete, not when mutation is executed on all (or any) targets!
void storage_proxy::send_to_live_endpoints(storage_proxy::response_id_type response_id, clock_type::time_point timeout)
{
    // extra-datacenter replicas, grouped by dc
    std::unordered_map<sstring, std::vector<gms::inet_address>> dc_groups;
    std::vector<std::pair<const sstring, std::vector<gms::inet_address>>> local;
    local.reserve(3);

    auto handler_ptr = get_write_response_handler(response_id);
    auto& stats = handler_ptr->stats();
    auto& handler = *handler_ptr;

    for(auto dest: handler.get_targets()) {
        sstring dc = get_dc(dest);
        // read repair writes do not go through coordinator since mutations are per destination
        if (handler.read_repair_write() || dc == get_local_dc()) {
            local.emplace_back("", std::vector<gms::inet_address>({dest}));
        } else {
            dc_groups[dc].push_back(dest);
        }
    }

    auto all = boost::range::join(local, dc_groups);
    auto my_address = utils::fb_utilities::get_broadcast_address();

    // lambda for applying mutation locally
    auto lmutate = [handler_ptr, response_id, this, my_address, timeout] (lw_shared_ptr<const frozen_mutation> m) mutable {
        tracing::trace(handler_ptr->get_trace_state(), "Executing a mutation locally");
        auto s = handler_ptr->get_schema();
        return mutate_locally(std::move(s), *m, timeout).then([response_id, this, my_address, m, h = std::move(handler_ptr), p = shared_from_this()] {
            // make mutation alive until it is processed locally, otherwise it
            // may disappear if write timeouts before this future is ready
            got_response(response_id, my_address, get_view_update_backlog());
        });
    };

    // lambda for applying mutation remotely
    auto rmutate = [this, handler_ptr, timeout, response_id, my_address, &stats] (gms::inet_address coordinator, std::vector<gms::inet_address>&& forward, const frozen_mutation& m) {
        auto& ms = netw::get_local_messaging_service();
        auto msize = m.representation().size();
        stats.queued_write_bytes += msize;

        auto& tr_state = handler_ptr->get_trace_state();
        tracing::trace(tr_state, "Sending a mutation to /{}", coordinator);

        return ms.send_mutation(netw::messaging_service::msg_addr{coordinator, 0}, timeout, m,
                std::move(forward), my_address, engine().cpu_id(), response_id, tracing::make_trace_info(tr_state)).finally([this, p = shared_from_this(), h = std::move(handler_ptr), msize, &stats] {
            stats.queued_write_bytes -= msize;
            unthrottle();
        });
    };

    // OK, now send and/or apply locally
    for (typename decltype(dc_groups)::value_type& dc_targets : all) {
        auto& forward = dc_targets.second;
        // last one in forward list is a coordinator
        auto coordinator = forward.back();
        forward.pop_back();

        size_t forward_size = forward.size();
        future<> f = make_ready_future<>();


        lw_shared_ptr<const frozen_mutation> m = handler.get_mutation_for(coordinator);

        if (!m || (handler.is_counter() && coordinator == my_address)) {
            got_response(response_id, coordinator, std::nullopt);
        } else {
            if (!handler.read_repair_write()) {
                ++stats.writes_attempts.get_ep_stat(coordinator);
            } else {
                ++stats.read_repair_write_attempts.get_ep_stat(coordinator);
            }

            if (coordinator == my_address) {
                f = futurize<void>::apply(lmutate, std::move(m));
            } else {
                f = futurize<void>::apply(rmutate, coordinator, std::move(forward), *m);
            }
        }

        f.handle_exception([response_id, forward_size, coordinator, handler_ptr, p = shared_from_this(), &stats] (std::exception_ptr eptr) {
            ++stats.writes_errors.get_ep_stat(coordinator);
            p->got_failure_response(response_id, coordinator, forward_size + 1, std::nullopt);
            try {
                std::rethrow_exception(eptr);
            } catch(rpc::closed_error&) {
                // ignore, disconnect will be logged by gossiper
            } catch(seastar::gate_closed_exception&) {
                // may happen during shutdown, ignore it
            } catch(timed_out_error&) {
                // from lmutate(). Ignore so that logs are not flooded
                // database total_writes_timedout counter was incremented.
            } catch(...) {
                slogger.error("exception during mutation write to {}: {}", coordinator, std::current_exception());
            }
        });
    }
}

// returns number of hints stored
template<typename Range>
size_t storage_proxy::hint_to_dead_endpoints(std::unique_ptr<mutation_holder>& mh, const Range& targets, db::write_type type, tracing::trace_state_ptr tr_state) noexcept
{
    if (hints_enabled(type)) {
        db::hints::manager& hints_manager = hints_manager_for(type);
        return boost::count_if(targets, [this, &mh, tr_state = std::move(tr_state), &hints_manager] (gms::inet_address target) mutable -> bool {
            auto m = mh->get_mutation_for(target);
            if (!m) {
                return 0;
            }
            return hints_manager.store_hint(target, mh->schema(), std::move(m), tr_state);
        });
    } else {
        return 0;
    }
}

future<> storage_proxy::schedule_repair(std::unordered_map<dht::token, std::unordered_map<gms::inet_address, std::optional<mutation>>> diffs, db::consistency_level cl, tracing::trace_state_ptr trace_state) {
    if (diffs.empty()) {
        return make_ready_future<>();
    }
    return mutate_internal(diffs | boost::adaptors::map_values, cl, false, std::move(trace_state));
}

class abstract_read_resolver {
protected:
    db::consistency_level _cl;
    size_t _targets_count;
    promise<> _done_promise; // all target responded
    bool _request_failed = false; // will be true if request fails or timeouts
    timer<storage_proxy::clock_type> _timeout;
    schema_ptr _schema;
    size_t _failed = 0;

    virtual void on_failure(std::exception_ptr ex) = 0;
    virtual void on_timeout() = 0;
    virtual size_t response_count() const = 0;
    virtual void fail_request(std::exception_ptr ex) {
        _request_failed = true;
        _done_promise.set_exception(ex);
        _timeout.cancel();
        on_failure(ex);
    }
public:
    abstract_read_resolver(schema_ptr schema, db::consistency_level cl, size_t target_count, storage_proxy::clock_type::time_point timeout)
        : _cl(cl)
        , _targets_count(target_count)
        , _schema(std::move(schema))
    {
        _timeout.set_callback([this] {
            on_timeout();
        });
        _timeout.arm(timeout);
    }
    virtual ~abstract_read_resolver() {};
    virtual void on_error(gms::inet_address ep, bool disconnect) = 0;
    future<> done() {
        return _done_promise.get_future();
    }
    void error(gms::inet_address ep, std::exception_ptr eptr) {
        sstring why;
        bool disconnect = false;
        try {
            std::rethrow_exception(eptr);
        } catch (rpc::closed_error&) {
            // do not report connection closed exception, gossiper does that
            disconnect = true;
        } catch (rpc::timeout_error&) {
            // do not report timeouts, the whole operation will timeout and be reported
            return; // also do not report timeout as replica failure for the same reason
        } catch(...) {
            slogger.error("Exception when communicating with {}: {}", ep, eptr);
        }

        if (!_request_failed) { // request may fail only once.
            on_error(ep, disconnect);
        }
    }
};

class digest_read_resolver : public abstract_read_resolver {
    size_t _block_for;
    size_t _cl_responses = 0;
    promise<foreign_ptr<lw_shared_ptr<query::result>>, bool> _cl_promise; // cl is reached
    bool _cl_reported = false;
    foreign_ptr<lw_shared_ptr<query::result>> _data_result;
    std::vector<query::result_digest> _digest_results;
    api::timestamp_type _last_modified = api::missing_timestamp;
    size_t _target_count_for_cl; // _target_count_for_cl < _targets_count if CL=LOCAL and RRD.GLOBAL

    void on_timeout() override {
        fail_request(std::make_exception_ptr(read_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl, _cl_responses, _block_for, _data_result)));
    }
    void on_failure(std::exception_ptr ex) override {
        if (!_cl_reported) {
            _cl_promise.set_exception(ex);
        }
        // we will not need them any more
        _data_result = foreign_ptr<lw_shared_ptr<query::result>>();
        _digest_results.clear();
    }
    virtual size_t response_count() const override {
        return _digest_results.size();
    }
public:
    digest_read_resolver(schema_ptr schema, db::consistency_level cl, size_t block_for, size_t target_count_for_cl, storage_proxy::clock_type::time_point timeout) : abstract_read_resolver(std::move(schema), cl, 0, timeout),
                                _block_for(block_for),  _target_count_for_cl(target_count_for_cl) {}
    void add_data(gms::inet_address from, foreign_ptr<lw_shared_ptr<query::result>> result) {
        if (!_request_failed) {
            // if only one target was queried digest_check() will be skipped so we can also skip digest calculation
            _digest_results.emplace_back(_targets_count == 1 ? query::result_digest() : *result->digest());
            _last_modified = std::max(_last_modified, result->last_modified());
            if (!_data_result) {
                _data_result = std::move(result);
            }
            got_response(from);
        }
    }
    void add_digest(gms::inet_address from, query::result_digest digest, api::timestamp_type last_modified) {
        if (!_request_failed) {
            _digest_results.emplace_back(std::move(digest));
            _last_modified = std::max(_last_modified, last_modified);
            got_response(from);
        }
    }
    bool digests_match() const {
        assert(response_count());
        if (response_count() == 1) {
            return true;
        }
        auto& first = *_digest_results.begin();
        return std::find_if(_digest_results.begin() + 1, _digest_results.end(), [&first] (query::result_digest digest) { return digest != first; }) == _digest_results.end();
    }
    bool waiting_for(gms::inet_address ep) {
        return db::is_datacenter_local(_cl) ? fbu::is_me(ep) || db::is_local(ep) : true;
    }
    void got_response(gms::inet_address ep) {
        if (!_cl_reported) {
            if (waiting_for(ep)) {
                _cl_responses++;
            }
            if (_cl_responses >= _block_for && _data_result) {
                _cl_reported = true;
                _cl_promise.set_value(std::move(_data_result), digests_match());
            }
        }
        if (is_completed()) {
            _timeout.cancel();
            _done_promise.set_value();
        }
    }
    void on_error(gms::inet_address ep, bool disconnect) override {
        if (waiting_for(ep)) {
            _failed++;
        }
        if (disconnect && _block_for == _target_count_for_cl) {
            // if the error is because of a connection disconnect and there is no targets to speculate
            // wait for timeout in hope that the client will issue speculative read
            // FIXME: resolver should have access to all replicas and try another one in this case
            return;
        }
        if (_block_for + _failed > _target_count_for_cl) {
            fail_request(std::make_exception_ptr(read_failure_exception(_schema->ks_name(), _schema->cf_name(), _cl, _cl_responses, _failed, _block_for, _data_result)));
        }
    }
    future<foreign_ptr<lw_shared_ptr<query::result>>, bool> has_cl() {
        return _cl_promise.get_future();
    }
    bool has_data() {
        return _data_result;
    }
    void add_wait_targets(size_t targets_count) {
        _targets_count += targets_count;
    }
    bool is_completed() {
        return response_count() == _targets_count;
    }
    api::timestamp_type last_modified() const {
        return _last_modified;
    }
};

class data_read_resolver : public abstract_read_resolver {
    struct reply {
        gms::inet_address from;
        foreign_ptr<lw_shared_ptr<reconcilable_result>> result;
        bool reached_end = false;
        reply(gms::inet_address from_, foreign_ptr<lw_shared_ptr<reconcilable_result>> result_) : from(std::move(from_)), result(std::move(result_)) {}
    };
    struct version {
        gms::inet_address from;
        std::optional<partition> par;
        bool reached_end;
        bool reached_partition_end;
        version(gms::inet_address from_, std::optional<partition> par_, bool reached_end, bool reached_partition_end)
                : from(std::move(from_)), par(std::move(par_)), reached_end(reached_end), reached_partition_end(reached_partition_end) {}
    };
    struct mutation_and_live_row_count {
        mutation mut;
        size_t live_row_count;
    };

    struct primary_key {
        dht::decorated_key partition;
        std::optional<clustering_key> clustering;

        class less_compare_clustering {
            bool _is_reversed;
            clustering_key::less_compare _ck_cmp;
        public:
            less_compare_clustering(const schema& s, bool is_reversed)
                : _is_reversed(is_reversed), _ck_cmp(s) { }

            bool operator()(const primary_key& a, const primary_key& b) const {
                if (!b.clustering) {
                    return false;
                }
                if (!a.clustering) {
                    return true;
                }
                if (_is_reversed) {
                    return _ck_cmp(*b.clustering, *a.clustering);
                } else {
                    return _ck_cmp(*a.clustering, *b.clustering);
                }
            }
        };

        class less_compare {
            const schema& _schema;
            less_compare_clustering _ck_cmp;
        public:
            less_compare(const schema& s, bool is_reversed)
                : _schema(s), _ck_cmp(s, is_reversed) { }

            bool operator()(const primary_key& a, const primary_key& b) const {
                auto pk_result = a.partition.tri_compare(_schema, b.partition);
                if (pk_result) {
                    return pk_result < 0;
                }
                return _ck_cmp(a, b);
            }
        };
    };

    size_t _total_live_count = 0;
    uint32_t _max_live_count = 0;
    uint32_t _short_read_diff = 0;
    uint32_t _max_per_partition_live_count = 0;
    uint32_t _partition_count = 0;
    uint32_t _live_partition_count = 0;
    bool _increase_per_partition_limit = false;
    bool _all_reached_end = true;
    query::short_read _is_short_read;
    std::vector<reply> _data_results;
    std::unordered_map<dht::token, std::unordered_map<gms::inet_address, std::optional<mutation>>> _diffs;
private:
    void on_timeout() override {
        fail_request(std::make_exception_ptr(read_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl, response_count(), _targets_count, response_count() != 0)));
    }
    void on_failure(std::exception_ptr ex) override {
        // we will not need them any more
        _data_results.clear();
    }

    virtual size_t response_count() const override {
        return _data_results.size();
    }

    void register_live_count(const std::vector<version>& replica_versions, uint32_t reconciled_live_rows, uint32_t limit) {
        bool any_not_at_end = boost::algorithm::any_of(replica_versions, [] (const version& v) {
            return !v.reached_partition_end;
        });
        if (any_not_at_end && reconciled_live_rows < limit && limit - reconciled_live_rows > _short_read_diff) {
            _short_read_diff = limit - reconciled_live_rows;
            _max_per_partition_live_count = reconciled_live_rows;
        }
    }
    void find_short_partitions(const std::vector<mutation_and_live_row_count>& rp, const std::vector<std::vector<version>>& versions,
                               uint32_t per_partition_limit, uint32_t row_limit, uint32_t partition_limit) {
        // Go through the partitions that weren't limited by the total row limit
        // and check whether we got enough rows to satisfy per-partition row
        // limit.
        auto partitions_left = partition_limit;
        auto rows_left = row_limit;
        auto pv = versions.rbegin();
        for (auto&& m_a_rc : rp | boost::adaptors::reversed) {
            auto row_count = m_a_rc.live_row_count;
            if (row_count < rows_left && partitions_left) {
                rows_left -= row_count;
                partitions_left -= !!row_count;
                register_live_count(*pv, row_count, per_partition_limit);
            } else {
                break;
            }
            ++pv;
        }
    }

    static primary_key get_last_row(const schema& s, const partition& p, bool is_reversed) {
        return {p.mut().decorated_key(s), is_reversed ? p.mut().partition().first_row_key() : p.mut().partition().last_row_key()  };
    }

    // Returns the highest row sent by the specified replica, according to the schema and the direction of
    // the query.
    // versions is a table where rows are partitions in descending order and the columns identify the partition
    // sent by a particular replica.
    static primary_key get_last_row(const schema& s, bool is_reversed, const std::vector<std::vector<version>>& versions, uint32_t replica) {
        const partition* last_partition = nullptr;
        // Versions are in the reversed order.
        for (auto&& pv : versions) {
            const std::optional<partition>& p = pv[replica].par;
            if (p) {
                last_partition = &p.value();
                break;
            }
        }
        assert(last_partition);
        return get_last_row(s, *last_partition, is_reversed);
    }

    static primary_key get_last_reconciled_row(const schema& s, const mutation_and_live_row_count& m_a_rc, const query::read_command& cmd, uint32_t limit, bool is_reversed) {
        const auto& m = m_a_rc.mut;
        auto mp = mutation_partition(s, m.partition());
        auto&& ranges = cmd.slice.row_ranges(s, m.key());
        mp.compact_for_query(s, cmd.timestamp, ranges, is_reversed, limit);

        std::optional<clustering_key> ck;
        if (!mp.clustered_rows().empty()) {
            if (is_reversed) {
                ck = mp.clustered_rows().begin()->key();
            } else {
                ck = mp.clustered_rows().rbegin()->key();
            }
        }
        return primary_key { m.decorated_key(), ck };
    }

    static bool got_incomplete_information_in_partition(const schema& s, const primary_key& last_reconciled_row, const std::vector<version>& versions, bool is_reversed) {
        primary_key::less_compare_clustering ck_cmp(s, is_reversed);
        for (auto&& v : versions) {
            if (!v.par || v.reached_partition_end) {
                continue;
            }
            auto replica_last_row = get_last_row(s, *v.par, is_reversed);
            if (ck_cmp(replica_last_row, last_reconciled_row)) {
                return true;
            }
        }
        return false;
    }

    bool got_incomplete_information_across_partitions(const schema& s, const query::read_command& cmd,
                                                      const primary_key& last_reconciled_row, std::vector<mutation_and_live_row_count>& rp,
                                                      const std::vector<std::vector<version>>& versions, bool is_reversed) {
        bool short_reads_allowed = cmd.slice.options.contains<query::partition_slice::option::allow_short_read>();
        primary_key::less_compare cmp(s, is_reversed);
        std::optional<primary_key> shortest_read;
        auto num_replicas = versions[0].size();
        for (uint32_t i = 0; i < num_replicas; ++i) {
            if (versions.front()[i].reached_end) {
                continue;
            }
            auto replica_last_row = get_last_row(s, is_reversed, versions, i);
            if (cmp(replica_last_row, last_reconciled_row)) {
                if (short_reads_allowed) {
                    if (!shortest_read || cmp(replica_last_row, *shortest_read)) {
                        shortest_read = std::move(replica_last_row);
                    }
                } else {
                    return true;
                }
            }
        }

        // Short reads are allowed, trim the reconciled result.
        if (shortest_read) {
            _is_short_read = query::short_read::yes;

            // Prepare to remove all partitions past shortest_read
            auto it = rp.begin();
            for (; it != rp.end() && shortest_read->partition.less_compare(s, it->mut.decorated_key()); ++it) { }

            // Remove all clustering rows past shortest_read
            if (it != rp.end() && it->mut.decorated_key().equal(s, shortest_read->partition)) {
                if (!shortest_read->clustering) {
                    ++it;
                } else {
                    std::vector<query::clustering_range> ranges;
                    ranges.emplace_back(is_reversed ? query::clustering_range::make_starting_with(std::move(*shortest_read->clustering))
                                                    : query::clustering_range::make_ending_with(std::move(*shortest_read->clustering)));
                    it->live_row_count = it->mut.partition().compact_for_query(s, cmd.timestamp, ranges, is_reversed, query::max_rows);
                }
            }

            // Actually remove all partitions past shortest_read
            rp.erase(rp.begin(), it);

            // Update total live count and live partition count
            _live_partition_count = 0;
            _total_live_count = boost::accumulate(rp, uint32_t(0), [this] (uint32_t lc, const mutation_and_live_row_count& m_a_rc) {
                _live_partition_count += !!m_a_rc.live_row_count;
                return lc + m_a_rc.live_row_count;
            });
        }

        return false;
    }

    bool got_incomplete_information(const schema& s, const query::read_command& cmd, uint32_t original_row_limit, uint32_t original_per_partition_limit,
                            uint32_t original_partition_limit, std::vector<mutation_and_live_row_count>& rp, const std::vector<std::vector<version>>& versions) {
        // We need to check whether the reconciled result contains all information from all available
        // replicas. It is possible that some of the nodes have returned less rows (because the limit
        // was set and they had some tombstones missing) than the others. In such cases we cannot just
        // merge all results and return that to the client as the replicas that returned less row
        // may have newer data for the rows they did not send than any other node in the cluster.
        //
        // This function is responsible for detecting whether such problem may happen. We get partition
        // and clustering keys of the last row that is going to be returned to the client and check if
        // it is in range of rows returned by each replicas that returned as many rows as they were
        // asked for (if a replica returned less rows it means it returned everything it has).
        auto is_reversed = cmd.slice.options.contains(query::partition_slice::option::reversed);

        auto rows_left = original_row_limit;
        auto partitions_left = original_partition_limit;
        auto pv = versions.rbegin();
        for (auto&& m_a_rc : rp | boost::adaptors::reversed) {
            auto row_count = m_a_rc.live_row_count;
            if (row_count < rows_left && partitions_left > !!row_count) {
                rows_left -= row_count;
                partitions_left -= !!row_count;
                if (original_per_partition_limit != query::max_rows) {
                    auto&& last_row = get_last_reconciled_row(s, m_a_rc, cmd, original_per_partition_limit, is_reversed);
                    if (got_incomplete_information_in_partition(s, last_row, *pv, is_reversed)) {
                        _increase_per_partition_limit = true;
                        return true;
                    }
                }
            } else {
                auto&& last_row = get_last_reconciled_row(s, m_a_rc, cmd, rows_left, is_reversed);
                return got_incomplete_information_across_partitions(s, cmd, last_row, rp, versions, is_reversed);
            }
            ++pv;
        }
        return false;
    }
public:
    data_read_resolver(schema_ptr schema, db::consistency_level cl, size_t targets_count, storage_proxy::clock_type::time_point timeout) : abstract_read_resolver(std::move(schema), cl, targets_count, timeout) {
        _data_results.reserve(targets_count);
    }
    void add_mutate_data(gms::inet_address from, foreign_ptr<lw_shared_ptr<reconcilable_result>> result) {
        if (!_request_failed) {
            _max_live_count = std::max(result->row_count(), _max_live_count);
            _data_results.emplace_back(std::move(from), std::move(result));
            if (_data_results.size() == _targets_count) {
                _timeout.cancel();
                _done_promise.set_value();
            }
        }
    }
    void on_error(gms::inet_address ep, bool disconnect) override {
        fail_request(std::make_exception_ptr(read_failure_exception(_schema->ks_name(), _schema->cf_name(), _cl, response_count(), 1, _targets_count, response_count() != 0)));
    }
    uint32_t max_live_count() const {
        return _max_live_count;
    }
    bool any_partition_short_read() const {
        return _short_read_diff > 0;
    }
    bool increase_per_partition_limit() const {
        return _increase_per_partition_limit;
    }
    uint32_t max_per_partition_live_count() const {
        return _max_per_partition_live_count;
    }
    uint32_t partition_count() const {
        return _partition_count;
    }
    uint32_t live_partition_count() const {
        return _live_partition_count;
    }
    bool all_reached_end() const {
        return _all_reached_end;
    }
    std::optional<reconcilable_result> resolve(schema_ptr schema, const query::read_command& cmd, uint32_t original_row_limit, uint32_t original_per_partition_limit,
            uint32_t original_partition_limit) {
        assert(_data_results.size());

        if (_data_results.size() == 1) {
            // if there is a result only from one node there is nothing to reconcile
            // should happen only for range reads since single key reads will not
            // try to reconcile for CL=ONE
            auto& p = _data_results[0].result;
            return reconcilable_result(p->row_count(), p->partitions(), p->is_short_read());
        }

        const auto& s = *schema;

        // return true if lh > rh
        auto cmp = [&s](reply& lh, reply& rh) {
            if (lh.result->partitions().size() == 0) {
                return false; // reply with empty partition array goes to the end of the sorted array
            } else if (rh.result->partitions().size() == 0) {
                return true;
            } else {
                auto lhk = lh.result->partitions().back().mut().key(s);
                auto rhk = rh.result->partitions().back().mut().key(s);
                return lhk.ring_order_tri_compare(s, rhk) > 0;
            }
        };

        // this array will have an entry for each partition which will hold all available versions
        std::vector<std::vector<version>> versions;
        versions.reserve(_data_results.front().result->partitions().size());

        for (auto& r : _data_results) {
            _is_short_read = _is_short_read || r.result->is_short_read();
            r.reached_end = !r.result->is_short_read() && r.result->row_count() < cmd.row_limit
                            && (cmd.partition_limit == query::max_partitions
                                || boost::range::count_if(r.result->partitions(), [] (const partition& p) {
                                    return p.row_count();
                                }) < cmd.partition_limit);
            _all_reached_end = _all_reached_end && r.reached_end;
        }

        do {
            // after this sort reply with largest key is at the beginning
            boost::sort(_data_results, cmp);
            if (_data_results.front().result->partitions().empty()) {
                break; // if top of the heap is empty all others are empty too
            }
            const auto& max_key = _data_results.front().result->partitions().back().mut().key(s);
            versions.emplace_back();
            std::vector<version>& v = versions.back();
            v.reserve(_targets_count);
            for (reply& r : _data_results) {
                auto pit = r.result->partitions().rbegin();
                if (pit != r.result->partitions().rend() && pit->mut().key(s).legacy_equal(s, max_key)) {
                    bool reached_partition_end = pit->row_count() < cmd.slice.partition_row_limit();
                    v.emplace_back(r.from, std::move(*pit), r.reached_end, reached_partition_end);
                    r.result->partitions().pop_back();
                } else {
                    // put empty partition for destination without result
                    v.emplace_back(r.from, std::optional<partition>(), r.reached_end, true);
                }
            }

            boost::sort(v, [] (const version& x, const version& y) {
                return x.from < y.from;
            });
        } while(true);

        std::vector<mutation_and_live_row_count> reconciled_partitions;
        reconciled_partitions.reserve(versions.size());

        // reconcile all versions
        boost::range::transform(boost::make_iterator_range(versions.begin(), versions.end()), std::back_inserter(reconciled_partitions),
                                [this, schema, original_per_partition_limit] (std::vector<version>& v) {
            auto it = boost::range::find_if(v, [] (auto&& ver) {
                    return bool(ver.par);
            });
            auto m = boost::accumulate(v, mutation(schema, it->par->mut().key(*schema)), [this, schema] (mutation& m, const version& ver) {
                if (ver.par) {
                    m.partition().apply(*schema, ver.par->mut().partition(), *schema);
                }
                return std::move(m);
            });
            auto live_row_count = m.live_row_count();
            _total_live_count += live_row_count;
            _live_partition_count += !!live_row_count;
            return mutation_and_live_row_count { std::move(m), live_row_count };
        });
        _partition_count = reconciled_partitions.size();

        bool has_diff = false;

        // calculate differences
        for (auto z : boost::combine(versions, reconciled_partitions)) {
            const mutation& m = z.get<1>().mut;
            for (const version& v : z.get<0>()) {
                auto diff = v.par
                          ? m.partition().difference(schema, v.par->mut().unfreeze(schema).partition())
                          : mutation_partition(*schema, m.partition());
                auto it = _diffs[m.token()].find(v.from);
                std::optional<mutation> mdiff;
                if (!diff.empty()) {
                    has_diff = true;
                    mdiff = mutation(schema, m.decorated_key(), std::move(diff));
                }
                if (it == _diffs[m.token()].end()) {
                    _diffs[m.token()].emplace(v.from, std::move(mdiff));
                } else {
                    // should not really happen, but lets try to deal with it
                    if (mdiff) {
                        if (it->second) {
                            it->second.value().apply(std::move(mdiff.value()));
                        } else {
                            it->second = std::move(mdiff);
                        }
                    }
                }
            }
        }

        if (has_diff) {
            if (got_incomplete_information(*schema, cmd, original_row_limit, original_per_partition_limit,
                                           original_partition_limit, reconciled_partitions, versions)) {
                return {};
            }
            // filter out partitions with empty diffs
            for (auto it = _diffs.begin(); it != _diffs.end();) {
                if (boost::algorithm::none_of(it->second | boost::adaptors::map_values, std::mem_fn(&std::optional<mutation>::operator bool))) {
                    it = _diffs.erase(it);
                } else {
                    ++it;
                }
            }
        } else {
            _diffs.clear();
        }

        find_short_partitions(reconciled_partitions, versions, original_per_partition_limit, original_row_limit, original_partition_limit);

        bool allow_short_reads = cmd.slice.options.contains<query::partition_slice::option::allow_short_read>();
        if (allow_short_reads && _max_live_count >= original_row_limit && _total_live_count < original_row_limit && _total_live_count) {
            // We ended up with less rows than the client asked for (but at least one),
            // avoid retry and mark as short read instead.
            _is_short_read = query::short_read::yes;
        }

        // build reconcilable_result from reconciled data
        // traverse backwards since large keys are at the start
        std::vector<partition> vec;
        auto r = boost::accumulate(reconciled_partitions | boost::adaptors::reversed, std::ref(vec), [] (std::vector<partition>& a, const mutation_and_live_row_count& m_a_rc) {
            a.emplace_back(partition(m_a_rc.live_row_count, freeze(m_a_rc.mut)));
            return std::ref(a);
        });

        return reconcilable_result(_total_live_count, std::move(r.get()), _is_short_read);
    }
    auto total_live_count() const {
        return _total_live_count;
    }
    auto get_diffs_for_repair() {
        return std::move(_diffs);
    }
};

query::digest_algorithm digest_algorithm() {
    return service::get_local_storage_service().cluster_supports_xxhash_digest_algorithm()
         ? query::digest_algorithm::xxHash
         : query::digest_algorithm::MD5;
}

class abstract_read_executor : public enable_shared_from_this<abstract_read_executor> {
protected:
    using targets_iterator = std::vector<gms::inet_address>::iterator;
    using digest_resolver_ptr = ::shared_ptr<digest_read_resolver>;
    using data_resolver_ptr = ::shared_ptr<data_read_resolver>;
    using clock_type = storage_proxy::clock_type;

    schema_ptr _schema;
    shared_ptr<storage_proxy> _proxy;
    lw_shared_ptr<query::read_command> _cmd;
    lw_shared_ptr<query::read_command> _retry_cmd;
    dht::partition_range _partition_range;
    db::consistency_level _cl;
    size_t _block_for;
    std::vector<gms::inet_address> _targets;
    // Targets that were succesfully used for a data or digest request
    std::vector<gms::inet_address> _used_targets;
    promise<foreign_ptr<lw_shared_ptr<query::result>>> _result_promise;
    tracing::trace_state_ptr _trace_state;
    lw_shared_ptr<column_family> _cf;
    bool _foreground = true;
private:
    void on_read_resolved() noexcept {
        // We could have !_foreground if this is called on behalf of background reconciliation.
        _proxy->_stats.foreground_reads -= int(_foreground);
        _foreground = false;
    }
public:
    abstract_read_executor(schema_ptr s, lw_shared_ptr<column_family> cf, shared_ptr<storage_proxy> proxy, lw_shared_ptr<query::read_command> cmd, dht::partition_range pr, db::consistency_level cl, size_t block_for,
            std::vector<gms::inet_address> targets, tracing::trace_state_ptr trace_state) :
                           _schema(std::move(s)), _proxy(std::move(proxy)), _cmd(std::move(cmd)), _partition_range(std::move(pr)), _cl(cl), _block_for(block_for), _targets(std::move(targets)), _trace_state(std::move(trace_state)),
                           _cf(std::move(cf)) {
        _proxy->_stats.reads++;
        _proxy->_stats.foreground_reads++;
    }
    virtual ~abstract_read_executor() {
        _proxy->_stats.reads--;
        _proxy->_stats.foreground_reads -= int(_foreground);
    }

    /// Targets that were successfully ised for data and/or digest requests.
    ///
    /// Only filled after the request is finished, call only after
    /// execute()'s future is ready.
    std::vector<gms::inet_address> used_targets() const {
        return _used_targets;
    }

protected:
    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> make_mutation_data_request(lw_shared_ptr<query::read_command> cmd, gms::inet_address ep, clock_type::time_point timeout) {
        ++_proxy->_stats.mutation_data_read_attempts.get_ep_stat(ep);
        if (fbu::is_me(ep)) {
            tracing::trace(_trace_state, "read_mutation_data: querying locally");
            return _proxy->query_mutations_locally(_schema, cmd, _partition_range, timeout, _trace_state);
        } else {
            auto& ms = netw::get_local_messaging_service();
            tracing::trace(_trace_state, "read_mutation_data: sending a message to /{}", ep);
            return ms.send_read_mutation_data(netw::messaging_service::msg_addr{ep, 0}, timeout, *cmd, _partition_range).then([this, ep](reconcilable_result&& result, rpc::optional<cache_temperature> hit_rate) {
                tracing::trace(_trace_state, "read_mutation_data: got response from /{}", ep);
                return make_ready_future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>(make_foreign(::make_lw_shared<reconcilable_result>(std::move(result))), hit_rate.value_or(cache_temperature::invalid()));
            });
        }
    }
    future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature> make_data_request(gms::inet_address ep, clock_type::time_point timeout, bool want_digest) {
        ++_proxy->_stats.data_read_attempts.get_ep_stat(ep);
        auto opts = want_digest
                  ? query::result_options{query::result_request::result_and_digest, digest_algorithm()}
                  : query::result_options{query::result_request::only_result, query::digest_algorithm::none};
        if (fbu::is_me(ep)) {
            tracing::trace(_trace_state, "read_data: querying locally");
            return _proxy->query_result_local(_schema, _cmd, _partition_range, opts, _trace_state, timeout);
        } else {
            auto& ms = netw::get_local_messaging_service();
            tracing::trace(_trace_state, "read_data: sending a message to /{}", ep);
            return ms.send_read_data(netw::messaging_service::msg_addr{ep, 0}, timeout, *_cmd, _partition_range, opts.digest_algo).then([this, ep](query::result&& result, rpc::optional<cache_temperature> hit_rate) {
                tracing::trace(_trace_state, "read_data: got response from /{}", ep);
                return make_ready_future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>(make_foreign(::make_lw_shared<query::result>(std::move(result))), hit_rate.value_or(cache_temperature::invalid()));
            });
        }
    }
    future<query::result_digest, api::timestamp_type, cache_temperature> make_digest_request(gms::inet_address ep, clock_type::time_point timeout) {
        ++_proxy->_stats.digest_read_attempts.get_ep_stat(ep);
        if (fbu::is_me(ep)) {
            tracing::trace(_trace_state, "read_digest: querying locally");
            return _proxy->query_result_local_digest(_schema, _cmd, _partition_range, _trace_state, timeout, digest_algorithm());
        } else {
            auto& ms = netw::get_local_messaging_service();
            tracing::trace(_trace_state, "read_digest: sending a message to /{}", ep);
            return ms.send_read_digest(netw::messaging_service::msg_addr{ep, 0}, timeout, *_cmd, _partition_range, digest_algorithm()).then([this, ep] (query::result_digest d, rpc::optional<api::timestamp_type> t,
                    rpc::optional<cache_temperature> hit_rate) {
                tracing::trace(_trace_state, "read_digest: got response from /{}", ep);
                return make_ready_future<query::result_digest, api::timestamp_type, cache_temperature>(d, t ? t.value() : api::missing_timestamp, hit_rate.value_or(cache_temperature::invalid()));
            });
        }
    }
    future<> make_mutation_data_requests(lw_shared_ptr<query::read_command> cmd, data_resolver_ptr resolver, targets_iterator begin, targets_iterator end, clock_type::time_point timeout) {
        return parallel_for_each(begin, end, [this, &cmd, resolver = std::move(resolver), timeout] (gms::inet_address ep) {
            return make_mutation_data_request(cmd, ep, timeout).then_wrapped([this, resolver, ep] (future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> f) {
                try {
                    auto v = f.get();
                    _cf->set_hit_rate(ep, std::get<1>(v));
                    resolver->add_mutate_data(ep, std::get<0>(std::move(v)));
                    ++_proxy->_stats.mutation_data_read_completed.get_ep_stat(ep);
                } catch(...) {
                    ++_proxy->_stats.mutation_data_read_errors.get_ep_stat(ep);
                    resolver->error(ep, std::current_exception());
                }
            });
        });
    }
    future<> make_data_requests(digest_resolver_ptr resolver, targets_iterator begin, targets_iterator end, clock_type::time_point timeout, bool want_digest) {
        return parallel_for_each(begin, end, [this, resolver = std::move(resolver), timeout, want_digest] (gms::inet_address ep) {
            return make_data_request(ep, timeout, want_digest).then_wrapped([this, resolver, ep] (future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature> f) {
                try {
                    auto v = f.get();
                    _cf->set_hit_rate(ep, std::get<1>(v));
                    resolver->add_data(ep, std::get<0>(std::move(v)));
                    ++_proxy->_stats.data_read_completed.get_ep_stat(ep);
                    _used_targets.push_back(ep);
                } catch(...) {
                    ++_proxy->_stats.data_read_errors.get_ep_stat(ep);
                    resolver->error(ep, std::current_exception());
                }
            });
        });
    }
    future<> make_digest_requests(digest_resolver_ptr resolver, targets_iterator begin, targets_iterator end, clock_type::time_point timeout) {
        return parallel_for_each(begin, end, [this, resolver = std::move(resolver), timeout] (gms::inet_address ep) {
            return make_digest_request(ep, timeout).then_wrapped([this, resolver, ep] (future<query::result_digest, api::timestamp_type, cache_temperature> f) {
                try {
                    auto v = f.get();
                    _cf->set_hit_rate(ep, std::get<2>(v));
                    resolver->add_digest(ep, std::get<0>(v), std::get<1>(v));
                    ++_proxy->_stats.digest_read_completed.get_ep_stat(ep);
                    _used_targets.push_back(ep);
                } catch(...) {
                    ++_proxy->_stats.digest_read_errors.get_ep_stat(ep);
                    resolver->error(ep, std::current_exception());
                }
            });
        });
    }
    virtual future<> make_requests(digest_resolver_ptr resolver, clock_type::time_point timeout) {
        resolver->add_wait_targets(_targets.size());
        auto want_digest = _targets.size() > 1;
        auto f_data = futurize_apply([&] { return make_data_requests(resolver, _targets.begin(), _targets.begin() + 1, timeout, want_digest); });
        auto f_digest = futurize_apply([&] { return make_digest_requests(resolver, _targets.begin() + 1, _targets.end(), timeout); });
        return when_all_succeed(std::move(f_data), std::move(f_digest)).handle_exception([] (auto&&) { });
    }
    virtual void got_cl() {}
    uint32_t original_row_limit() const {
        return _cmd->row_limit;
    }
    uint32_t original_per_partition_row_limit() const {
        return _cmd->slice.partition_row_limit();
    }
    uint32_t original_partition_limit() const {
        return _cmd->partition_limit;
    }
    void reconcile(db::consistency_level cl, storage_proxy::clock_type::time_point timeout, lw_shared_ptr<query::read_command> cmd) {
        data_resolver_ptr data_resolver = ::make_shared<data_read_resolver>(_schema, cl, _targets.size(), timeout);
        auto exec = shared_from_this();

        make_mutation_data_requests(cmd, data_resolver, _targets.begin(), _targets.end(), timeout).finally([exec]{});

        data_resolver->done().then_wrapped([this, exec, data_resolver, cmd = std::move(cmd), cl, timeout] (future<> f) {
            try {
                f.get();
                auto rr_opt = data_resolver->resolve(_schema, *cmd, original_row_limit(), original_per_partition_row_limit(), original_partition_limit()); // reconciliation happens here

                // We generate a retry if at least one node reply with count live columns but after merge we have less
                // than the total number of column we are interested in (which may be < count on a retry).
                // So in particular, if no host returned count live columns, we know it's not a short read.
                bool can_send_short_read = rr_opt && rr_opt->is_short_read() && rr_opt->row_count() > 0;
                if (rr_opt && (can_send_short_read || data_resolver->all_reached_end() || rr_opt->row_count() >= original_row_limit()
                               || data_resolver->live_partition_count() >= original_partition_limit())
                        && !data_resolver->any_partition_short_read()) {
                    auto result = ::make_foreign(::make_lw_shared(
                            to_data_query_result(std::move(*rr_opt), _schema, _cmd->slice, _cmd->row_limit, cmd->partition_limit)));
                    // wait for write to complete before returning result to prevent multiple concurrent read requests to
                    // trigger repair multiple times and to prevent quorum read to return an old value, even after a quorum
                    // another read had returned a newer value (but the newer value had not yet been sent to the other replicas)
                    _proxy->schedule_repair(data_resolver->get_diffs_for_repair(), _cl, _trace_state).then([this, result = std::move(result)] () mutable {
                        _result_promise.set_value(std::move(result));
                        on_read_resolved();
                    }).handle_exception([this, exec] (std::exception_ptr eptr) {
                        try {
                            std::rethrow_exception(eptr);
                        } catch (mutation_write_timeout_exception&) {
                            // convert write error to read error
                            _result_promise.set_exception(read_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl, _block_for - 1, _block_for, true));
                        } catch (...) {
                            _result_promise.set_exception(std::current_exception());
                        }
                        on_read_resolved();
                    });
                } else {
                    _proxy->_stats.read_retries++;
                    _retry_cmd = make_lw_shared<query::read_command>(*cmd);
                    // We asked t (= cmd->row_limit) live columns and got l (=data_resolver->total_live_count) ones.
                    // From that, we can estimate that on this row, for x requested
                    // columns, only l/t end up live after reconciliation. So for next
                    // round we want to ask x column so that x * (l/t) == t, i.e. x = t^2/l.
                    auto x = [](uint64_t t, uint64_t l) -> uint32_t {
                        auto ret = std::min(static_cast<uint64_t>(query::max_rows), l == 0 ? t + 1 : ((t * t) / l) + 1);
                        return static_cast<uint32_t>(ret);
                    };
                    if (data_resolver->any_partition_short_read() || data_resolver->increase_per_partition_limit()) {
                        // The number of live rows was bounded by the per partition limit.
                        auto new_limit = x(cmd->slice.partition_row_limit(), data_resolver->max_per_partition_live_count());
                        _retry_cmd->slice.set_partition_row_limit(new_limit);
                        _retry_cmd->row_limit = std::max(cmd->row_limit, data_resolver->partition_count() * new_limit);
                    } else {
                        // The number of live rows was bounded by the total row limit or partition limit.
                        if (cmd->partition_limit != query::max_partitions) {
                            _retry_cmd->partition_limit = x(cmd->partition_limit, data_resolver->live_partition_count());
                        }
                        if (cmd->row_limit != query::max_rows) {
                            _retry_cmd->row_limit = x(cmd->row_limit, data_resolver->total_live_count());
                        }
                    }

                    // We may be unable to send a single live row because of replicas bailing out too early.
                    // If that is the case disallow short reads so that we can make progress.
                    if (!data_resolver->total_live_count()) {
                        _retry_cmd->slice.options.remove<query::partition_slice::option::allow_short_read>();
                    }

                    slogger.trace("Retrying query with command {} (previous is {})", *_retry_cmd, *cmd);
                    reconcile(cl, timeout, _retry_cmd);
                }
            } catch (...) {
                _result_promise.set_exception(std::current_exception());
                on_read_resolved();
            }
        });
    }
    void reconcile(db::consistency_level cl, storage_proxy::clock_type::time_point timeout) {
        reconcile(cl, timeout, _cmd);
    }

public:
    virtual future<foreign_ptr<lw_shared_ptr<query::result>>> execute(storage_proxy::clock_type::time_point timeout) {
        digest_resolver_ptr digest_resolver = ::make_shared<digest_read_resolver>(_schema, _cl, _block_for,
                db::is_datacenter_local(_cl) ? db::count_local_endpoints(_targets): _targets.size(), timeout);
        auto exec = shared_from_this();

        make_requests(digest_resolver, timeout).finally([exec]() {
            // hold on to executor until all queries are complete
        });

        digest_resolver->has_cl().then_wrapped([exec, digest_resolver, timeout] (future<foreign_ptr<lw_shared_ptr<query::result>>, bool> f) mutable {
            bool background_repair_check = false;
            try {
                exec->got_cl();

                foreign_ptr<lw_shared_ptr<query::result>> result;
                bool digests_match;
                std::tie(result, digests_match) = f.get(); // can throw

                if (digests_match) {
                    exec->_result_promise.set_value(std::move(result));
                    if (exec->_block_for < exec->_targets.size()) { // if there are more targets then needed for cl, check digest in background
                        background_repair_check = true;
                    }
                    exec->on_read_resolved();
                } else { // digest mismatch
                    if (is_datacenter_local(exec->_cl)) {
                        auto write_timeout = exec->_proxy->_db.local().get_config().write_request_timeout_in_ms() * 1000;
                        auto delta = int64_t(digest_resolver->last_modified()) - int64_t(exec->_cmd->read_timestamp);
                        if (std::abs(delta) <= write_timeout) {
                            exec->_proxy->_stats.global_read_repairs_canceled_due_to_concurrent_write++;
                            // if CL is local and non matching data is modified less then write_timeout ms ago do only local repair
                            auto i = boost::range::remove_if(exec->_targets, std::not1(std::cref(db::is_local)));
                            exec->_targets.erase(i, exec->_targets.end());
                        }
                    }
                    exec->reconcile(exec->_cl, timeout);
                    exec->_proxy->_stats.read_repair_repaired_blocking++;
                }
            } catch (...) {
                exec->_result_promise.set_exception(std::current_exception());
                exec->on_read_resolved();
            }

            digest_resolver->done().then([exec, digest_resolver, timeout, background_repair_check] () mutable {
                if (background_repair_check && !digest_resolver->digests_match()) {
                    exec->_proxy->_stats.read_repair_repaired_background++;
                    exec->_result_promise = promise<foreign_ptr<lw_shared_ptr<query::result>>>();
                    exec->reconcile(exec->_cl, timeout);
                    return exec->_result_promise.get_future().discard_result();
                } else {
                    return make_ready_future<>();
                }
            }).handle_exception([] (std::exception_ptr eptr) {
                // ignore any failures during background repair
            });
        });

        return _result_promise.get_future();
    }

    lw_shared_ptr<column_family>& get_cf() {
        return _cf;
    }
};

class never_speculating_read_executor : public abstract_read_executor {
public:
    never_speculating_read_executor(schema_ptr s, lw_shared_ptr<column_family> cf, shared_ptr<storage_proxy> proxy, lw_shared_ptr<query::read_command> cmd, dht::partition_range pr, db::consistency_level cl, std::vector<gms::inet_address> targets, tracing::trace_state_ptr trace_state) :
                                        abstract_read_executor(std::move(s), std::move(cf), std::move(proxy), std::move(cmd), std::move(pr), cl, 0, std::move(targets), std::move(trace_state)) {
        _block_for = _targets.size();
    }
};

// this executor always asks for one additional data reply
class always_speculating_read_executor : public abstract_read_executor {
public:
    using abstract_read_executor::abstract_read_executor;
    virtual future<> make_requests(digest_resolver_ptr resolver, storage_proxy::clock_type::time_point timeout) {
        resolver->add_wait_targets(_targets.size());
        // FIXME: consider disabling for CL=*ONE
        bool want_digest = true;
        return when_all(make_data_requests(resolver, _targets.begin(), _targets.begin() + 2, timeout, want_digest),
                        make_digest_requests(resolver, _targets.begin() + 2, _targets.end(), timeout)).discard_result();
    }
};

// this executor sends request to an additional replica after some time below timeout
class speculating_read_executor : public abstract_read_executor {
    timer<storage_proxy::clock_type> _speculate_timer;
public:
    using abstract_read_executor::abstract_read_executor;
    virtual future<> make_requests(digest_resolver_ptr resolver, storage_proxy::clock_type::time_point timeout) {
        _speculate_timer.set_callback([this, resolver, timeout] {
            if (!resolver->is_completed()) { // at the time the callback runs request may be completed already
                resolver->add_wait_targets(1); // we send one more request so wait for it too
                // FIXME: consider disabling for CL=*ONE
                auto send_request = [&] (bool has_data) {
                    if (has_data) {
                        _proxy->_stats.speculative_digest_reads++;
                        return make_digest_requests(resolver, _targets.end() - 1, _targets.end(), timeout);
                    } else {
                        _proxy->_stats.speculative_data_reads++;
                        return make_data_requests(resolver, _targets.end() - 1, _targets.end(), timeout, true);
                    }
                };
                send_request(resolver->has_data()).finally([exec = shared_from_this()]{});
            }
        });
        auto& sr = _schema->speculative_retry();
        auto t = (sr.get_type() == speculative_retry::type::PERCENTILE) ?
            std::min(_cf->get_coordinator_read_latency_percentile(sr.get_value()), std::chrono::milliseconds(_proxy->get_db().local().get_config().read_request_timeout_in_ms()/2)) :
            std::chrono::milliseconds(unsigned(sr.get_value()));
        _speculate_timer.arm(t);

        // if CL + RR result in covering all replicas, getReadExecutor forces AlwaysSpeculating.  So we know
        // that the last replica in our list is "extra."
        resolver->add_wait_targets(_targets.size() - 1);
        // FIXME: consider disabling for CL=*ONE
        bool want_digest = true;
        if (_block_for < _targets.size() - 1) {
            // We're hitting additional targets for read repair.  Since our "extra" replica is the least-
            // preferred by the snitch, we do an extra data read to start with against a replica more
            // likely to reply; better to let RR fail than the entire query.
            return when_all(make_data_requests(resolver, _targets.begin(), _targets.begin() + 2, timeout, want_digest),
                            make_digest_requests(resolver, _targets.begin() + 2, _targets.end(), timeout)).discard_result();
        } else {
            // not doing read repair; all replies are important, so it doesn't matter which nodes we
            // perform data reads against vs digest.
            return when_all(make_data_requests(resolver, _targets.begin(), _targets.begin() + 1, timeout, want_digest),
                            make_digest_requests(resolver, _targets.begin() + 1, _targets.end() - 1, timeout)).discard_result();
        }
    }
    virtual void got_cl() override {
        _speculate_timer.cancel();
    }
};

class range_slice_read_executor : public never_speculating_read_executor {
public:
    using never_speculating_read_executor::never_speculating_read_executor;
    virtual future<foreign_ptr<lw_shared_ptr<query::result>>> execute(storage_proxy::clock_type::time_point timeout) override {
        if (!service::get_local_storage_service().cluster_supports_digest_multipartition_reads()) {
            reconcile(_cl, timeout);
            return _result_promise.get_future();
        }
        return never_speculating_read_executor::execute(timeout);
    }
};

db::read_repair_decision storage_proxy::new_read_repair_decision(const schema& s) {
    double chance = _read_repair_chance(_urandom);
    if (s.read_repair_chance() > chance) {
        return db::read_repair_decision::GLOBAL;
    }

    if (s.dc_local_read_repair_chance() > chance) {
        return db::read_repair_decision::DC_LOCAL;
    }

    return db::read_repair_decision::NONE;
}

::shared_ptr<abstract_read_executor> storage_proxy::get_read_executor(lw_shared_ptr<query::read_command> cmd,
        schema_ptr schema,
        dht::partition_range pr,
        db::consistency_level cl,
        db::read_repair_decision repair_decision,
        tracing::trace_state_ptr trace_state,
        const std::vector<gms::inet_address>& preferred_endpoints) {
    const dht::token& token = pr.start()->value().token();
    keyspace& ks = _db.local().find_keyspace(schema->ks_name());
    speculative_retry::type retry_type = schema->speculative_retry().get_type();
    gms::inet_address extra_replica;

    std::vector<gms::inet_address> all_replicas = get_live_sorted_endpoints(ks, token);
    auto cf = _db.local().find_column_family(schema).shared_from_this();
    std::vector<gms::inet_address> target_replicas = db::filter_for_query(cl, ks, all_replicas, preferred_endpoints, repair_decision,
            retry_type == speculative_retry::type::NONE ? nullptr : &extra_replica,
            _db.local().get_config().cache_hit_rate_read_balancing() ? &*cf : nullptr);

    slogger.trace("creating read executor for token {} with all: {} targets: {} rp decision: {}", token, all_replicas, target_replicas, repair_decision);
    tracing::trace(trace_state, "Creating read executor for token {} with all: {} targets: {} repair decision: {}", token, all_replicas, target_replicas, repair_decision);

    // Throw UAE early if we don't have enough replicas.
    try {
        db::assure_sufficient_live_nodes(cl, ks, target_replicas);
    } catch (exceptions::unavailable_exception& ex) {
        slogger.debug("Read unavailable: cl={} required {} alive {}", ex.consistency, ex.required, ex.alive);
        _stats.read_unavailables.mark();
        throw;
    }

    if (repair_decision != db::read_repair_decision::NONE) {
        _stats.read_repair_attempts++;
    }

    size_t block_for = db::block_for(ks, cl);
    auto p = shared_from_this();
    // Speculative retry is disabled *OR* there are simply no extra replicas to speculate.
    if (retry_type == speculative_retry::type::NONE || block_for == all_replicas.size()
            || (repair_decision == db::read_repair_decision::DC_LOCAL && is_datacenter_local(cl) && block_for == target_replicas.size())) {
        return ::make_shared<never_speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, std::move(target_replicas), std::move(trace_state));
    }

    if (target_replicas.size() == all_replicas.size()) {
        // CL.ALL, RRD.GLOBAL or RRD.DC_LOCAL and a single-DC.
        // We are going to contact every node anyway, so ask for 2 full data requests instead of 1, for redundancy
        // (same amount of requests in total, but we turn 1 digest request into a full blown data request).
        return ::make_shared<always_speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, block_for, std::move(target_replicas), std::move(trace_state));
    }

    // RRD.NONE or RRD.DC_LOCAL w/ multiple DCs.
    if (target_replicas.size() == block_for) { // If RRD.DC_LOCAL extra replica may already be present
        if (is_datacenter_local(cl) && !db::is_local(extra_replica)) {
            slogger.trace("read executor no extra target to speculate");
            return ::make_shared<never_speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, std::move(target_replicas), std::move(trace_state));
        } else {
            target_replicas.push_back(extra_replica);
            slogger.trace("creating read executor with extra target {}", extra_replica);
        }
    }

    if (retry_type == speculative_retry::type::ALWAYS) {
        return ::make_shared<always_speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, block_for, std::move(target_replicas), std::move(trace_state));
    } else {// PERCENTILE or CUSTOM.
        return ::make_shared<speculating_read_executor>(schema, cf, p, cmd, std::move(pr), cl, block_for, std::move(target_replicas), std::move(trace_state));
    }
}

future<query::result_digest, api::timestamp_type, cache_temperature>
storage_proxy::query_result_local_digest(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr, tracing::trace_state_ptr trace_state, storage_proxy::clock_type::time_point timeout, query::digest_algorithm da, uint64_t max_size) {
    return query_result_local(std::move(s), std::move(cmd), pr, query::result_options::only_digest(da), std::move(trace_state), timeout, max_size).then([] (foreign_ptr<lw_shared_ptr<query::result>> result, cache_temperature hit_rate) {
        return make_ready_future<query::result_digest, api::timestamp_type, cache_temperature>(*result->digest(), result->last_modified(), hit_rate);
    });
}

future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>
storage_proxy::query_result_local(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr, query::result_options opts,
                                  tracing::trace_state_ptr trace_state, storage_proxy::clock_type::time_point timeout, uint64_t max_size) {
    cmd->slice.options.set_if<query::partition_slice::option::with_digest>(opts.request != query::result_request::only_result);
    if (pr.is_singular()) {
        unsigned shard = _db.local().shard_of(pr.start()->value().token());
        _stats.replica_cross_shard_ops += shard != engine().cpu_id();
        return _db.invoke_on(shard, [max_size, gs = global_schema_ptr(s), prv = dht::partition_range_vector({pr}) /* FIXME: pr is copied */, cmd, opts, timeout, gt = tracing::global_trace_state_ptr(std::move(trace_state))] (database& db) mutable {
            auto trace_state = gt.get();
            tracing::trace(trace_state, "Start querying the token range that starts with {}", seastar::value_of([&prv] { return prv.begin()->start()->value().token(); }));
            return db.query(gs, *cmd, opts, prv, trace_state, max_size, timeout).then([trace_state](auto&& f, cache_temperature ht) {
                tracing::trace(trace_state, "Querying is done");
                return make_ready_future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>(make_foreign(std::move(f)), ht);
            });
        });
    } else {
        return query_nonsingular_mutations_locally(s, cmd, {pr}, std::move(trace_state), max_size, timeout).then([s, cmd, opts] (foreign_ptr<lw_shared_ptr<reconcilable_result>>&& r, cache_temperature&& ht) {
            return make_ready_future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>(
                    ::make_foreign(::make_lw_shared(to_data_query_result(*r, s, cmd->slice,  cmd->row_limit, cmd->partition_limit, opts))), ht);
        });
    }
}

void storage_proxy::handle_read_error(std::exception_ptr eptr, bool range) {
    try {
        std::rethrow_exception(eptr);
    } catch (read_timeout_exception& ex) {
        slogger.debug("Read timeout: received {} of {} required replies, data {}present", ex.received, ex.block_for, ex.data_present ? "" : "not ");
        if (range) {
            _stats.range_slice_timeouts.mark();
        } else {
            _stats.read_timeouts.mark();
        }
    } catch (...) {
        slogger.debug("Error during read query {}", eptr);
    }
}

future<storage_proxy::coordinator_query_result>
storage_proxy::query_singular(lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl,
        storage_proxy::coordinator_query_options query_options) {
    std::vector<std::pair<::shared_ptr<abstract_read_executor>, dht::token_range>> exec;
    exec.reserve(partition_ranges.size());

    schema_ptr schema = local_schema_registry().get(cmd->schema_version);

    db::read_repair_decision repair_decision = query_options.read_repair_decision
        ? *query_options.read_repair_decision : new_read_repair_decision(*schema);

    for (auto&& pr: partition_ranges) {
        if (!pr.is_singular()) {
            throw std::runtime_error("mixed singular and non singular range are not supported");
        }

        auto token_range = dht::token_range::make_singular(pr.start()->value().token());
        auto it = query_options.preferred_replicas.find(token_range);
        const auto replicas = it == query_options.preferred_replicas.end()
            ? std::vector<gms::inet_address>{} : replica_ids_to_endpoints(it->second);

        exec.emplace_back(get_read_executor(cmd, schema, std::move(pr), cl, repair_decision, query_options.trace_state, replicas),
                std::move(token_range));
    }

    query::result_merger merger(cmd->row_limit, cmd->partition_limit);
    merger.reserve(exec.size());

    auto used_replicas = make_lw_shared<replicas_per_token_range>();

    auto f = ::map_reduce(exec.begin(), exec.end(), [timeout = query_options.timeout(*this), used_replicas] (
                std::pair<::shared_ptr<abstract_read_executor>, dht::token_range>& executor_and_token_range) {
        auto& [rex, token_range] = executor_and_token_range;
        utils::latency_counter lc;
        lc.start();
        return rex->execute(timeout).then_wrapped([lc, rex, used_replicas, token_range = std::move(token_range)] (
                    future<foreign_ptr<lw_shared_ptr<query::result>>> f) mutable {
            if (!f.failed()) {
                used_replicas->emplace(std::move(token_range), endpoints_to_replica_ids(rex->used_targets()));
            }
            if (lc.is_start()) {
                rex->get_cf()->add_coordinator_read_latency(lc.stop().latency());
            }
            return std::move(f);
        });
    }, std::move(merger));

    return f.then_wrapped([exec = std::move(exec),
            p = shared_from_this(),
            used_replicas,
            repair_decision] (future<foreign_ptr<lw_shared_ptr<query::result>>> f) {
        if (f.failed()) {
            auto eptr = f.get_exception();
            // hold onto exec until read is complete
            p->handle_read_error(eptr, false);
            return make_exception_future<storage_proxy::coordinator_query_result>(eptr);
        }
        return make_ready_future<coordinator_query_result>(coordinator_query_result(std::move(f.get0()), std::move(*used_replicas), repair_decision));
    });
}

future<std::vector<foreign_ptr<lw_shared_ptr<query::result>>>, replicas_per_token_range>
storage_proxy::query_partition_key_range_concurrent(storage_proxy::clock_type::time_point timeout,
        std::vector<foreign_ptr<lw_shared_ptr<query::result>>>&& results,
        lw_shared_ptr<query::read_command> cmd,
        db::consistency_level cl,
        query_ranges_to_vnodes_generator&& ranges_to_vnodes,
        int concurrency_factor,
        tracing::trace_state_ptr trace_state,
        uint32_t remaining_row_count,
        uint32_t remaining_partition_count,
        replicas_per_token_range preferred_replicas) {
    schema_ptr schema = local_schema_registry().get(cmd->schema_version);
    keyspace& ks = _db.local().find_keyspace(schema->ks_name());
    std::vector<::shared_ptr<abstract_read_executor>> exec;
    auto p = shared_from_this();
    auto& cf= _db.local().find_column_family(schema);
    auto pcf = _db.local().get_config().cache_hit_rate_read_balancing() ? &cf : nullptr;
    std::unordered_map<abstract_read_executor*, std::vector<dht::token_range>> ranges_per_exec;

    const auto preferred_replicas_for_range = [&preferred_replicas] (const dht::partition_range& r) {
        auto it = preferred_replicas.find(r.transform(std::mem_fn(&dht::ring_position::token)));
        return it == preferred_replicas.end() ? std::vector<gms::inet_address>{} : replica_ids_to_endpoints(it->second);
    };
    const auto to_token_range = [] (const dht::partition_range& r) { return r.transform(std::mem_fn(&dht::ring_position::token)); };

    dht::partition_range_vector ranges = ranges_to_vnodes(concurrency_factor);
    dht::partition_range_vector::iterator i = ranges.begin();

    while (i != ranges.end()) {
        dht::partition_range& range = *i;
        std::vector<gms::inet_address> live_endpoints = get_live_sorted_endpoints(ks, end_token(range));
        std::vector<gms::inet_address> merged_preferred_replicas = preferred_replicas_for_range(*i);
        std::vector<gms::inet_address> filtered_endpoints = filter_for_query(cl, ks, live_endpoints, merged_preferred_replicas, pcf);
        std::vector<dht::token_range> merged_ranges{to_token_range(range)};
        ++i;

        // getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
        // the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
        // still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
        while (i != ranges.end())
        {
            const auto current_range_preferred_replicas = preferred_replicas_for_range(*i);
            dht::partition_range& next_range = *i;
            std::vector<gms::inet_address> next_endpoints = get_live_sorted_endpoints(ks, end_token(next_range));
            std::vector<gms::inet_address> next_filtered_endpoints = filter_for_query(cl, ks, next_endpoints, current_range_preferred_replicas, pcf);

            // Origin has this to say here:
            // *  If the current range right is the min token, we should stop merging because CFS.getRangeSlice
            // *  don't know how to deal with a wrapping range.
            // *  Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
            // *  the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
            // *  wire compatibility, so It's likely easier not to bother;
            // It obviously not apply for us(?), but lets follow origin for now
            if (end_token(range) == dht::maximum_token()) {
                break;
            }

            std::vector<gms::inet_address> merged = intersection(live_endpoints, next_endpoints);
            std::vector<gms::inet_address> current_merged_preferred_replicas = intersection(merged_preferred_replicas, current_range_preferred_replicas);

            // Check if there is enough endpoint for the merge to be possible.
            if (!is_sufficient_live_nodes(cl, ks, merged)) {
                break;
            }

            std::vector<gms::inet_address> filtered_merged = filter_for_query(cl, ks, merged, current_merged_preferred_replicas, pcf);

            // Estimate whether merging will be a win or not
            if (!locator::i_endpoint_snitch::get_local_snitch_ptr()->is_worth_merging_for_range_query(filtered_merged, filtered_endpoints, next_filtered_endpoints)) {
                break;
            } else if (pcf) {
                // check that merged set hit rate is not to low
                auto find_min = [pcf] (const std::vector<gms::inet_address>& range) {
                    struct {
                        column_family* cf = nullptr;
                        float operator()(const gms::inet_address& ep) const {
                            return float(cf->get_hit_rate(ep).rate);
                        }
                    } ep_to_hr{pcf};
                    return *boost::range::min_element(range | boost::adaptors::transformed(ep_to_hr));
                };
                auto merged = find_min(filtered_merged) * 1.2; // give merged set 20% boost
                if (merged < find_min(filtered_endpoints) && merged < find_min(next_filtered_endpoints)) {
                    // if lowest cache hits rate of a merged set is smaller than lowest cache hit
                    // rate of un-merged sets then do not merge. The idea is that we better issue
                    // two different range reads with highest chance of hitting a cache then one read that
                    // will cause more IO on contacted nodes
                    break;
                }
            }

            // If we get there, merge this range and the next one
            range = dht::partition_range(range.start(), next_range.end());
            live_endpoints = std::move(merged);
            merged_preferred_replicas = std::move(current_merged_preferred_replicas);
            filtered_endpoints = std::move(filtered_merged);
            ++i;
            merged_ranges.push_back(to_token_range(next_range));
        }
        slogger.trace("creating range read executor with targets {}", filtered_endpoints);
        try {
            db::assure_sufficient_live_nodes(cl, ks, filtered_endpoints);
        } catch(exceptions::unavailable_exception& ex) {
            slogger.debug("Read unavailable: cl={} required {} alive {}", ex.consistency, ex.required, ex.alive);
            _stats.range_slice_unavailables.mark();
            throw;
        }

        exec.push_back(::make_shared<range_slice_read_executor>(schema, cf.shared_from_this(), p, cmd, std::move(range), cl, std::move(filtered_endpoints), trace_state));
        ranges_per_exec.emplace(exec.back().get(), std::move(merged_ranges));
    }

    query::result_merger merger(cmd->row_limit, cmd->partition_limit);
    merger.reserve(exec.size());

    auto f = ::map_reduce(exec.begin(), exec.end(), [timeout] (::shared_ptr<abstract_read_executor>& rex) {
        return rex->execute(timeout);
    }, std::move(merger));

    return f.then([p,
            exec = std::move(exec),
            results = std::move(results),
            ranges_to_vnodes = std::move(ranges_to_vnodes),
            cl,
            cmd,
            concurrency_factor,
            timeout,
            remaining_row_count,
            remaining_partition_count,
            trace_state = std::move(trace_state),
            preferred_replicas = std::move(preferred_replicas),
            ranges_per_exec = std::move(ranges_per_exec)] (foreign_ptr<lw_shared_ptr<query::result>>&& result) mutable {
        result->ensure_counts();
        remaining_row_count -= result->row_count().value();
        remaining_partition_count -= result->partition_count().value();
        results.emplace_back(std::move(result));
        if (ranges_to_vnodes.empty() || !remaining_row_count || !remaining_partition_count) {
            auto used_replicas = replicas_per_token_range();
            for (auto& e : exec) {
                // We add used replicas in separate per-vnode entries even if
                // they were merged, for two reasons:
                // 1) The list of replicas is determined for each vnode
                // separately and thus this makes lookups more convenient.
                // 2) On the next page the ranges might not be merged.
                auto replica_ids = endpoints_to_replica_ids(e->used_targets());
                for (auto& r : ranges_per_exec[e.get()]) {
                    used_replicas.emplace(std::move(r), replica_ids);
                }
            }
            return make_ready_future<std::vector<foreign_ptr<lw_shared_ptr<query::result>>>, replicas_per_token_range>(std::move(results), std::move(used_replicas));
        } else {
            cmd->row_limit = remaining_row_count;
            cmd->partition_limit = remaining_partition_count;
            return p->query_partition_key_range_concurrent(timeout, std::move(results), cmd, cl, std::move(ranges_to_vnodes),
                    concurrency_factor * 2, std::move(trace_state), remaining_row_count, remaining_partition_count, std::move(preferred_replicas));
        }
    }).handle_exception([p] (std::exception_ptr eptr) {
        p->handle_read_error(eptr, true);
        return make_exception_future<std::vector<foreign_ptr<lw_shared_ptr<query::result>>>, replicas_per_token_range>(eptr);
    });
}

future<storage_proxy::coordinator_query_result>
storage_proxy::query_partition_key_range(lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector partition_ranges,
        db::consistency_level cl,
        storage_proxy::coordinator_query_options query_options) {
    schema_ptr schema = local_schema_registry().get(cmd->schema_version);
    keyspace& ks = _db.local().find_keyspace(schema->ks_name());

    // when dealing with LocalStrategy keyspaces, we can skip the range splitting and merging (which can be
    // expensive in clusters with vnodes)
    query_ranges_to_vnodes_generator ranges_to_vnodes(schema, std::move(partition_ranges), ks.get_replication_strategy().get_type() == locator::replication_strategy_type::local);

    int result_rows_per_range = 0;
    int concurrency_factor = 1;

    std::vector<foreign_ptr<lw_shared_ptr<query::result>>> results;

    slogger.debug("Estimated result rows per range: {}; requested rows: {}, concurrent range requests: {}",
            result_rows_per_range, cmd->row_limit, concurrency_factor);

    // The call to `query_partition_key_range_concurrent()` below
    // updates `cmd` directly when processing the results. Under
    // some circumstances, when the query executes without deferring,
    // this updating will happen before the lambda object is constructed
    // and hence the updates will be visible to the lambda. This will
    // result in the merger below trimming the results according to the
    // updated (decremented) limits and causing the paging logic to
    // declare the query exhausted due to the non-full page. To avoid
    // this save the original values of the limits here and pass these
    // to the lambda below.
    const auto row_limit = cmd->row_limit;
    const auto partition_limit = cmd->partition_limit;

    return query_partition_key_range_concurrent(query_options.timeout(*this),
            std::move(results),
            cmd,
            cl,
            std::move(ranges_to_vnodes),
            concurrency_factor,
            std::move(query_options.trace_state),
            cmd->row_limit,
            cmd->partition_limit,
            std::move(query_options.preferred_replicas)).then([row_limit, partition_limit] (
                    std::vector<foreign_ptr<lw_shared_ptr<query::result>>> results,
                    replicas_per_token_range used_replicas) {
        query::result_merger merger(row_limit, partition_limit);
        merger.reserve(results.size());

        for (auto&& r: results) {
            merger(std::move(r));
        }

        return make_ready_future<coordinator_query_result>(coordinator_query_result(merger.get(), std::move(used_replicas)));
    });
}

future<storage_proxy::coordinator_query_result>
storage_proxy::query(schema_ptr s,
    lw_shared_ptr<query::read_command> cmd,
    dht::partition_range_vector&& partition_ranges,
    db::consistency_level cl,
    storage_proxy::coordinator_query_options query_options)
{
    if (slogger.is_enabled(logging::log_level::trace) || qlogger.is_enabled(logging::log_level::trace)) {
        static thread_local int next_id = 0;
        auto query_id = next_id++;

        slogger.trace("query {}.{} cmd={}, ranges={}, id={}", s->ks_name(), s->cf_name(), *cmd, partition_ranges, query_id);
        return do_query(s, cmd, std::move(partition_ranges), cl, std::move(query_options)).then([query_id, cmd, s] (coordinator_query_result qr) {
            auto& res = qr.query_result;
            if (res->buf().is_linearized()) {
                res->ensure_counts();
                slogger.trace("query_result id={}, size={}, rows={}, partitions={}", query_id, res->buf().size(), *res->row_count(), *res->partition_count());
            } else {
                slogger.trace("query_result id={}, size={}", query_id, res->buf().size());
            }
            qlogger.trace("id={}, {}", query_id, res->pretty_printer(s, cmd->slice));
            return make_ready_future<coordinator_query_result>(std::move(qr));
        });
    }

    return do_query(s, cmd, std::move(partition_ranges), cl, std::move(query_options));
}

future<storage_proxy::coordinator_query_result>
storage_proxy::do_query(schema_ptr s,
    lw_shared_ptr<query::read_command> cmd,
    dht::partition_range_vector&& partition_ranges,
    db::consistency_level cl,
    storage_proxy::coordinator_query_options query_options)
{
    static auto make_empty = [] {
        return make_ready_future<coordinator_query_result>(make_foreign(make_lw_shared<query::result>()));
    };

    auto& slice = cmd->slice;
    if (partition_ranges.empty() ||
            (slice.default_row_ranges().empty() && !slice.get_specific_ranges())) {
        return make_empty();
    }
    utils::latency_counter lc;
    lc.start();
    auto p = shared_from_this();

    if (query::is_single_partition(partition_ranges[0])) { // do not support mixed partitions (yet?)
        try {
            return query_singular(cmd,
                    std::move(partition_ranges),
                    cl,
                    std::move(query_options)).finally([lc, p] () mutable {
                p->_stats.read.mark(lc.stop().latency());
                if (lc.is_start()) {
                    p->_stats.estimated_read.add(lc.latency(), p->_stats.read.hist.count);
                }
            });
        } catch (const no_such_column_family&) {
            _stats.read.mark(lc.stop().latency());
            return make_empty();
        }
    }

    return query_partition_key_range(cmd,
            std::move(partition_ranges),
            cl,
            std::move(query_options)).finally([lc, p] () mutable {
        p->_stats.range.mark(lc.stop().latency());
        if (lc.is_start()) {
            p->_stats.estimated_range.add(lc.latency(), p->_stats.range.hist.count);
        }
    });
}

std::vector<gms::inet_address> storage_proxy::get_live_endpoints(keyspace& ks, const dht::token& token) {
    auto& rs = ks.get_replication_strategy();
    std::vector<gms::inet_address> eps = rs.get_natural_endpoints(token);
    auto itend = boost::range::remove_if(eps, std::not1(std::bind1st(std::mem_fn(&gms::gossiper::is_alive), &gms::get_local_gossiper())));
    eps.erase(itend, eps.end());
    return eps;
}

std::vector<gms::inet_address> storage_proxy::get_live_sorted_endpoints(keyspace& ks, const dht::token& token) {
    auto eps = get_live_endpoints(ks, token);
    locator::i_endpoint_snitch::get_local_snitch_ptr()->sort_by_proximity(utils::fb_utilities::get_broadcast_address(), eps);
    // FIXME: before dynamic snitch is implement put local address (if present) at the beginning
    auto it = boost::range::find(eps, utils::fb_utilities::get_broadcast_address());
    if (it != eps.end() && it != eps.begin()) {
        std::iter_swap(it, eps.begin());
    }
    return eps;
}

std::vector<gms::inet_address> storage_proxy::intersection(const std::vector<gms::inet_address>& l1, const std::vector<gms::inet_address>& l2) {
    std::vector<gms::inet_address> inter;
    inter.reserve(l1.size());
    std::remove_copy_if(l1.begin(), l1.end(), std::back_inserter(inter), [&l2] (const gms::inet_address& a) {
        return std::find(l2.begin(), l2.end(), a) == l2.end();
    });
    return inter;
}

query_ranges_to_vnodes_generator::query_ranges_to_vnodes_generator(schema_ptr s, dht::partition_range_vector ranges, bool local) :
        query_ranges_to_vnodes_generator(get_local_storage_service().get_token_metadata(), std::move(s), std::move(ranges), local) {}
query_ranges_to_vnodes_generator::query_ranges_to_vnodes_generator(locator::token_metadata& tm, schema_ptr s, dht::partition_range_vector ranges, bool local) :
        _s(s), _ranges(std::move(ranges)), _i(_ranges.begin()), _local(local), _tm(tm) {}

dht::partition_range_vector query_ranges_to_vnodes_generator::operator()(size_t n) {
    n = std::min(n, size_t(1024));

    dht::partition_range_vector result;
    result.reserve(n);
    while (_i != _ranges.end() && result.size() != n) {
        process_one_range(n, result);
    }
    return result;
}

bool query_ranges_to_vnodes_generator::empty() const {
    return _ranges.end() == _i;
}

/**
 * Compute all ranges we're going to query, in sorted order. Nodes can be replica destinations for many ranges,
 * so we need to restrict each scan to the specific range we want, or else we'd get duplicate results.
 */
void query_ranges_to_vnodes_generator::process_one_range(size_t n, dht::partition_range_vector& ranges) {
    dht::ring_position_comparator cmp(*_s);
    dht::partition_range& cr = *_i;

    auto get_remainder = [this, &cr] {
        _i++;
       return std::move(cr);
    };

    auto add_range = [&ranges] (dht::partition_range&& r) {
        ranges.emplace_back(std::move(r));
    };

    if (_local) { // if the range is local no need to divide to vnodes
        add_range(get_remainder());
        return;
    }

    // special case for bounds containing exactly 1 token
    if (start_token(cr) == end_token(cr)) {
        if (start_token(cr).is_minimum()) {
            _i++; // empty range? Move to the next one
            return;
        }
        add_range(get_remainder());
        return;
    }

    // divide the queryRange into pieces delimited by the ring
    auto ring_iter = _tm.ring_range(cr.start(), false);
    for (const dht::token& upper_bound_token : ring_iter) {
        /*
         * remainder can be a range/bounds of token _or_ keys and we want to split it with a token:
         *   - if remainder is tokens, then we'll just split using the provided token.
         *   - if remainder is keys, we want to split using token.upperBoundKey. For instance, if remainder
         *     is [DK(10, 'foo'), DK(20, 'bar')], and we have 3 nodes with tokens 0, 15, 30. We want to
         *     split remainder to A=[DK(10, 'foo'), 15] and B=(15, DK(20, 'bar')]. But since we can't mix
         *     tokens and keys at the same time in a range, we uses 15.upperBoundKey() to have A include all
         *     keys having 15 as token and B include none of those (since that is what our node owns).
         * asSplitValue() abstracts that choice.
         */

        dht::ring_position split_point(upper_bound_token, dht::ring_position::token_bound::end);
        if (!cr.contains(split_point, cmp)) {
            break; // no more splits
        }


        // We shouldn't attempt to split on upper bound, because it may result in
        // an ambiguous range of the form (x; x]
        if (end_token(cr) == upper_bound_token) {
            break;
        }

        std::pair<dht::partition_range, dht::partition_range> splits =
                cr.split(split_point, cmp);

        add_range(std::move(splits.first));
        cr = std::move(splits.second);
        if (ranges.size() == n) {
            // we have enough ranges
            break;
        }
    }

    if (ranges.size() < n) {
        add_range(get_remainder());
    }
}

bool storage_proxy::hints_enabled(db::write_type type) noexcept {
    return bool(_hints_manager) || type == db::write_type::VIEW;
}

db::hints::manager& storage_proxy::hints_manager_for(db::write_type type) {
    return type == db::write_type::VIEW ? _hints_for_views_manager : *_hints_manager;
}

future<> storage_proxy::truncate_blocking(sstring keyspace, sstring cfname) {
    slogger.debug("Starting a blocking truncate operation on keyspace {}, CF {}", keyspace, cfname);

    auto& gossiper = gms::get_local_gossiper();

    if (!gossiper.get_unreachable_token_owners().empty()) {
        slogger.info("Cannot perform truncate, some hosts are down");
        // Since the truncate operation is so aggressive and is typically only
        // invoked by an admin, for simplicity we require that all nodes are up
        // to perform the operation.
        auto live_members = gossiper.get_live_members().size();

        throw exceptions::unavailable_exception(db::consistency_level::ALL,
                live_members + gossiper.get_unreachable_members().size(),
                live_members);
    }

    auto all_endpoints = gossiper.get_live_token_owners();
    auto& ms = netw::get_local_messaging_service();
    auto timeout = std::chrono::milliseconds(_db.local().get_config().truncate_request_timeout_in_ms());

    slogger.trace("Enqueuing truncate messages to hosts {}", all_endpoints);

    return parallel_for_each(all_endpoints, [keyspace, cfname, &ms, timeout](auto ep) {
        return ms.send_truncate(netw::messaging_service::msg_addr{ep, 0}, timeout, keyspace, cfname);
    }).handle_exception([cfname](auto ep) {
       try {
           std::rethrow_exception(ep);
       } catch (rpc::timeout_error& e) {
           slogger.trace("Truncation of {} timed out: {}", cfname, e.what());
           throw;
       } catch (...) {
           throw;
       }
    });
}

void storage_proxy::init_messaging_service() {
    auto& ms = netw::get_local_messaging_service();
    ms.register_counter_mutation([] (const rpc::client_info& cinfo, rpc::opt_time_point t, std::vector<frozen_mutation> fms, db::consistency_level cl, std::optional<tracing::trace_info> trace_info) {
        auto src_addr = netw::messaging_service::get_source(cinfo);

        tracing::trace_state_ptr trace_state_ptr;
        if (trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "Message received from /{}", src_addr.addr);
        }

        return do_with(std::vector<frozen_mutation_and_schema>(),
                       [cl, src_addr, timeout = *t, fms = std::move(fms), trace_state_ptr = std::move(trace_state_ptr)] (std::vector<frozen_mutation_and_schema>& mutations) mutable {
            return parallel_for_each(std::move(fms), [&mutations, src_addr] (frozen_mutation& fm) {
                // FIXME: optimise for cases when all fms are in the same schema
                auto schema_version = fm.schema_version();
                return get_schema_for_write(schema_version, std::move(src_addr)).then([&mutations, fm = std::move(fm)] (schema_ptr s) mutable {
                    mutations.emplace_back(frozen_mutation_and_schema { std::move(fm), std::move(s) });
                });
            }).then([trace_state_ptr = std::move(trace_state_ptr), &mutations, cl, timeout] {
                auto sp = get_local_shared_storage_proxy();
                return sp->mutate_counters_on_leader(std::move(mutations), cl, timeout, std::move(trace_state_ptr));
            });
        });
    });
    ms.register_mutation([] (const rpc::client_info& cinfo, rpc::opt_time_point t, frozen_mutation in, std::vector<gms::inet_address> forward, gms::inet_address reply_to, unsigned shard, storage_proxy::response_id_type response_id, rpc::optional<std::optional<tracing::trace_info>> trace_info) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);

        if (trace_info && *trace_info) {
            tracing::trace_info& tr_info = **trace_info;
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(tr_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "Message received from /{}", src_addr.addr);
        }

        storage_proxy::clock_type::time_point timeout;
        if (!t) {
            auto timeout_in_ms = get_local_shared_storage_proxy()->_db.local().get_config().write_request_timeout_in_ms();
            timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
        } else {
            timeout = *t;
        }

        return do_with(std::move(in), get_local_shared_storage_proxy(), size_t(0),
                [src_addr = std::move(src_addr), &cinfo, forward = std::move(forward), reply_to, shard, response_id, trace_state_ptr, timeout] (const frozen_mutation& m, shared_ptr<storage_proxy>& p, size_t& errors) mutable {
            ++p->_stats.received_mutations;
            p->_stats.forwarded_mutations += forward.size();
            return when_all(
                // mutate_locally() may throw, putting it into apply() converts exception to a future.
                futurize<void>::apply([timeout, &p, &m, reply_to, shard, src_addr = std::move(src_addr)] () mutable {
                    // FIXME: get_schema_for_write() doesn't timeout
                    return get_schema_for_write(m.schema_version(), netw::messaging_service::msg_addr{reply_to, shard}).then([&m, &p, timeout] (schema_ptr s) {
                        return p->mutate_locally(std::move(s), m, timeout);
                    });
                }).then([&p, reply_to, shard, response_id, trace_state_ptr] () {
                    auto& ms = netw::get_local_messaging_service();
                    // We wait for send_mutation_done to complete, otherwise, if reply_to is busy, we will accumulate
                    // lots of unsent responses, which can OOM our shard.
                    //
                    // Usually we will return immediately, since this work only involves appending data to the connection
                    // send buffer.
                    tracing::trace(trace_state_ptr, "Sending mutation_done to /{}", reply_to);
                    return ms.send_mutation_done(
                            netw::messaging_service::msg_addr{reply_to, shard},
                            shard,
                            response_id,
                            p->get_view_update_backlog()).then_wrapped([] (future<> f) {
                        f.ignore_ready_future();
                    });
                }).handle_exception([reply_to, shard, &p, &errors] (std::exception_ptr eptr) {
                    seastar::log_level l = seastar::log_level::warn;
                    try {
                        std::rethrow_exception(eptr);
                    } catch (timed_out_error&) {
                        // ignore timeouts so that logs are not flooded.
                        // database total_writes_timedout counter was incremented.
                        l = seastar::log_level::debug;
                    } catch (...) {
                        // ignore
                    }
                    slogger.log(l, "Failed to apply mutation from {}#{}: {}", reply_to, shard, eptr);
                    errors++;
                }),
                parallel_for_each(forward.begin(), forward.end(), [reply_to, shard, response_id, &m, &p, trace_state_ptr, timeout, &errors] (gms::inet_address forward) {
                    auto& ms = netw::get_local_messaging_service();
                    tracing::trace(trace_state_ptr, "Forwarding a mutation to /{}", forward);
                    return ms.send_mutation(netw::messaging_service::msg_addr{forward, 0}, timeout, m, {}, reply_to, shard, response_id, tracing::make_trace_info(trace_state_ptr)).then_wrapped([&p, &errors] (future<> f) {
                        if (f.failed()) {
                            ++p->_stats.forwarding_errors;
                            errors++;
                        };
                        f.ignore_ready_future();
                    });
                })
            ).then_wrapped([trace_state_ptr, reply_to, shard, response_id, &errors, &p] (future<std::tuple<future<>, future<>>>&& f) {
                // ignore results, since we'll be returning them via MUTATION_DONE/MUTATION_FAILURE verbs
                auto fut = make_ready_future<seastar::rpc::no_wait_type>(netw::messaging_service::no_wait());
                if (errors) {
                    if (get_local_storage_service().cluster_supports_write_failure_reply()) {
                        tracing::trace(trace_state_ptr, "Sending mutation_failure with {} failures to /{}", errors, reply_to);
                        auto& ms = netw::get_local_messaging_service();
                        fut = ms.send_mutation_failed(
                                netw::messaging_service::msg_addr{reply_to, shard},
                                shard,
                                response_id,
                                errors,
                                p->get_view_update_backlog()).then_wrapped([] (future<> f) {
                            f.ignore_ready_future();
                            return netw::messaging_service::no_wait();
                        });
                    }
                }
                return fut.finally([trace_state_ptr] {
                    tracing::trace(trace_state_ptr, "Mutation handling is done");
                });
            });
        });
    });
    ms.register_mutation_done([this] (const rpc::client_info& cinfo, unsigned shard, storage_proxy::response_id_type response_id, rpc::optional<db::view::update_backlog> backlog) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        _stats.replica_cross_shard_ops += shard != engine().cpu_id();
        return get_storage_proxy().invoke_on(shard, [from, response_id, backlog = std::move(backlog)] (storage_proxy& sp) mutable {
            sp.got_response(response_id, from, std::move(backlog));
            return netw::messaging_service::no_wait();
        });
    });
    ms.register_mutation_failed([this] (const rpc::client_info& cinfo, unsigned shard, storage_proxy::response_id_type response_id, size_t num_failed, rpc::optional<db::view::update_backlog> backlog) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        _stats.replica_cross_shard_ops += shard != engine().cpu_id();
        return get_storage_proxy().invoke_on(shard, [from, response_id, num_failed, backlog = std::move(backlog)] (storage_proxy& sp) mutable {
            sp.got_failure_response(response_id, from, num_failed, std::move(backlog));
            return netw::messaging_service::no_wait();
        });
    });
    ms.register_read_data([] (const rpc::client_info& cinfo, rpc::opt_time_point t, query::read_command cmd, ::compat::wrapping_partition_range pr, rpc::optional<query::digest_algorithm> oda) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);
        if (cmd.trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*cmd.trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "read_data: message received from /{}", src_addr.addr);
        }
        auto da = oda.value_or(query::digest_algorithm::MD5);
        auto max_size = cinfo.retrieve_auxiliary<uint64_t>("max_result_size");
        return do_with(std::move(pr), get_local_shared_storage_proxy(), std::move(trace_state_ptr), [&cinfo, cmd = make_lw_shared<query::read_command>(std::move(cmd)), src_addr = std::move(src_addr), da, max_size, t] (::compat::wrapping_partition_range& pr, shared_ptr<storage_proxy>& p, tracing::trace_state_ptr& trace_state_ptr) mutable {
            p->_stats.replica_data_reads++;
            auto src_ip = src_addr.addr;
            return get_schema_for_read(cmd->schema_version, std::move(src_addr)).then([cmd, da, &pr, &p, &trace_state_ptr, max_size, t] (schema_ptr s) {
                auto pr2 = ::compat::unwrap(std::move(pr), *s);
                if (pr2.second) {
                    // this function assumes singular queries but doesn't validate
                    throw std::runtime_error("READ_DATA called with wrapping range");
                }
                query::result_options opts;
                opts.digest_algo = da;
                opts.request = da == query::digest_algorithm::none ? query::result_request::only_result : query::result_request::result_and_digest;
                auto timeout = t ? *t : db::no_timeout;
                return p->query_result_local(std::move(s), cmd, std::move(pr2.first), opts, trace_state_ptr, timeout, max_size);
            }).finally([&trace_state_ptr, src_ip] () mutable {
                tracing::trace(trace_state_ptr, "read_data handling is done, sending a response to /{}", src_ip);
            });
        });
    });
    ms.register_read_mutation_data([] (const rpc::client_info& cinfo, rpc::opt_time_point t, query::read_command cmd, ::compat::wrapping_partition_range pr) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);
        if (cmd.trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*cmd.trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "read_mutation_data: message received from /{}", src_addr.addr);
        }
        auto max_size = cinfo.retrieve_auxiliary<uint64_t>("max_result_size");
        return do_with(std::move(pr),
                       get_local_shared_storage_proxy(),
                       std::move(trace_state_ptr),
                       ::compat::one_or_two_partition_ranges({}),
                       [&cinfo, cmd = make_lw_shared<query::read_command>(std::move(cmd)), src_addr = std::move(src_addr), max_size, t] (
                               ::compat::wrapping_partition_range& pr,
                               shared_ptr<storage_proxy>& p,
                               tracing::trace_state_ptr& trace_state_ptr,
                               ::compat::one_or_two_partition_ranges& unwrapped) mutable {
            p->_stats.replica_mutation_data_reads++;
            auto src_ip = src_addr.addr;
            return get_schema_for_read(cmd->schema_version, std::move(src_addr)).then([cmd, &pr, &p, &trace_state_ptr, max_size, &unwrapped, t] (schema_ptr s) mutable {
                unwrapped = ::compat::unwrap(std::move(pr), *s);
                auto timeout = t ? *t : db::no_timeout;
                return p->query_mutations_locally(std::move(s), std::move(cmd), unwrapped, timeout, trace_state_ptr, max_size);
            }).finally([&trace_state_ptr, src_ip] () mutable {
                tracing::trace(trace_state_ptr, "read_mutation_data handling is done, sending a response to /{}", src_ip);
            });
        });
    });
    ms.register_read_digest([] (const rpc::client_info& cinfo, rpc::opt_time_point t, query::read_command cmd, ::compat::wrapping_partition_range pr, rpc::optional<query::digest_algorithm> oda) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);
        if (cmd.trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*cmd.trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "read_digest: message received from /{}", src_addr.addr);
        }
        auto da = oda.value_or(query::digest_algorithm::MD5);
        auto max_size = cinfo.retrieve_auxiliary<uint64_t>("max_result_size");
        return do_with(std::move(pr), get_local_shared_storage_proxy(), std::move(trace_state_ptr), [&cinfo, cmd = make_lw_shared<query::read_command>(std::move(cmd)), src_addr = std::move(src_addr), da, max_size, t] (::compat::wrapping_partition_range& pr, shared_ptr<storage_proxy>& p, tracing::trace_state_ptr& trace_state_ptr) mutable {
            p->_stats.replica_digest_reads++;
            auto src_ip = src_addr.addr;
            return get_schema_for_read(cmd->schema_version, std::move(src_addr)).then([cmd, &pr, &p, &trace_state_ptr, max_size, t, da] (schema_ptr s) {
                auto pr2 = ::compat::unwrap(std::move(pr), *s);
                if (pr2.second) {
                    // this function assumes singular queries but doesn't validate
                    throw std::runtime_error("READ_DIGEST called with wrapping range");
                }
                auto timeout = t ? *t : db::no_timeout;
                return p->query_result_local_digest(std::move(s), cmd, std::move(pr2.first), trace_state_ptr, timeout, da, max_size);
            }).finally([&trace_state_ptr, src_ip] () mutable {
                tracing::trace(trace_state_ptr, "read_digest handling is done, sending a response to /{}", src_ip);
            });
        });
    });
    ms.register_truncate([](sstring ksname, sstring cfname) {
        return do_with(utils::make_joinpoint([] { return db_clock::now();}),
                        [ksname, cfname](auto& tsf) {
            return get_storage_proxy().invoke_on_all([ksname, cfname, &tsf](storage_proxy& sp) {
                return sp._db.local().truncate(ksname, cfname, [&tsf] { return tsf.value(); });
            });
        });
    });

    ms.register_get_schema_version([this] (unsigned shard, table_schema_version v) {
        _stats.replica_cross_shard_ops += shard != engine().cpu_id();
        return get_storage_proxy().invoke_on(shard, [v] (auto&& sp) {
            slogger.debug("Schema version request for {}", v);
            return local_schema_registry().get_frozen(v);
        });
    });
}

void storage_proxy::uninit_messaging_service() {
    auto& ms = netw::get_local_messaging_service();
    ms.unregister_mutation();
    ms.unregister_mutation_done();
    ms.unregister_mutation_failed();
    ms.unregister_read_data();
    ms.unregister_read_mutation_data();
    ms.unregister_read_digest();
    ms.unregister_truncate();
}

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>
storage_proxy::query_mutations_locally(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr,
                                       storage_proxy::clock_type::time_point timeout,
                                       tracing::trace_state_ptr trace_state, uint64_t max_size) {
    if (pr.is_singular()) {
        unsigned shard = _db.local().shard_of(pr.start()->value().token());
        _stats.replica_cross_shard_ops += shard != engine().cpu_id();
        return _db.invoke_on(shard, [max_size, cmd, &pr, gs=global_schema_ptr(s), timeout, gt = tracing::global_trace_state_ptr(std::move(trace_state))] (database& db) mutable {
          return db.get_result_memory_limiter().new_mutation_read(max_size).then([&] (query::result_memory_accounter ma) {
            return db.query_mutations(gs, *cmd, pr, std::move(ma), gt, timeout).then([] (reconcilable_result&& result, cache_temperature ht) {
                return make_ready_future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>(make_foreign(make_lw_shared(std::move(result))), ht);
            });
          });
        });
    } else {
        return query_nonsingular_mutations_locally(std::move(s), std::move(cmd), {pr}, std::move(trace_state), max_size, timeout);
    }
}

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>
storage_proxy::query_mutations_locally(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const ::compat::one_or_two_partition_ranges& pr,
                                       storage_proxy::clock_type::time_point timeout,
                                       tracing::trace_state_ptr trace_state, uint64_t max_size) {
    if (!pr.second) {
        return query_mutations_locally(std::move(s), std::move(cmd), pr.first, timeout, std::move(trace_state), max_size);
    } else {
        return query_nonsingular_mutations_locally(std::move(s), std::move(cmd), pr, std::move(trace_state), max_size, timeout);
    }
}

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>
storage_proxy::query_nonsingular_mutations_locally(schema_ptr s,
                                                   lw_shared_ptr<query::read_command> cmd,
                                                   const dht::partition_range_vector&& prs,
                                                   tracing::trace_state_ptr trace_state,
                                                   uint64_t max_size,
                                                   storage_proxy::clock_type::time_point timeout) {
    return do_with(cmd, std::move(prs), [=, s = std::move(s), trace_state = std::move(trace_state)] (lw_shared_ptr<query::read_command>& cmd,
                const dht::partition_range_vector& prs) mutable {
        return query_mutations_on_all_shards(_db, std::move(s), *cmd, prs, std::move(trace_state), max_size, timeout);
    });
}

future<> storage_proxy::start_hints_manager(shared_ptr<gms::gossiper> gossiper_ptr, shared_ptr<service::storage_service> ss_ptr) {
    return _hints_resource_manager.start(shared_from_this(), gossiper_ptr, ss_ptr);
}

void storage_proxy::allow_replaying_hints() noexcept {
    return _hints_resource_manager.allow_replaying();
}

void storage_proxy::on_join_cluster(const gms::inet_address& endpoint) {};

void storage_proxy::on_leave_cluster(const gms::inet_address& endpoint) {};

void storage_proxy::on_up(const gms::inet_address& endpoint) {};

void storage_proxy::on_down(const gms::inet_address& endpoint) {
    assert(thread::running_in_thread());
    for (auto it = _view_update_handlers_list->begin(); it != _view_update_handlers_list->end(); ++it) {
        auto guard = it->shared_from_this();
        if (it->get_targets().count(endpoint) > 0) {
            it->timeout_cb();
        }
        seastar::thread::yield();
    }
};

future<> storage_proxy::drain_on_shutdown() {
    return do_with(::shared_ptr<abstract_write_response_handler>(), [this] (::shared_ptr<abstract_write_response_handler>& intrusive_list_guard) {
        return do_for_each(*_view_update_handlers_list, [&intrusive_list_guard] (abstract_write_response_handler& handler) {
            intrusive_list_guard = handler.shared_from_this();
            handler.timeout_cb();
        });
    }).then([this] {
        return _hints_resource_manager.stop();
    });
}

future<>
storage_proxy::stop() {
    // FIXME: hints manager should be stopped here but it seems like this function is never called
    uninit_messaging_service();
    return make_ready_future<>();
}

}

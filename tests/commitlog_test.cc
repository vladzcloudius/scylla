/*
 * Copyright (C) 2015 ScyllaDB
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


#include <boost/test/unit_test.hpp>
#include <boost/range/adaptor/map.hpp>

#include <stdlib.h>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <set>

#include "tests/test-utils.hh"
#include "core/future-util.hh"
#include "core/do_with.hh"
#include "core/scollectd_api.hh"
#include "core/file.hh"
#include "core/reactor.hh"
#include "utils/UUID_gen.hh"
#include "tmpdir.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry_serializer.hh"
#include "db/commitlog/rp_set.hh"
#include "log.hh"
#include "test_services.hh"
#include "schema_builder.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace db;

#if 1
template<typename Func>
static future<> cl_test(commitlog::config cfg, Func && f) {
    tmpdir tmp;
    sstring clog_path = tmp.path;
    return seastar::async([cfg = std::move(cfg), f = std::forward<Func>(f), clog_path = std::move(clog_path)] () mutable {
        cfg.commit_log_location = clog_path;
        commitlog log = commitlog::create_commitlog(cfg).get0();
        storage_service_for_tests ssft;
        std::exception_ptr eptr;
        try {
            auto common_schema = schema_builder("ks", "test")
                .with_column("pk_col", bytes_type, column_kind::partition_key)
                .with_column("ck_col_1", bytes_type, column_kind::clustering_key);
            schema_ptr s = common_schema.build();
            futurize_apply(f, log, std::move(s)).get();
        } catch (...) {
            printf("\t\tReceived exception\n");
            eptr = std::current_exception();
        }

        printf("\t\tBefore log.shutdown()\n");
        log.shutdown().get();
        printf("\t\tBefore log.clear()\n");
        log.clear().get();

        if (eptr) {
            std::rethrow_exception(eptr);
        }
    }).finally([tmp = std::move(tmp)] {});
}

#else
template<typename Func>
static future<> cl_test(commitlog::config cfg, Func && f) {
    return seastar::async([cfg = std::move(cfg), f = std::forward<Func>(f)] () mutable {
        tmpdir tmp;
        cfg.commit_log_location = tmp.path;
        storage_service_for_tests ssft;
        auto common_schema = schema_builder("ks", "test")
            .with_column("pk_col", bytes_type, column_kind::partition_key)
            .with_column("ck_col_1", bytes_type, column_kind::clustering_key);
        schema_ptr s = common_schema.build();
        commitlog::create_commitlog(cfg).then([f = std::forward<Func>(f), s = std::move(s)](commitlog log) mutable {
            return do_with(std::move(log), [f = std::forward<Func>(f), s = std::move(s)](commitlog& log) {
                return futurize_apply(f, log, std::move(s)).finally([&log] {
                    return log.shutdown().then([&log] {
                        return log.clear();
                    });
                });
            });
        }).finally([tmp = std::move(tmp)] {
        }).get();
    });
}
#endif

template<typename Func>
static future<> cl_test(Func && f) {
    commitlog::config cfg;
    return cl_test(cfg, std::forward<Func>(f));
}

#if 0
static int loggo = [] {
        logging::logger_registry().set_logger_level("commitlog", logging::log_level::trace);
        return 0;
}();
#endif

class serializer_func_entry_writer final : public db::entry_writer {
    db::commitlog::serializer_func _func;
    size_t _size;
    schema_ptr _schema;
public:
    serializer_func_entry_writer(schema_ptr s, size_t sz, db::commitlog::serializer_func func)
        : _func(std::move(func)), _size(sz), _schema(s)
    { }
    virtual ~serializer_func_entry_writer() {}
    virtual size_t exact_size() const override { return _size; }
    virtual size_t estimate_size() const override { return _size; }
    virtual void write(data_output& out) const override {
        _func(out);
    }
    virtual void set_with_schema(bool) {}
    virtual bool with_schema() const { return false; }
    virtual schema_ptr schema() const { return _schema; }
};

commitlog::timeout_clock::time_point get_timeout_time_point(int nsec) {
    return commitlog::timeout_clock::now() + std::chrono::seconds(nsec);
}

#if 0
// just write in-memory...
SEASTAR_TEST_CASE(test_create_commitlog) {
    return cl_test([](commitlog& log, schema_ptr s) {
        sstring tmp = "hej bubba cow";

        shared_ptr<db::entry_writer> cew(make_shared<serializer_func_entry_writer>(s, tmp.size(), [&tmp] (db::commitlog::output& dst) {
            dst.write(tmp.begin(), tmp.end());
        }));
        db::replay_position rp = log.add_entry(s->id(), std::move(cew),  get_timeout_time_point(2)).get0();
        BOOST_CHECK_NE(rp, db::replay_position());
    });
}

// check we
SEASTAR_TEST_CASE(test_commitlog_written_to_disk_batch){
    commitlog::config cfg;
    cfg.mode = commitlog::sync_mode::BATCH;
    return cl_test(cfg, [](commitlog& log, schema_ptr s) {
        sstring tmp = "hej bubba cow";
        shared_ptr<db::entry_writer> cew(make_shared<serializer_func_entry_writer>(s, tmp.size(), [&tmp] (db::commitlog::output& dst) {
            dst.write(tmp.begin(), tmp.end());
        }));
        db::replay_position rp = log.add_entry(s->id(), std::move(cew),  get_timeout_time_point(2)).get0();
        BOOST_CHECK_NE(rp, db::replay_position());
        auto n = log.get_flush_count();
        BOOST_REQUIRE(n > 0);
    });
}

SEASTAR_TEST_CASE(test_commitlog_written_to_disk_periodic){
    return cl_test([](commitlog& log, schema_ptr s) {
        bool state = false;
        sstring tmp = "hej bubba cow";
        while (!state) {
            shared_ptr<db::entry_writer> cew(make_shared<serializer_func_entry_writer>(s, tmp.size(), [&tmp] (db::commitlog::output& dst) {
                dst.write(tmp.begin(), tmp.end());
            }));
            db::replay_position rp = log.add_entry(s->id(), std::move(cew),  get_timeout_time_point(2)).get0();
            BOOST_CHECK_NE(rp, db::replay_position());
            auto n = log.get_flush_count();
            state = n > 0;
        }
    });
}

SEASTAR_TEST_CASE(test_commitlog_new_segment){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log, schema_ptr s) {
        sstring tmp = "hej bubba cow";
        rp_set set;
        while (set.size() <= 1) {
            shared_ptr<db::entry_writer> cew(make_shared<serializer_func_entry_writer>(s, tmp.size(), [&tmp] (db::commitlog::output& dst) {
                dst.write(tmp.begin(), tmp.end());
            }));
            rp_handle h = log.add_entry(s->id(), std::move(cew),  get_timeout_time_point(2)).get0();
            BOOST_CHECK_NE(h.rp(), db::replay_position());
            set.put(std::move(h));
        }

        auto n = log.get_active_segment_names().size();
        BOOST_REQUIRE(n > 1);
    });
}
typedef std::vector<sstring> segment_names;

static segment_names segment_diff(commitlog& log, segment_names prev = {}) {
    segment_names now = log.get_active_segment_names();
    segment_names diff;
    // safety fix. We should always get segment names in alphabetical order, but
    // we're not explicitly guaranteed it. Lets sort the sets just to be sure.
    std::sort(now.begin(), now.end());
    std::sort(prev.begin(), prev.end());
    std::set_difference(prev.begin(), prev.end(), now.begin(), now.end(), std::back_inserter(diff));
    return diff;
}

SEASTAR_TEST_CASE(test_commitlog_discard_completed_segments){
    //logging::logger_registry().set_logger_level("commitlog", logging::log_level::trace);
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log, schema_ptr s) {
        printf("test_commitlog_discard_completed_segments\n");
        struct state_type {
            std::vector<schema_ptr> cfs;
            std::unordered_map<utils::UUID, db::rp_set> rps;

            mutable size_t index = 0;

            state_type(schema_ptr s0) {
                cfs.push_back(s0);
                for (int i = 1; i < 10; ++i) {
                    auto common_schema = schema_builder("ks", seastar::format("test{}", i))
                            .with_column("pk_col", bytes_type, column_kind::partition_key)
                            .with_column("ck_col_1", bytes_type, column_kind::clustering_key);
                    schema_ptr s = common_schema.build();
                    cfs.push_back(s);
                }
            }

            const schema_ptr& next_cf() const {
                return cfs[index++ % cfs.size()];
            }

            bool done() const {
                return std::any_of(rps.begin(), rps.end(), [](auto& rps) {
                    return rps.second.size() > 1;
                });
            }
        };

        state_type state(s);
        sstring tmp = "hej bubba cow";
        while (!state.done()) {
            schema_ptr next_cf = state.next_cf();
            shared_ptr<db::entry_writer> cew(make_shared<serializer_func_entry_writer>(next_cf, tmp.size(), [&tmp](db::commitlog::output& dst) {
                dst.write(tmp.begin(), tmp.end());
            }));
            db::rp_handle h = log.add_entry(next_cf->id(), std::move(cew), get_timeout_time_point(2)).get0();
            state.rps[next_cf->id()].put(std::move(h));
        }

        auto names = log.get_active_segment_names();
        BOOST_REQUIRE(names.size() > 1);
        // sync all so we have no outstanding async sync ops that
        // might prevent discard_completed_segments to actually dispose
        // of clean segments (shared_ptr in task)
        log.sync_all_segments().get();

        for (auto& p : state.rps) {
            log.discard_completed_segments(p.first, p.second);
        }
        auto diff = segment_diff(log, names);
        auto nn = diff.size();
        auto dn = log.get_num_segments_destroyed();

        BOOST_REQUIRE(nn > 0);
        BOOST_REQUIRE(nn <= names.size());
        BOOST_REQUIRE(dn <= nn);
    });
}

SEASTAR_TEST_CASE(test_equal_record_limit) {
    printf("test_equal_record_limit\n");
    return cl_test([](commitlog& log, schema_ptr s) {
        auto size = log.max_record_size();

        shared_ptr<db::entry_writer> cew(make_shared<serializer_func_entry_writer>(s, size, [size] (db::commitlog::output& dst) {
            dst.write(char(1), size);
        }));
        db::replay_position rp = log.add_entry(s->id(), std::move(cew),  get_timeout_time_point(2)).get0();
        BOOST_CHECK_NE(rp, db::replay_position());

        printf("test_equal_record_limit end\n");

    });
}
#else
//typedef std::vector<sstring> segment_names;
//static segment_names segment_diff(commitlog& log, segment_names prev = {}) {
//    segment_names now = log.get_active_segment_names();
//    segment_names diff;
//    // safety fix. We should always get segment names in alphabetical order, but
//    // we're not explicitly guaranteed it. Lets sort the sets just to be sure.
//    std::sort(now.begin(), now.end());
//    std::sort(prev.begin(), prev.end());
//    std::set_difference(prev.begin(), prev.end(), now.begin(), now.end(), std::back_inserter(diff));
//    return diff;
//}

#endif

SEASTAR_TEST_CASE(test_exceed_record_limit){
    commitlog::config cfg;
    cfg.commitlog_total_space_in_mb = 1;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log, schema_ptr s) {
        auto size = log.max_record_size() + 1;
        printf("test_exceed_record_limit: %ld\n", size);
#if 1

        shared_ptr<db::entry_writer> cew(make_shared<serializer_func_entry_writer>(s, size, [size] (db::commitlog::output& dst) {
            printf("\tbefore writing\n");
            dst.write(char(1), size);
            printf("\tafter writing\n");
        }));
        rp_set set;

        set.put(log.add_entry(s->id(), cew, get_timeout_time_point(2)).get0());

        try {
            printf("\tbefore add_entry()\n");
            set.put(log.add_entry(s->id(), cew, get_timeout_time_point(2)).get0());
            printf("\tafter add_entry()\n");
        } catch (std::exception& e) {
            // ok.
            printf("\tGot exception - all is good! %s\n", e.what());
            return;
        }
#endif
        throw std::runtime_error("Did not get expected exception from writing too large record");
        printf("test_exceed_record_limit end\n");
    });
}

#if 0
SEASTAR_TEST_CASE(test_commitlog_delete_when_over_disk_limit) {
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 2;
    cfg.commitlog_total_space_in_mb = 1;
    cfg.commitlog_sync_period_in_ms = 1;
    return cl_test(cfg, [](commitlog& log, schema_ptr s) {
        printf("test_commitlog_delete_when_over_disk_limit\n");
        semaphore sem(0);
        segment_names segments;

        // add a flush handler that simply says we're done with the range.
        auto r = log.add_flush_handler([&log, &sem, &segments](cf_id_type id, replay_position pos) {
            segments = log.get_active_segment_names();
            log.discard_completed_segments(id);
            sem.signal();
        });

        std::set<segment_id_type> set;
        sstring tmp = "hej bubba cow";

        while (!(set.size() > 2 && sem.try_wait())) {
            shared_ptr<db::entry_writer> cew(make_shared<serializer_func_entry_writer>(s, tmp.size(), [&tmp](db::commitlog::output& dst) {
                dst.write(tmp.begin(), tmp.end());
            }));
            db::rp_handle h = log.add_entry(s->id(), std::move(cew), get_timeout_time_point(2)).get0();
            BOOST_CHECK_NE(h.rp(), db::replay_position());
            set.insert(h.release().id);
        }

        auto diff = segment_diff(log, segments);
        auto nn = diff.size();
        auto dn = log.get_num_segments_destroyed();

        BOOST_REQUIRE(nn > 0);
        BOOST_REQUIRE(nn <= segments.size());
        BOOST_REQUIRE(dn <= nn);
    });
}

SEASTAR_TEST_CASE(test_commitlog_reader){
    static auto count_mutations_in_segment = [] (sstring path) -> future<size_t> {
        auto count = make_lw_shared<size_t>(0);
        return db::commitlog::read_log_file(path, [count](temporary_buffer<char> buf, db::replay_position rp) {
            sstring str(buf.get(), buf.size());
            BOOST_CHECK_EQUAL(str, "hej bubba cow");
            (*count)++;
            return make_ready_future<>();
        }).then([](auto s) {
            return do_with(std::move(s), [](auto& s) {
                return s->done();
            });
        }).then([count] {
            return *count;
        });
    };
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log, schema_ptr s) {
        rp_set set;
        size_t count = 0;
        sstring tmp = "hej bubba cow";

        while (set.size() <= 1) {
            shared_ptr<db::entry_writer> cew(make_shared<serializer_func_entry_writer>(s, tmp.size(), [&tmp](db::commitlog::output& dst) {
                dst.write(tmp.begin(), tmp.end());
            }));
            db::rp_handle h = log.add_entry(s->id(), std::move(cew), get_timeout_time_point(2)).get0();

            BOOST_CHECK_NE(db::replay_position(), h.rp());
            set.put(std::move(h));
            if (set.size() == 1) {
                ++count;
            }
        }

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(segments.size() > 1);

        auto ids = boost::copy_range<std::vector<segment_id_type>>(set.usage() | boost::adaptors::map_keys);
        std::sort(ids.begin(), ids.end());
        auto id = ids.front();
        auto i = std::find_if(segments.begin(), segments.end(), [&id](sstring filename) {
            commitlog::descriptor desc(filename);
            return desc.id == id;
        });
        if (i == segments.end()) {
            throw std::runtime_error("Did not find expected log file");
        }
        auto segment_path = *i;
        // Check reading from an unsynced segment
        size_t replay_count = count_mutations_in_segment(segment_path).get0();

        BOOST_CHECK_GE(count, replay_count);

        log.sync_all_segments().get();
        replay_count = count_mutations_in_segment(segment_path).get0();

        BOOST_CHECK_EQUAL(count, replay_count);
    });
}

static future<> corrupt_segment(sstring seg, uint64_t off, uint32_t value) {
    return open_file_dma(seg, open_flags::rw).then([off, value](file f) {
        size_t size = align_up<size_t>(off, 4096);
        return do_with(std::move(f), [size, off, value](file& f) {
            return f.dma_read_exactly<char>(0, size).then([&f, off, value](auto buf) {
                *unaligned_cast<uint32_t *>(buf.get_write() + off) = value;
                auto dst = buf.get();
                auto size = buf.size();
                return f.dma_write(0, dst, size).then([buf = std::move(buf)](size_t) {});
            });
        });
    });
}

SEASTAR_TEST_CASE(test_commitlog_entry_corruption){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log, schema_ptr s) {
        std::vector<db::replay_position> rps;
        sstring tmp = "hej bubba cow";

        while (rps.size() <= 1) {
            shared_ptr<db::entry_writer> cew(make_shared<serializer_func_entry_writer>(s, tmp.size(), [&tmp](db::commitlog::output& dst) {
                dst.write(tmp.begin(), tmp.end());
            }));
            db::rp_handle h = log.add_entry(s->id(), std::move(cew), get_timeout_time_point(2)).get0();

            BOOST_CHECK_NE(h.rp(), db::replay_position());
            rps.push_back(h.release());
        }

        log.sync_all_segments().get();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());
        auto seg = segments[0];

        corrupt_segment(seg, rps.at(1).pos + 4, 0x451234ab).get();

        try {
            auto sb = db::commitlog::read_log_file(seg, [&rps](temporary_buffer<char> buf, db::replay_position rp) {
                BOOST_CHECK_EQUAL(rp, rps.at(0));
                return make_ready_future<>();
            }).get0();

            sb->done().get();
            BOOST_FAIL("Expected exception");
        } catch (commitlog::segment_data_corruption_error& e) {
            // ok.
            BOOST_REQUIRE(e.bytes() > 0);
        }
    });
}


SEASTAR_TEST_CASE(test_commitlog_chunk_corruption){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log, schema_ptr s) {
        std::vector<db::replay_position> rps;
        sstring tmp = "hej bubba cow";

        while (rps.size() <= 1) {
            shared_ptr<db::entry_writer> cew(make_shared<serializer_func_entry_writer>(s, tmp.size(), [&tmp](db::commitlog::output& dst) {
                dst.write(tmp.begin(), tmp.end());
            }));
            db::rp_handle h = log.add_entry(s->id(), std::move(cew), get_timeout_time_point(2)).get0();

            BOOST_CHECK_NE(h.rp(), db::replay_position());
            rps.push_back(h.release());
        }

        log.sync_all_segments().get();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());
        auto seg = segments[0];

        corrupt_segment(seg, rps.at(0).pos - 4, 0x451234ab).get();

        try {
            auto sb = db::commitlog::read_log_file(seg, [&rps](temporary_buffer<char> buf, db::replay_position rp) {
                BOOST_FAIL("Should not reach");
                return make_ready_future<>();
            }).get0();

            sb->done().get();
            BOOST_FAIL("Expected exception");
        } catch (commitlog::segment_data_corruption_error& e) {
            // ok.
            BOOST_REQUIRE(e.bytes() > 0);
        }
    });
}


SEASTAR_TEST_CASE(test_commitlog_reader_produce_exception){
    commitlog::config cfg;
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log, schema_ptr s) {
        std::vector<db::replay_position> rps;
        sstring tmp = "hej bubba cow";

        while (rps.size() <= 1) {
            shared_ptr<db::entry_writer> cew(make_shared<serializer_func_entry_writer>(s, tmp.size(), [&tmp](db::commitlog::output& dst) {
                dst.write(tmp.begin(), tmp.end());
            }));
            db::rp_handle h = log.add_entry(s->id(), std::move(cew), get_timeout_time_point(2)).get0();
            BOOST_CHECK_NE(h.rp(), db::replay_position());
            rps.push_back(h.release());
        }

        log.sync_all_segments().get();

        auto segments = log.get_active_segment_names();
        BOOST_REQUIRE(!segments.empty());
        auto seg = segments[0];

        try {
            auto sb = db::commitlog::read_log_file(seg, [&rps](temporary_buffer<char> buf, db::replay_position rp) {
                return make_exception_future(std::runtime_error("I am in a throwing mode"));
            }).get0();

            sb->done().get();
            BOOST_FAIL("Expected exception");
        } catch(std::runtime_error&) {
            // Ok
            return;
        } catch(...) {
            // Not ok!
            BOOST_FAIL("Wrong exception");
        }
    });
}


SEASTAR_TEST_CASE(test_commitlog_counters) {
    auto count_cl_counters = []() -> size_t {
        auto ids = scollectd::get_collectd_ids();
        return std::count_if(ids.begin(), ids.end(), [](const scollectd::type_instance_id& id) {
            return id.plugin() == "commitlog";
        });
    };
    BOOST_CHECK_EQUAL(count_cl_counters(), 0);
    return cl_test([count_cl_counters](commitlog& log, schema_ptr s) {
        BOOST_CHECK_GT(count_cl_counters(), 0);
    }).finally([count_cl_counters] {
        BOOST_CHECK_EQUAL(count_cl_counters(), 0);
    });
}

#ifndef DEFAULT_ALLOCATOR

SEASTAR_TEST_CASE(test_allocation_failure){
    return cl_test([](commitlog& log, schema_ptr s) {
        auto size = log.max_record_size() - 1;
        std::list<std::unique_ptr<char[]>> junk;

        // Use us loads of memory so we can OOM at the appropriate place
        try {
            for (;;) {
                junk.emplace_back(new char[size]);
            }
        } catch (std::bad_alloc&) {
        }

        try {
            shared_ptr<db::entry_writer> cew(make_shared<serializer_func_entry_writer>(s, size, [size](db::commitlog::output& dst) {
                dst.write(char(1), size);
            }));
            log.add_entry(s->id(), std::move(cew), get_timeout_time_point(2)).get0();
        } catch (std::bad_alloc&) {
            // ok. this is what we expected
            junk.clear();
            return;
        } catch (...) {
        }
        throw std::runtime_error("Did not get expected exception from writing too large record");
    });
}
#endif
#endif
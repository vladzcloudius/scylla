/*
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


#include <boost/test/unit_test.hpp>

#include <stdlib.h>
#include <iostream>
#include <list>
#include <unordered_set>

#include "tests/test_services.hh"
#include "tests/test-utils.hh"

#include "tests/mutation_source_test.hh"
#include "tests/mutation_assertions.hh"

#include "core/future-util.hh"
#include "core/do_with.hh"
#include "core/scollectd_api.hh"
#include "core/file.hh"
#include "core/reactor.hh"
#include "utils/UUID_gen.hh"
#include "tmpdir.hh"
#include "db/commitlog/commitlog.hh"
#include "db/hints/hint_entry_serializer.hh"
#include "log.hh"
#include "schema.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace db;

template<typename Func>
static future<> cl_test(commitlog::config cfg, Func && f) {
    tmpdir tmp;
    cfg.commit_log_location = tmp.path;
    return commitlog::create_commitlog(cfg).then([f = std::forward<Func>(f)](commitlog log) mutable {
        return do_with(std::move(log), [f = std::forward<Func>(f)](commitlog& log) {
            return futurize_apply(f, log).finally([&log] {
                return log.shutdown().then([&log] {
                    return log.clear();
                });
            });
        });
    }).finally([tmp = std::move(tmp)] {
    });
}

template<typename Func>
static future<> cl_test(Func && f) {
    commitlog::config cfg;
    return cl_test(cfg, std::forward<Func>(f));
}

SEASTAR_TEST_CASE(test_commitlog_new_segment_custom_prefix){
    commitlog::config cfg;
    cfg.fname_prefix = "HintedLog-0-kaka-";
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log) {
        return do_with(rp_set(), [&log](auto& set) {
            auto uuid = utils::UUID_gen::get_time_UUID();
            return do_until([&set]() { return set.size() > 1; }, [&log, &set, uuid]() {
                sstring tmp = "hej bubba cow";
                return log.add_mutation(uuid, tmp.size(), [tmp](db::commitlog::output& dst) {
                    dst.write(tmp.begin(), tmp.end());
                }).then([&set](rp_handle h) {
                    BOOST_CHECK_NE(h.rp(), db::replay_position());
                    set.put(std::move(h));
                });
            });
        }).then([&log] {
//          std::cout << log.get_active_segment_names() <<std::endl;
            auto n = log.get_active_segment_names().size();
            BOOST_REQUIRE(n > 1);
        });
    });
}

bool operator==(const column_mapping_entry& a, const column_mapping_entry& b) {
    return a.name() == b.name() && a.type_name() == b.type_name();
}

bool operator==(const column_mapping& a, const column_mapping& b) {
    auto& a_columns = a.columns();
    auto& b_columns = b.columns();

    return a_columns == b_columns;
}

SEASTAR_TEST_CASE(test_hints_writing_and_reading) {
    commitlog::config cfg;
    cfg.fname_prefix = "HintedLog-0-kaka-";
    cfg.commitlog_segment_size_in_mb = 1;
    return cl_test(cfg, [](commitlog& log) {
        return seastar::async([&log] {

            struct hint {
                lw_shared_ptr<const frozen_mutation> fm;
                db::hints::ts_clock::time_point ts;
                db::hints::gc_gs_clock::time_point gc_gs;
                column_mapping mapping;
                schema_ptr s;
            };

            lw_shared_ptr<rp_set> set = make_lw_shared<rp_set>();
            lw_shared_ptr<std::list<hint>> hints_list(make_lw_shared<std::list<hint>>());
            storage_service_for_tests ssft;

            for_each_mutation([&log, hints_list, set] (const mutation& m) {
                auto fm = make_lw_shared(freeze(m));
                shared_ptr<hints::entry_writer> hew(make_shared<hints::entry_writer>(m.schema(), *fm));

                hint h{std::move(fm),
                       hew->ts(),
                       hew->min_gc_gs(),
                       m.schema()->get_column_mapping(),
                       m.schema()
                };

                set->put(log.add_entry(h.s->id(), std::move(hew), commitlog::timeout_clock::now() + hints::max_write_duration_ms).get0());
                hints_list->push_back(std::move(h));
            });

            log.sync_all_segments().get();

            auto segs = log.get_active_segment_names();
            printf("Hints written, before reading %ld hints...\n", hints_list->size());
            seastar::print("%s\n", segs);

            std::for_each(segs.begin(), segs.end(), [hints_list] (const sstring& path) {
                printf("\tReading from %s...\n", path.c_str());
                commitlog::read_log_file(path, [hints_list] (temporary_buffer<char> buf, replay_position rp) {
                    BOOST_REQUIRE(!hints_list->empty());
                    auto& orig_hint = hints_list->front();

                    hint_entry h = hints::entry_reader::deserialize(std::move(buf));
                    commitlog_entry& hint_commitlog_entry = h.cl_entry;
                    const frozen_mutation& fm = hint_commitlog_entry.mutation();
                    const frozen_mutation& orig_fm = *orig_hint.fm;
                    schema_ptr orig_schema = orig_hint.s;

                    BOOST_REQUIRE_EQUAL(orig_fm.schema_version(), fm.schema_version());
                    assert_that(fm.unfreeze(orig_schema)).is_equal_to(orig_fm.unfreeze(orig_schema));
                    BOOST_REQUIRE(fm.decorated_key(*orig_schema).equal(*orig_schema, orig_fm.decorated_key(*orig_schema)));
                    BOOST_REQUIRE_EQUAL(orig_hint.ts.time_since_epoch().count(), h.ts().time_since_epoch().count());
                    BOOST_REQUIRE_EQUAL(orig_hint.gc_gs.time_since_epoch().count(), h.min_gc_gs().time_since_epoch().count());

                    if (hint_commitlog_entry.mapping()) {
                        seastar::print("\t\tChecking the columns mapping for CF %s\n", fm.schema_version());
                        BOOST_REQUIRE_EQUAL(*hint_commitlog_entry.mapping(), orig_hint.mapping);
                    } else {
                        printf("\t\tNo mapping info\n");
                    }

                    hints_list->pop_front();

                    return make_ready_future<>();
                }).then([](auto s) {
                    return do_with(std::move(s), [](auto& s) {
                        return s->done();
                    });
                }).get();
            });
        });
    });
}


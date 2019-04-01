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

#include "cql3/statements/select_statement.hh"
#include "cql3/statements/raw/select_statement.hh"

#include "transport/messages/result_message.hh"
#include "cql3/functions/as_json_function.hh"
#include "cql3/selection/selection.hh"
#include "cql3/util.hh"
#include "cql3/restrictions/single_column_primary_key_restrictions.hh"
#include <seastar/core/shared_ptr.hh>
#include "query-result-reader.hh"
#include "query_result_merger.hh"
#include "service/pager/query_pagers.hh"
#include <seastar/core/execution_stage.hh>
#include "view_info.hh"
#include "partition_slice_builder.hh"
#include "cql3/untyped_result_set.hh"
#include "db/timeout_clock.hh"
#include "db/consistency_level_validations.hh"
#include "database.hh"
#include <boost/algorithm/cxx11/any_of.hpp>

namespace cql3 {

namespace statements {

thread_local const shared_ptr<select_statement::parameters> select_statement::_default_parameters = ::make_shared<select_statement::parameters>();

select_statement::parameters::parameters()
    : _is_distinct{false}
    , _allow_filtering{false}
    , _is_json{false}
{ }

select_statement::parameters::parameters(orderings_type orderings,
                                         bool is_distinct,
                                         bool allow_filtering)
    : _orderings{std::move(orderings)}
    , _is_distinct{is_distinct}
    , _allow_filtering{allow_filtering}
    , _is_json{false}
{ }

select_statement::parameters::parameters(orderings_type orderings,
                                         bool is_distinct,
                                         bool allow_filtering,
                                         bool is_json,
                                         bool bypass_cache)
    : _orderings{std::move(orderings)}
    , _is_distinct{is_distinct}
    , _allow_filtering{allow_filtering}
    , _is_json{is_json}
    , _bypass_cache{bypass_cache}
{ }

bool select_statement::parameters::is_distinct() const {
    return _is_distinct;
}

bool select_statement::parameters::is_json() const {
   return _is_json;
}

bool select_statement::parameters::allow_filtering() const {
    return _allow_filtering;
}

bool select_statement::parameters::bypass_cache() const {
    return _bypass_cache;
}

select_statement::parameters::orderings_type const& select_statement::parameters::orderings() const {
    return _orderings;
}

timeout_config_selector
select_timeout(const restrictions::statement_restrictions& restrictions) {
    if (restrictions.is_key_range()) {
        return &timeout_config::range_read_timeout;
    } else {
        return &timeout_config::read_timeout;
    }
}

select_statement::select_statement(schema_ptr schema,
                                   uint32_t bound_terms,
                                   ::shared_ptr<parameters> parameters,
                                   ::shared_ptr<selection::selection> selection,
                                   ::shared_ptr<restrictions::statement_restrictions> restrictions,
                                   bool is_reversed,
                                   ordering_comparator_type ordering_comparator,
                                   ::shared_ptr<term> limit,
                                   ::shared_ptr<term> per_partition_limit,
                                   cql_stats& stats)
    : cql_statement(select_timeout(*restrictions))
    , _schema(schema)
    , _bound_terms(bound_terms)
    , _parameters(std::move(parameters))
    , _selection(std::move(selection))
    , _restrictions(std::move(restrictions))
    , _is_reversed(is_reversed)
    , _limit(std::move(limit))
    , _per_partition_limit(std::move(per_partition_limit))
    , _ordering_comparator(std::move(ordering_comparator))
    , _stats(stats)
{
    _opts = _selection->get_query_options();
    _opts.set_if<query::partition_slice::option::bypass_cache>(_parameters->bypass_cache());
}

bool select_statement::uses_function(const sstring& ks_name, const sstring& function_name) const {
    return _selection->uses_function(ks_name, function_name)
        || _restrictions->uses_function(ks_name, function_name)
        || (_limit && _limit->uses_function(ks_name, function_name));
}

::shared_ptr<const cql3::metadata> select_statement::get_result_metadata() const {
    // FIXME: COUNT needs special result metadata handling.
    return _selection->get_result_metadata();
}

uint32_t select_statement::get_bound_terms() {
    return _bound_terms;
}

future<> select_statement::check_access(const service::client_state& state) {
    try {
        auto&& s = service::get_local_storage_proxy().get_db().local().find_schema(keyspace(), column_family());
        auto& cf_name = s->is_view() ? s->view_info()->base_name() : column_family();
        return state.has_column_family_access(keyspace(), cf_name, auth::permission::SELECT);
    } catch (const no_such_column_family& e) {
        // Will be validated afterwards.
        return make_ready_future<>();
    }
}

void select_statement::validate(service::storage_proxy&, const service::client_state& state) {
    // Nothing to do, all validation has been done by raw_statemet::prepare()
}

bool select_statement::depends_on_keyspace(const sstring& ks_name) const {
    return keyspace() == ks_name;
}

bool select_statement::depends_on_column_family(const sstring& cf_name) const {
    return column_family() == cf_name;
}

const sstring& select_statement::keyspace() const {
    return _schema->ks_name();
}

const sstring& select_statement::column_family() const {
    return _schema->cf_name();
}

query::partition_slice
select_statement::make_partition_slice(const query_options& options)
{
    query::column_id_vector static_columns;
    query::column_id_vector regular_columns;

    if (_selection->contains_static_columns()) {
        static_columns.reserve(_selection->get_column_count());
    }

    regular_columns.reserve(_selection->get_column_count());

    for (auto&& col : _selection->get_columns()) {
        if (col->is_static()) {
            static_columns.push_back(col->id);
        } else if (col->is_regular()) {
            regular_columns.push_back(col->id);
        }
    }

    if (_parameters->is_distinct()) {
        _opts.set(query::partition_slice::option::distinct);
        return query::partition_slice({ query::clustering_range::make_open_ended_both_sides() },
            std::move(static_columns), {}, _opts, nullptr, options.get_cql_serialization_format());
    }

    auto bounds =_restrictions->get_clustering_bounds(options);
    if (bounds.size() > 1) {
        auto comparer = position_in_partition::less_compare(*_schema);
        auto bounds_sorter = [&comparer] (const query::clustering_range& lhs, const query::clustering_range& rhs) {
            return comparer(position_in_partition_view::for_range_start(lhs), position_in_partition_view::for_range_start(rhs));
        };
        std::sort(bounds.begin(), bounds.end(), bounds_sorter);
    }
    if (_is_reversed) {
        _opts.set(query::partition_slice::option::reversed);
        std::reverse(bounds.begin(), bounds.end());
        ++_stats.reverse_queries;
    }
    return query::partition_slice(std::move(bounds),
        std::move(static_columns), std::move(regular_columns), _opts, nullptr, options.get_cql_serialization_format(), get_per_partition_limit(options));
}

uint32_t select_statement::do_get_limit(const query_options& options, ::shared_ptr<term> limit) const {
    if (!limit || _selection->is_aggregate()) {
        return query::max_rows;
    }

    auto val = limit->bind_and_get(options);
    if (val.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null value of limit");
    }
    if (val.is_unset_value()) {
        return query::max_rows;
    }
  return with_linearized(*val, [&] (bytes_view bv) {
    try {
        int32_type->validate(bv);
        auto l = value_cast<int32_t>(int32_type->deserialize(bv));
        if (l <= 0) {
            throw exceptions::invalid_request_exception("LIMIT must be strictly positive");
        }
        return l;
    } catch (const marshal_exception& e) {
        throw exceptions::invalid_request_exception("Invalid limit value");
    }
  });
}

bool select_statement::needs_post_query_ordering() const {
    // We need post-query ordering only for queries with IN on the partition key and an ORDER BY.
    return _restrictions->key_is_in_relation() && !_parameters->orderings().empty();
}

struct select_statement_executor {
    static auto get() { return &select_statement::do_execute; }
};

static thread_local inheriting_concrete_execution_stage<
        future<shared_ptr<cql_transport::messages::result_message>>,
        select_statement*,
        service::storage_proxy&,
        service::query_state&,
        const query_options&> select_stage{"cql3_select", select_statement_executor::get()};

future<shared_ptr<cql_transport::messages::result_message>>
select_statement::execute(service::storage_proxy& proxy,
                             service::query_state& state,
                             const query_options& options)
{
    return select_stage(this, seastar::ref(proxy), seastar::ref(state), seastar::cref(options));
}

future<shared_ptr<cql_transport::messages::result_message>>
select_statement::do_execute(service::storage_proxy& proxy,
                          service::query_state& state,
                          const query_options& options)
{
    tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());

    auto cl = options.get_consistency();

    validate_for_read(cl);

    int32_t limit = get_limit(options);
    auto now = gc_clock::now();

    const bool restrictions_need_filtering = _restrictions->need_filtering();
    ++_stats.reads;
    _stats.filtered_reads += restrictions_need_filtering;

    auto command = ::make_lw_shared<query::read_command>(_schema->id(), _schema->version(),
        make_partition_slice(options), limit, now, tracing::make_trace_info(state.get_trace_state()), query::max_partitions, utils::UUID(), options.get_timestamp(state));

    int32_t page_size = options.get_page_size();

    _stats.unpaged_select_queries += page_size <= 0;

    // An aggregation query will never be paged for the user, but we always page it internally to avoid OOM.
    // If we user provided a page_size we'll use that to page internally (because why not), otherwise we use our default
    // Note that if there are some nodes in the cluster with a version less than 2.0, we can't use paging (CASSANDRA-6707).
    const bool aggregate = _selection->is_aggregate();
    const bool nonpaged_filtering = restrictions_need_filtering && page_size <= 0;
    if (aggregate || nonpaged_filtering) {
        page_size = DEFAULT_COUNT_PAGE_SIZE;
    }

    auto key_ranges = _restrictions->get_partition_key_ranges(options);

    if (!aggregate && !restrictions_need_filtering && (page_size <= 0
            || !service::pager::query_pagers::may_need_paging(*_schema, page_size,
                    *command, key_ranges))) {
        return execute(proxy, command, std::move(key_ranges), state, options, now);
    }

    command->slice.options.set<query::partition_slice::option::allow_short_read>();
    auto timeout_duration = options.get_timeout_config().*get_timeout_config_selector();
    auto p = service::pager::query_pagers::pager(_schema, _selection,
            state, options, command, std::move(key_ranges), _stats, restrictions_need_filtering ? _restrictions : nullptr);

    tracing::trace(state.get_trace_state(), "Begin fetching data");

    if (aggregate || nonpaged_filtering) {
        return do_with(
                cql3::selection::result_set_builder(*_selection, now,
                        options.get_cql_serialization_format()),
                [this, p, page_size, now, timeout_duration, restrictions_need_filtering](auto& builder) {
                    return do_until([p] {return p->is_exhausted();},
                            [p, &builder, page_size, now, timeout_duration] {
                                auto timeout = db::timeout_clock::now() + timeout_duration;
                                return p->fetch_page(builder, page_size, now, timeout);
                            }
                    ).then([this, &builder, restrictions_need_filtering] {
                                auto rs = builder.build();
                                if (restrictions_need_filtering) {
                                    _stats.filtered_rows_matched_total += rs->size();
                                }
                                update_stats_rows_read(rs->size());
                                auto msg = ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs)));
                                return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(std::move(msg));
                            });
                });
    }

    if (needs_post_query_ordering()) {
        throw exceptions::invalid_request_exception(
                "Cannot page queries with both ORDER BY and a IN restriction on the partition key;"
                        " you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query");
    }

    auto timeout = db::timeout_clock::now() + timeout_duration;
    if (_selection->is_trivial() && !restrictions_need_filtering && !_per_partition_limit) {
        tracing::trace(state.get_trace_state(), "Trivial selection without restrictions and partition limits");
        return p->fetch_page_generator(page_size, now, timeout, _stats).then([this, p] (result_generator generator) {
            auto meta = [&] () -> shared_ptr<const cql3::metadata> {
                if (!p->is_exhausted()) {
                    auto meta = make_shared<metadata>(*_selection->get_result_metadata());
                    meta->set_paging_state(p->state());
                    return meta;
                } else {
                    return _selection->get_result_metadata();
                }
            }();

            return shared_ptr<cql_transport::messages::result_message>(
                make_shared<cql_transport::messages::result_message::rows>(result(std::move(generator), std::move(meta)))
            );
        });
    }

    return p->fetch_page(page_size, now, timeout).then(
            [this, p, &options, now, restrictions_need_filtering](std::unique_ptr<cql3::result_set> rs) {

                if (!p->is_exhausted()) {
                    rs->get_metadata().set_paging_state(p->state());
                }

                if (restrictions_need_filtering) {
                    _stats.filtered_rows_matched_total += rs->size();
                }
                update_stats_rows_read(rs->size());
                auto msg = ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs)));
                return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(std::move(msg));
            });
}

template<typename KeyType>
GCC6_CONCEPT(
    requires (std::is_same_v<KeyType, partition_key> || std::is_same_v<KeyType, clustering_key_prefix>)
)
static KeyType
generate_base_key_from_index_pk(const partition_key& index_pk, const clustering_key& index_ck, const schema& base_schema, const schema& view_schema) {
    const auto& base_columns = std::is_same_v<KeyType, partition_key> ? base_schema.partition_key_columns() : base_schema.clustering_key_columns();
    std::vector<bytes_view> exploded_base_key;
    exploded_base_key.reserve(base_columns.size());

    for (const column_definition& base_col : base_columns) {
        const column_definition* view_col = view_schema.view_info()->view_column(base_col);
        if (view_col->is_partition_key()) {
            exploded_base_key.push_back(index_pk.get_component(view_schema, view_col->id));
        } else {
            exploded_base_key.push_back(index_ck.get_component(view_schema, view_col->id));
        }
    }
    return KeyType::from_range(exploded_base_key);
}

lw_shared_ptr<query::read_command>
indexed_table_select_statement::prepare_command_for_base_query(const query_options& options, service::query_state& state, gc_clock::time_point now, bool use_paging) {
    lw_shared_ptr<query::read_command> cmd = ::make_lw_shared<query::read_command>(
            _schema->id(),
            _schema->version(),
            make_partition_slice(options),
            get_limit(options),
            now,
            tracing::make_trace_info(state.get_trace_state()),
            query::max_partitions,
            utils::UUID(),
            options.get_timestamp(state));
    if (use_paging) {
        cmd->slice.options.set<query::partition_slice::option::allow_short_read>();
        cmd->slice.options.set<query::partition_slice::option::send_partition_key>();
        if (_schema->clustering_key_size() > 0) {
            cmd->slice.options.set<query::partition_slice::option::send_clustering_key>();
        }
    }
    return cmd;
}

future<shared_ptr<cql_transport::messages::result_message>>
indexed_table_select_statement::execute_base_query(
        service::storage_proxy& proxy,
        dht::partition_range_vector&& partition_ranges,
        service::query_state& state,
        const query_options& options,
        gc_clock::time_point now,
        ::shared_ptr<const service::pager::paging_state> paging_state) {
    auto cmd = prepare_command_for_base_query(options, state, now, bool(paging_state));
    auto timeout = db::timeout_clock::now() + options.get_timeout_config().*get_timeout_config_selector();
    uint32_t queried_ranges_count = partition_ranges.size();
    service::query_ranges_to_vnodes_generator ranges_to_vnodes(_schema, std::move(partition_ranges));

    struct base_query_state {
        query::result_merger merger;
        service::query_ranges_to_vnodes_generator ranges_to_vnodes;
        size_t concurrency = 1;
        base_query_state(uint32_t row_limit, service::query_ranges_to_vnodes_generator&& ranges_to_vnodes_)
                : merger(row_limit, query::max_partitions)
                , ranges_to_vnodes(std::move(ranges_to_vnodes_))
                {}
        base_query_state(base_query_state&&) = default;
        base_query_state(const base_query_state&) = delete;
    };

    base_query_state query_state{cmd->row_limit * queried_ranges_count, std::move(ranges_to_vnodes)};
    return do_with(std::move(query_state), [this, &proxy, &state, &options, cmd, timeout] (auto&& query_state) {
        auto& [merger, ranges_to_vnodes, concurrency] = query_state;
        return repeat([this, &ranges_to_vnodes, &merger, &proxy, &state, &options, &concurrency, cmd, timeout]() {
            // Starting with 1 range, we check if the result was a short read, and if not,
            // we continue exponentially, asking for 2x more ranges than before
            dht::partition_range_vector prange = ranges_to_vnodes(concurrency);
            auto command = ::make_lw_shared<query::read_command>(*cmd);
            auto old_paging_state = options.get_paging_state();
            if (old_paging_state && concurrency == 1) {
                auto base_pk = generate_base_key_from_index_pk<partition_key>(old_paging_state->get_partition_key(),
                        *old_paging_state->get_clustering_key(), *_schema, *_view_schema);
                auto base_ck = generate_base_key_from_index_pk<clustering_key>(old_paging_state->get_partition_key(),
                        *old_paging_state->get_clustering_key(), *_schema, *_view_schema);
                command->slice.set_range(*_schema, base_pk,
                        std::vector<query::clustering_range>{query::clustering_range::make_starting_with(range_bound<clustering_key>(base_ck, false))});
            }
            concurrency *= 2;
            return proxy.query(_schema, command, std::move(prange), options.get_consistency(), {timeout, state.get_trace_state()})
            .then([&ranges_to_vnodes, &merger] (service::storage_proxy::coordinator_query_result qr) {
                bool is_short_read = qr.query_result->is_short_read();
                merger(std::move(qr.query_result));
                return stop_iteration(is_short_read || ranges_to_vnodes.empty());
            });
        }).then([&merger]() {
            return merger.get();
        });
    }).then([this, &proxy, &state, &options, now, cmd, paging_state = std::move(paging_state)] (foreign_ptr<lw_shared_ptr<query::result>> result) mutable {
        return this->process_base_query_results(std::move(result), cmd, proxy, state, options, now, std::move(paging_state));
    });
}

// Function for fetching the selected columns from a list of clustering rows.
// It is currently used only in our Secondary Index implementation - ordinary
// CQL SELECT statements do not have the syntax to request a list of rows.
// FIXME: The current implementation is very inefficient - it requests each
// row separately (and, incrementally, in parallel). Even multiple rows from a single
// partition are requested separately. This last case can be easily improved,
// but to implement the general case (multiple rows from multiple partitions)
// efficiently, we will need more support from other layers.
// Keys are ordered in token order (see #3423)
future<shared_ptr<cql_transport::messages::result_message>>
indexed_table_select_statement::execute_base_query(
        service::storage_proxy& proxy,
        std::vector<primary_key>&& primary_keys,
        service::query_state& state,
        const query_options& options,
        gc_clock::time_point now,
        ::shared_ptr<const service::pager::paging_state> paging_state) {
    auto cmd = prepare_command_for_base_query(options, state, now, bool(paging_state));
    auto timeout = db::timeout_clock::now() + options.get_timeout_config().*get_timeout_config_selector();

    struct base_query_state {
        query::result_merger merger;
        std::vector<primary_key> primary_keys;
        std::vector<primary_key>::iterator current_primary_key;
        base_query_state(uint32_t row_limit, std::vector<primary_key>&& keys)
                : merger(row_limit, query::max_partitions)
                , primary_keys(std::move(keys))
                , current_primary_key(primary_keys.begin())
                {}
        base_query_state(base_query_state&&) = default;
        base_query_state(const base_query_state&) = delete;
    };

    base_query_state query_state{cmd->row_limit, std::move(primary_keys)};
    return do_with(std::move(query_state), [this, &proxy, &state, &options, cmd, timeout] (auto&& query_state) {
        auto &merger = query_state.merger;
        auto &keys = query_state.primary_keys;
        auto &key_it = query_state.current_primary_key;
        return repeat([this, &keys, &key_it, &merger, &proxy, &state, &options, cmd, timeout]() {
            // Starting with 1 key, we check if the result was a short read, and if not,
            // we continue exponentially, asking for 2x more key than before
            auto key_it_end = std::min(key_it + std::distance(keys.begin(), key_it) + 1, keys.end());
            auto command = ::make_lw_shared<query::read_command>(*cmd);

            query::result_merger oneshot_merger(cmd->row_limit, query::max_partitions);
            return map_reduce(key_it, key_it_end, [this, &proxy, &state, &options, cmd, timeout] (auto& key) {
                auto command = ::make_lw_shared<query::read_command>(*cmd);
                // for each partition, read just one clustering row (TODO: can
                // get all needed rows of one partition at once.)
                command->slice._row_ranges.clear();
                if (key.clustering) {
                    command->slice._row_ranges.push_back(query::clustering_range::make_singular(key.clustering));
                }
                return proxy.query(_schema, command, {dht::partition_range::make_singular(key.partition)}, options.get_consistency(), {timeout, state.get_trace_state()})
                .then([] (service::storage_proxy::coordinator_query_result qr) {
                    return std::move(qr.query_result);
                });
            }, std::move(oneshot_merger)).then([&key_it, key_it_end = std::move(key_it_end), &keys, &merger] (foreign_ptr<lw_shared_ptr<query::result>> result) {
                bool is_short_read = result->is_short_read();
                merger(std::move(result));
                key_it = key_it_end;
                return stop_iteration(is_short_read || key_it == keys.end());
            });
        }).then([&merger] () {
            return merger.get();
        });
    }).then([this, &proxy, &state, &options, now, cmd, paging_state = std::move(paging_state)] (foreign_ptr<lw_shared_ptr<query::result>> result) mutable {
        return this->process_base_query_results(std::move(result), cmd, proxy, state, options, now, std::move(paging_state));
    });
}

future<shared_ptr<cql_transport::messages::result_message>>
select_statement::execute(service::storage_proxy& proxy,
                          lw_shared_ptr<query::read_command> cmd,
                          dht::partition_range_vector&& partition_ranges,
                          service::query_state& state,
                          const query_options& options,
                          gc_clock::time_point now)
{
    // If this is a query with IN on partition key, ORDER BY clause and LIMIT
    // is specified we need to get "limit" rows from each partition since there
    // is no way to tell which of these rows belong to the query result before
    // doing post-query ordering.
    auto timeout = db::timeout_clock::now() + options.get_timeout_config().*get_timeout_config_selector();
    if (needs_post_query_ordering() && _limit) {
        return do_with(std::forward<dht::partition_range_vector>(partition_ranges), [this, &proxy, &state, &options, cmd, timeout](auto& prs) {
            assert(cmd->partition_limit == query::max_partitions);
            query::result_merger merger(cmd->row_limit * prs.size(), query::max_partitions);
            return map_reduce(prs.begin(), prs.end(), [this, &proxy, &state, &options, cmd, timeout] (auto& pr) {
                dht::partition_range_vector prange { pr };
                auto command = ::make_lw_shared<query::read_command>(*cmd);
                return proxy.query(_schema,
                        command,
                        std::move(prange),
                        options.get_consistency(),
                        {timeout, state.get_trace_state()}).then([] (service::storage_proxy::coordinator_query_result qr) {
                    return std::move(qr.query_result);
                });
            }, std::move(merger));
        }).then([this, &options, now, cmd] (auto result) {
            return this->process_results(std::move(result), cmd, options, now);
        });
    } else {
        return proxy.query(_schema, cmd, std::move(partition_ranges), options.get_consistency(), {timeout, state.get_trace_state()})
            .then([this, &options, now, cmd] (service::storage_proxy::coordinator_query_result qr) {
                return this->process_results(std::move(qr.query_result), cmd, options, now);
            });
    }
}

shared_ptr<cql_transport::messages::result_message>
indexed_table_select_statement::process_base_query_results(
        foreign_ptr<lw_shared_ptr<query::result>> results,
        lw_shared_ptr<query::read_command> cmd,
        service::storage_proxy& proxy,
        service::query_state& state,
        const query_options& options,
        gc_clock::time_point now,
        ::shared_ptr<const service::pager::paging_state> paging_state)
{
    if (paging_state) {
        paging_state = generate_view_paging_state_from_base_query_results(paging_state, results, proxy, state, options);
        _selection->get_result_metadata()->maybe_set_paging_state(std::move(paging_state));
    }
    return process_results(std::move(results), std::move(cmd), options, now);
}

shared_ptr<cql_transport::messages::result_message>
select_statement::process_results(foreign_ptr<lw_shared_ptr<query::result>> results,
                                  lw_shared_ptr<query::read_command> cmd,
                                  const query_options& options,
                                  gc_clock::time_point now)
{
    const bool restrictions_need_filtering = _restrictions->need_filtering();
    const bool fast_path = !needs_post_query_ordering() && _selection->is_trivial() && !restrictions_need_filtering;
    if (fast_path) {
        return make_shared<cql_transport::messages::result_message::rows>(result(
            result_generator(_schema, std::move(results), std::move(cmd), _selection, _stats),
            ::make_shared<metadata>(*_selection->get_result_metadata())
        ));
    }

    cql3::selection::result_set_builder builder(*_selection, now,
            options.get_cql_serialization_format());
    if (restrictions_need_filtering) {
        results->ensure_counts();
        _stats.filtered_rows_read_total += *results->row_count();
        query::result_view::consume(*results, cmd->slice,
                cql3::selection::result_set_builder::visitor(builder, *_schema,
                        *_selection, cql3::selection::result_set_builder::restrictions_filter(_restrictions, options, cmd->row_limit, _schema, cmd->slice.partition_row_limit())));
    } else {
        query::result_view::consume(*results, cmd->slice,
                cql3::selection::result_set_builder::visitor(builder, *_schema,
                        *_selection));
    }
    auto rs = builder.build();

    if (needs_post_query_ordering()) {
        rs->sort(_ordering_comparator);
        if (_is_reversed) {
            rs->reverse();
        }
        rs->trim(cmd->row_limit);
    }
    update_stats_rows_read(rs->size());
    _stats.filtered_rows_matched_total += restrictions_need_filtering ? rs->size() : 0;
    return ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs)));
}

::shared_ptr<restrictions::statement_restrictions> select_statement::get_restrictions() const {
    return _restrictions;
}

primary_key_select_statement::primary_key_select_statement(schema_ptr schema, uint32_t bound_terms,
                                                           ::shared_ptr<parameters> parameters,
                                                           ::shared_ptr<selection::selection> selection,
                                                           ::shared_ptr<restrictions::statement_restrictions> restrictions,
                                                           bool is_reversed,
                                                           ordering_comparator_type ordering_comparator,
                                                           ::shared_ptr<term> limit,
                                                           ::shared_ptr<term> per_partition_limit,
                                                           cql_stats &stats)
    : select_statement{schema, bound_terms, parameters, selection, restrictions, is_reversed, ordering_comparator, limit, per_partition_limit, stats}
{}

::shared_ptr<cql3::statements::select_statement>
indexed_table_select_statement::prepare(database& db,
                                        schema_ptr schema,
                                        uint32_t bound_terms,
                                        ::shared_ptr<parameters> parameters,
                                        ::shared_ptr<selection::selection> selection,
                                        ::shared_ptr<restrictions::statement_restrictions> restrictions,
                                        bool is_reversed,
                                        ordering_comparator_type ordering_comparator,
                                        ::shared_ptr<term> limit,
                                         ::shared_ptr<term> per_partition_limit,
                                         cql_stats &stats)
{
    auto& sim = db.find_column_family(schema).get_index_manager();
    auto [index_opt, used_index_restrictions] = restrictions->find_idx(sim);
    if (!index_opt) {
        throw std::runtime_error("No index found.");
    }

    const auto& im = index_opt->metadata();
    sstring index_table_name = im.name() + "_index";
    schema_ptr view_schema = db.find_schema(schema->ks_name(), index_table_name);

    return ::make_shared<cql3::statements::indexed_table_select_statement>(
            schema,
            bound_terms,
            parameters,
            std::move(selection),
            std::move(restrictions),
            is_reversed,
            std::move(ordering_comparator),
            limit,
            per_partition_limit,
            stats,
            *index_opt,
            std::move(used_index_restrictions),
            view_schema);

}

indexed_table_select_statement::indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms,
                                                           ::shared_ptr<parameters> parameters,
                                                           ::shared_ptr<selection::selection> selection,
                                                           ::shared_ptr<restrictions::statement_restrictions> restrictions,
                                                           bool is_reversed,
                                                           ordering_comparator_type ordering_comparator,
                                                           ::shared_ptr<term> limit,
                                                           ::shared_ptr<term> per_partition_limit,
                                                           cql_stats &stats,
                                                           const secondary_index::index& index,
                                                           ::shared_ptr<restrictions::restrictions> used_index_restrictions,
                                                           schema_ptr view_schema)
    : select_statement{schema, bound_terms, parameters, selection, restrictions, is_reversed, ordering_comparator, limit, per_partition_limit, stats}
    , _index{index}
    , _used_index_restrictions(used_index_restrictions)
    , _view_schema(view_schema)
{
    if (_index.metadata().local()) {
        _get_partition_ranges_for_posting_list = [this] (const query_options& options) { return get_partition_ranges_for_local_index_posting_list(options); };
        _get_partition_slice_for_posting_list = [this] (const query_options& options) { return get_partition_slice_for_local_index_posting_list(options); };
    } else {
        _get_partition_ranges_for_posting_list = [this] (const query_options& options) { return get_partition_ranges_for_global_index_posting_list(options); };
        _get_partition_slice_for_posting_list = [this] (const query_options& options) { return get_partition_slice_for_global_index_posting_list(options); };
    }
}

template<typename KeyType>
GCC6_CONCEPT(
    requires (std::is_same_v<KeyType, partition_key> || std::is_same_v<KeyType, clustering_key_prefix>)
)
static void append_base_key_to_index_ck(std::vector<bytes_view>& exploded_index_ck, const KeyType& base_key, const column_definition& index_cdef) {
    auto key_view = base_key.view();
    auto begin = key_view.begin();
    if ((std::is_same_v<KeyType, partition_key> && index_cdef.is_partition_key())
            || (std::is_same_v<KeyType, clustering_key_prefix> && index_cdef.is_clustering_key())) {
        auto key_position = std::next(begin, index_cdef.id);
        std::move(begin, key_position, std::back_inserter(exploded_index_ck));
        begin = std::next(key_position);
    }
    std::move(begin, key_view.end(), std::back_inserter(exploded_index_ck));
}

::shared_ptr<const service::pager::paging_state> indexed_table_select_statement::generate_view_paging_state_from_base_query_results(::shared_ptr<const service::pager::paging_state> paging_state,
        const foreign_ptr<lw_shared_ptr<query::result>>& results, service::storage_proxy& proxy, service::query_state& state, const query_options& options) const {
    const column_definition* cdef = _schema->get_column_definition(to_bytes(_index.target_column()));
    if (!cdef) {
        throw exceptions::invalid_request_exception("Indexed column not found in schema");
    }

    auto result_view = query::result_view(*results);
    if (!results->row_count() || *results->row_count() == 0) {
        return std::move(paging_state);
    }
    auto [last_base_pk, last_base_ck] = result_view.get_last_partition_and_clustering_key();

    bytes_opt indexed_column_value = _used_index_restrictions->value_for(*cdef, options);

    auto index_pk = [&]() {
        if (_index.metadata().local()) {
            return last_base_pk;
        } else {
            return partition_key::from_single_value(*_view_schema, *indexed_column_value);
        }
    }();

    std::vector<bytes_view> exploded_index_ck;
    exploded_index_ck.reserve(_view_schema->clustering_key_size());

    bytes token_bytes;
    if (_index.metadata().local()) {
        exploded_index_ck.push_back(bytes_view(*indexed_column_value));
    } else {
        dht::i_partitioner& partitioner = dht::global_partitioner();
        token_bytes = partitioner.token_to_bytes(partitioner.get_token(*_schema, last_base_pk));
        exploded_index_ck.push_back(bytes_view(token_bytes));
        append_base_key_to_index_ck<partition_key>(exploded_index_ck, last_base_pk, *cdef);
    }
    if (last_base_ck) {
        append_base_key_to_index_ck<clustering_key>(exploded_index_ck, *last_base_ck, *cdef);
    }

    auto index_ck = clustering_key::from_range(std::move(exploded_index_ck));
    if (partition_key::tri_compare(*_view_schema)(paging_state->get_partition_key(), index_pk) == 0
            && (!paging_state->get_clustering_key() || clustering_key::prefix_equal_tri_compare(*_view_schema)(*paging_state->get_clustering_key(), index_ck) == 0)) {
        return std::move(paging_state);
    }

    auto paging_state_copy = ::make_shared<service::pager::paging_state>(service::pager::paging_state(*paging_state));
    paging_state_copy->set_partition_key(std::move(index_pk));
    paging_state_copy->set_clustering_key(std::move(index_ck));
    return std::move(paging_state_copy);
}

future<shared_ptr<cql_transport::messages::result_message>>
indexed_table_select_statement::do_execute(service::storage_proxy& proxy,
                             service::query_state& state,
                             const query_options& options)
{
    tracing::add_table_name(state.get_trace_state(), _view_schema->ks_name(), _view_schema->cf_name());
    tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());

    auto cl = options.get_consistency();

    validate_for_read(cl);

    auto now = gc_clock::now();

    ++_stats.reads;
    ++_stats.secondary_index_reads;

    assert(_restrictions->uses_secondary_indexing());

    _stats.unpaged_select_queries += options.get_page_size() <= 0;

    // Secondary index search has two steps: 1. use the index table to find a
    // list of primary keys matching the query. 2. read the rows matching
    // these primary keys from the base table and return the selected columns.
    // In "whole_partitions" case, we can do the above in whole partition
    // granularity. "partition_slices" is similar, but we fetch the same
    // clustering prefix (make_partition_slice()) from a list of partitions.
    // In other cases we need to list, and retrieve, individual rows and
    // not entire partitions. See issue #3405 for more details.
    bool whole_partitions = false;
    bool partition_slices = false;
    if (_schema->clustering_key_size() == 0) {
        // Obviously, if there are no clustering columns, then we can work at
        // the granularity of whole partitions.
        whole_partitions = true;
    } else {
        if (_index.depends_on(*(_schema->clustering_key_columns().begin()))) {
            // Searching on the *first* clustering column means in each of
            // matching partition, we can take the same contiguous clustering
            // slice (clustering prefix).
            partition_slices = true;
        } else {
            // Search on any partition column means that either all rows
            // match or all don't, so we can work with whole partitions.
            for (auto& cdef : _schema->partition_key_columns()) {
                if (_index.depends_on(cdef)) {
                    whole_partitions = true;
                    break;
                }
            }
        }
    }

    if (whole_partitions || partition_slices) {
        // In this case, can use our normal query machinery, which retrieves
        // entire partitions or the same slice for many partitions.
        return find_index_partition_ranges(proxy, state, options).then([now, &state, &options, &proxy, this] (dht::partition_range_vector partition_ranges, ::shared_ptr<const service::pager::paging_state> paging_state) {
            return this->execute_base_query(proxy, std::move(partition_ranges), state, options, now, std::move(paging_state));
        });
    } else {
        // In this case, we need to retrieve a list of rows (not entire
        // partitions) and then retrieve those specific rows.
        return find_index_clustering_rows(proxy, state, options).then([now, &state, &options, &proxy, this] (std::vector<primary_key> primary_keys, ::shared_ptr<const service::pager::paging_state> paging_state) {
            return this->execute_base_query(proxy, std::move(primary_keys), state, options, now, std::move(paging_state));
        });
    }
}

dht::partition_range_vector indexed_table_select_statement::get_partition_ranges_for_local_index_posting_list(const query_options& options) const {
    return _restrictions->get_partition_key_restrictions()->bounds_ranges(options);
}

dht::partition_range_vector indexed_table_select_statement::get_partition_ranges_for_global_index_posting_list(const query_options& options) const {
    dht::partition_range_vector partition_ranges;

    const column_definition* cdef = _schema->get_column_definition(to_bytes(_index.target_column()));
    if (!cdef) {
        throw exceptions::invalid_request_exception("Indexed column not found in schema");
    }

    bytes_opt value = _used_index_restrictions->value_for(*cdef, options);
    if (value) {
        auto pk = partition_key::from_single_value(*_view_schema, *value);
        auto dk = dht::global_partitioner().decorate_key(*_view_schema, pk);
        auto range = dht::partition_range::make_singular(dk);
        partition_ranges.emplace_back(range);
    }

    return partition_ranges;
}

query::partition_slice indexed_table_select_statement::get_partition_slice_for_global_index_posting_list(const query_options& options) const {
    partition_slice_builder partition_slice_builder{*_view_schema};

    if (!_restrictions->has_partition_key_unrestricted_components()) {
        auto single_pk_restrictions = dynamic_pointer_cast<restrictions::single_column_partition_key_restrictions>(_restrictions->get_partition_key_restrictions());
        // Only EQ restrictions on base partition key can be used in an index view query
        if (single_pk_restrictions && single_pk_restrictions->is_all_eq()) {
            auto clustering_restrictions = ::make_shared<restrictions::single_column_clustering_key_restrictions>(_view_schema, *single_pk_restrictions);
            // Computed token column needs to be added to index view restrictions
            const column_definition& token_cdef = *_view_schema->clustering_key_columns().begin();
            auto base_pk = partition_key::from_optional_exploded(*_schema, _restrictions->get_partition_key_restrictions()->values(options));
            bytes token_value = dht::global_partitioner().token_to_bytes(dht::global_partitioner().get_token(*_schema, base_pk));
            auto token_restriction = ::make_shared<restrictions::single_column_restriction::EQ>(token_cdef, ::make_shared<cql3::constants::value>(cql3::raw_value::make_value(token_value)));
            clustering_restrictions->merge_with(token_restriction);

            if (_restrictions->get_clustering_columns_restrictions()->prefix_size() > 0) {
                auto single_ck_restrictions = dynamic_pointer_cast<restrictions::single_column_clustering_key_restrictions>(_restrictions->get_clustering_columns_restrictions());
                if (single_ck_restrictions) {
                    auto prefix_restrictions = single_ck_restrictions->get_longest_prefix_restrictions();
                    auto clustering_restrictions_from_base = ::make_shared<restrictions::single_column_clustering_key_restrictions>(_view_schema, *prefix_restrictions);
                    for (auto restriction_it : clustering_restrictions_from_base->restrictions()) {
                        clustering_restrictions->merge_with(restriction_it.second);
                    }
                }
            }

            partition_slice_builder.with_ranges(clustering_restrictions->bounds_ranges(options));
        }
    }

    return partition_slice_builder.build();
}

query::partition_slice indexed_table_select_statement::get_partition_slice_for_local_index_posting_list(const query_options& options) const {
    partition_slice_builder partition_slice_builder{*_view_schema};

    ::shared_ptr<restrictions::single_column_clustering_key_restrictions> clustering_restrictions;
    // For local indexes, the first clustering key is the indexed column itself, followed by base clustering key
    clustering_restrictions = ::make_shared<restrictions::single_column_clustering_key_restrictions>(_view_schema, true);
    const column_definition* cdef = _schema->get_column_definition(to_bytes(_index.target_column()));

    bytes_opt value = _used_index_restrictions->value_for(*cdef, options);
    if (value) {
        const column_definition* view_cdef = _view_schema->get_column_definition(to_bytes(_index.target_column()));
        auto index_eq_restriction = ::make_shared<restrictions::single_column_restriction::EQ>(*view_cdef, ::make_shared<cql3::constants::value>(cql3::raw_value::make_value(*value)));
        clustering_restrictions->merge_with(index_eq_restriction);
    }

    if (_restrictions->get_clustering_columns_restrictions()->prefix_size() > 0) {
        auto single_ck_restrictions = dynamic_pointer_cast<restrictions::single_column_clustering_key_restrictions>(_restrictions->get_clustering_columns_restrictions());
        if (single_ck_restrictions) {
            auto prefix_restrictions = single_ck_restrictions->get_longest_prefix_restrictions();
            auto clustering_restrictions_from_base = ::make_shared<restrictions::single_column_clustering_key_restrictions>(_view_schema, *prefix_restrictions);
            for (auto restriction_it : clustering_restrictions_from_base->restrictions()) {
                clustering_restrictions->merge_with(restriction_it.second);
            }
        }
    }

    partition_slice_builder.with_ranges(clustering_restrictions->bounds_ranges(options));

    return partition_slice_builder.build();
}

// Utility function for reading from the index view (get_index_view()))
// the posting-list for a particular value of the indexed column.
// Remember a secondary index can only be created on a single column.
future<::shared_ptr<cql_transport::messages::result_message::rows>>
indexed_table_select_statement::read_posting_list(service::storage_proxy& proxy,
                  const query_options& options,
                  int32_t limit,
                  service::query_state& state,
                  gc_clock::time_point now,
                  db::timeout_clock::time_point timeout,
                  bool include_base_clustering_key)
{
    dht::partition_range_vector partition_ranges = _get_partition_ranges_for_posting_list(options);
    auto partition_slice = _get_partition_slice_for_posting_list(options);

    auto cmd = ::make_lw_shared<query::read_command>(
            _view_schema->id(),
            _view_schema->version(),
            partition_slice,
            limit,
            now,
            tracing::make_trace_info(state.get_trace_state()),
            query::max_partitions,
            utils::UUID(),
            options.get_timestamp(state));

    std::vector<const column_definition*> columns;
    for (const column_definition& cdef : _schema->partition_key_columns()) {
        columns.emplace_back(_view_schema->get_column_definition(cdef.name()));
    }
    if (include_base_clustering_key) {
        for (const column_definition& cdef : _schema->clustering_key_columns()) {
            columns.emplace_back(_view_schema->get_column_definition(cdef.name()));
        }
    }
    auto selection = selection::selection::for_columns(_view_schema, columns);

    int32_t page_size = options.get_page_size();
    if (page_size <= 0 || !service::pager::query_pagers::may_need_paging(*_view_schema, page_size, *cmd, partition_ranges)) {
        return proxy.query(_view_schema, cmd, std::move(partition_ranges), options.get_consistency(), {timeout, state.get_trace_state()})
        .then([this, now, &options, selection = std::move(selection), partition_slice = std::move(partition_slice)] (service::storage_proxy::coordinator_query_result qr) {
            cql3::selection::result_set_builder builder(*selection, now, options.get_cql_serialization_format());
            query::result_view::consume(*qr.query_result,
                                        std::move(partition_slice),
                                        cql3::selection::result_set_builder::visitor(builder, *_view_schema, *selection));
            return ::make_shared<cql_transport::messages::result_message::rows>(std::move(result(builder.build())));
        });
    }

    auto p = service::pager::query_pagers::pager(_view_schema, selection,
            state, options, cmd, std::move(partition_ranges), _stats, nullptr);
    return p->fetch_page(options.get_page_size(), now, timeout).then([p, &options, limit, now] (std::unique_ptr<cql3::result_set> rs) {
        rs->get_metadata().set_paging_state(p->state());
        return ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs)));
    });
}

// Note: the partitions keys returned by this function are sorted
// in token order. See issue #3423.
future<dht::partition_range_vector, ::shared_ptr<const service::pager::paging_state>>
indexed_table_select_statement::find_index_partition_ranges(service::storage_proxy& proxy,
                                             service::query_state& state,
                                             const query_options& options)
{
    auto now = gc_clock::now();
    auto timeout = db::timeout_clock::now() + options.get_timeout_config().*get_timeout_config_selector();
    return read_posting_list(proxy, options, get_limit(options), state, now, timeout, false).then(
            [this, now, &options] (::shared_ptr<cql_transport::messages::result_message::rows> rows) {
        auto rs = cql3::untyped_result_set(rows);
        dht::partition_range_vector partition_ranges;
        partition_ranges.reserve(rs.size());
        // We are reading the list of primary keys as rows of a single
        // partition (in the index view), so they are sorted in
        // lexicographical order (N.B. this is NOT token order!). We need
        // to avoid outputting the same partition key twice, but luckily in
        // the sorted order, these will be adjacent.
        std::optional<dht::decorated_key> last_dk;
        for (size_t i = 0; i < rs.size(); i++) {
            const auto& row = rs.at(i);
            std::vector<bytes> pk_columns;
            for (const auto& column : row.get_columns()) {
                pk_columns.push_back(row.get_blob(column->name->to_string()));
            }
            auto pk = partition_key::from_exploded(*_schema, pk_columns);
            auto dk = dht::global_partitioner().decorate_key(*_schema, pk);
            if (last_dk && last_dk->equal(*_schema, dk)) {
                // Another row of the same partition, no need to output the
                // same partition key again.
                continue;
            }
            last_dk = dk;
            auto range = dht::partition_range::make_singular(dk);
            partition_ranges.emplace_back(range);
        }
        auto paging_state = rows->rs().get_metadata().paging_state();
        return make_ready_future<dht::partition_range_vector, ::shared_ptr<const service::pager::paging_state>>(std::move(partition_ranges), std::move(paging_state));
    });
}

// Note: the partitions keys returned by this function are sorted
// in token order. See issue #3423.
future<std::vector<indexed_table_select_statement::primary_key>, ::shared_ptr<const service::pager::paging_state>>
indexed_table_select_statement::find_index_clustering_rows(service::storage_proxy& proxy, service::query_state& state, const query_options& options)
{
    auto now = gc_clock::now();
    auto timeout = db::timeout_clock::now() + options.get_timeout_config().*get_timeout_config_selector();
    return read_posting_list(proxy, options, get_limit(options), state, now, timeout, true).then(
            [this, now, &options] (::shared_ptr<cql_transport::messages::result_message::rows> rows) {

        auto rs = cql3::untyped_result_set(rows);
        std::vector<primary_key> primary_keys;
        primary_keys.reserve(rs.size());
        for (size_t i = 0; i < rs.size(); i++) {
            const auto& row = rs.at(i);
            auto pk_columns = _schema->partition_key_columns() | boost::adaptors::transformed([&] (auto& cdef) {
                return row.get_blob(cdef.name_as_text());
            });
            auto pk = partition_key::from_range(pk_columns);
            auto dk = dht::global_partitioner().decorate_key(*_schema, pk);
            auto ck_columns = _schema->clustering_key_columns() | boost::adaptors::transformed([&] (auto& cdef) {
                return row.get_blob(cdef.name_as_text());
            });
            auto ck = clustering_key::from_range(ck_columns);
            primary_keys.emplace_back(primary_key{std::move(dk), std::move(ck)});
        }
        auto paging_state = rows->rs().get_metadata().paging_state();
        return make_ready_future<std::vector<indexed_table_select_statement::primary_key>, ::shared_ptr<const service::pager::paging_state>>(std::move(primary_keys), std::move(paging_state));
    });
}

namespace raw {

select_statement::select_statement(::shared_ptr<cf_name> cf_name,
                                   ::shared_ptr<parameters> parameters,
                                   std::vector<::shared_ptr<selection::raw_selector>> select_clause,
                                   std::vector<::shared_ptr<relation>> where_clause,
                                   ::shared_ptr<term::raw> limit,
                                   ::shared_ptr<term::raw> per_partition_limit)
    : cf_statement(std::move(cf_name))
    , _parameters(std::move(parameters))
    , _select_clause(std::move(select_clause))
    , _where_clause(std::move(where_clause))
    , _limit(std::move(limit))
    , _per_partition_limit(std::move(per_partition_limit))
{ }

void select_statement::maybe_jsonize_select_clause(database& db, schema_ptr schema) {
    // Fill wildcard clause with explicit column identifiers for as_json function
    if (_parameters->is_json()) {
        if (_select_clause.empty()) {
            _select_clause.reserve(schema->all_columns().size());
            for (const column_definition& column_def : schema->all_columns_in_select_order()) {
                _select_clause.push_back(make_shared<selection::raw_selector>(
                        make_shared<column_identifier::raw>(column_def.name_as_text(), true), nullptr));
            }
        }

        // Prepare selector names + types for as_json function
        std::vector<sstring> selector_names;
        std::vector<data_type> selector_types;
        std::vector<const column_definition*> defs;
        selector_names.reserve(_select_clause.size());
        auto selectables = selection::raw_selector::to_selectables(_select_clause, schema);
        selection::selector_factories factories(selection::raw_selector::to_selectables(_select_clause, schema), db, schema, defs);
        auto selectors = factories.new_instances();
        for (size_t i = 0; i < selectors.size(); ++i) {
            selector_names.push_back(selectables[i]->to_string());
            selector_types.push_back(selectors[i]->get_type());
        }

        // Prepare args for as_json_function
        std::vector<::shared_ptr<selection::selectable::raw>> raw_selectables;
        raw_selectables.reserve(_select_clause.size());
        for (const auto& raw_selector : _select_clause) {
            raw_selectables.push_back(raw_selector->selectable_);
        }
        auto as_json = ::make_shared<functions::as_json_function>(std::move(selector_names), std::move(selector_types));
        auto as_json_selector = ::make_shared<selection::raw_selector>(
                ::make_shared<selection::selectable::with_anonymous_function::raw>(as_json, std::move(raw_selectables)), nullptr);
        _select_clause.clear();
        _select_clause.push_back(as_json_selector);
    }
}

std::unique_ptr<prepared_statement> select_statement::prepare(database& db, cql_stats& stats, bool for_view) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
    auto bound_names = get_bound_variables();

    maybe_jsonize_select_clause(db, schema);

    auto selection = _select_clause.empty()
                     ? selection::selection::wildcard(schema)
                     : selection::selection::from_selectors(db, schema, _select_clause);

    auto restrictions = prepare_restrictions(db, schema, bound_names, selection, for_view, _parameters->allow_filtering());

    if (_parameters->is_distinct()) {
        validate_distinct_selection(schema, selection, restrictions);
    }

    select_statement::ordering_comparator_type ordering_comparator;
    bool is_reversed_ = false;

    if (!_parameters->orderings().empty()) {
        assert(!for_view);
        verify_ordering_is_allowed(restrictions);
        ordering_comparator = get_ordering_comparator(schema, selection, restrictions);
        is_reversed_ = is_reversed(schema);
    }

    check_needs_filtering(restrictions);
    ensure_filtering_columns_retrieval(db, selection, restrictions);

    ::shared_ptr<cql3::statements::select_statement> stmt;
    if (restrictions->uses_secondary_indexing()) {
        stmt = indexed_table_select_statement::prepare(
                db,
                schema,
                bound_names->size(),
                _parameters,
                std::move(selection),
                std::move(restrictions),
                is_reversed_,
                std::move(ordering_comparator),
                prepare_limit(db, bound_names, _limit),
                prepare_limit(db, bound_names, _per_partition_limit),
                stats);
    } else {
        stmt = ::make_shared<cql3::statements::primary_key_select_statement>(
                schema,
                bound_names->size(),
                _parameters,
                std::move(selection),
                std::move(restrictions),
                is_reversed_,
                std::move(ordering_comparator),
                prepare_limit(db, bound_names, _limit),
                prepare_limit(db, bound_names, _per_partition_limit),
                stats);
    }

    auto partition_key_bind_indices = bound_names->get_partition_key_bind_indexes(schema);

    return std::make_unique<prepared>(std::move(stmt), std::move(*bound_names), std::move(partition_key_bind_indices));
}

::shared_ptr<restrictions::statement_restrictions>
select_statement::prepare_restrictions(database& db,
                                       schema_ptr schema,
                                       ::shared_ptr<variable_specifications> bound_names,
                                       ::shared_ptr<selection::selection> selection,
                                       bool for_view,
                                       bool allow_filtering)
{
    try {
        return ::make_shared<restrictions::statement_restrictions>(db, schema, statement_type::SELECT, std::move(_where_clause), bound_names,
            selection->contains_only_static_columns(), selection->contains_a_collection(), for_view, allow_filtering);
    } catch (const exceptions::unrecognized_entity_exception& e) {
        if (contains_alias(e.entity)) {
            throw exceptions::invalid_request_exception(format("Aliases aren't allowed in the where clause ('{}')", e.relation->to_string()));
        }
        throw;
    }
}

/** Returns a ::shared_ptr<term> for the limit or null if no limit is set */
::shared_ptr<term>
select_statement::prepare_limit(database& db, ::shared_ptr<variable_specifications> bound_names, ::shared_ptr<term::raw> limit)
{
    if (!limit) {
        return {};
    }

    auto prep_limit = limit->prepare(db, keyspace(), limit_receiver());
    prep_limit->collect_marker_specification(bound_names);
    return prep_limit;
}

void select_statement::verify_ordering_is_allowed(::shared_ptr<restrictions::statement_restrictions> restrictions)
{
    if (restrictions->uses_secondary_indexing()) {
        throw exceptions::invalid_request_exception("ORDER BY with 2ndary indexes is not supported.");
    }
    if (restrictions->is_key_range()) {
        throw exceptions::invalid_request_exception("ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
    }
}

void select_statement::validate_distinct_selection(schema_ptr schema,
                                                   ::shared_ptr<selection::selection> selection,
                                                   ::shared_ptr<restrictions::statement_restrictions> restrictions)
{
    if (restrictions->has_non_primary_key_restriction() || restrictions->has_clustering_columns_restriction()) {
        throw exceptions::invalid_request_exception(
            "SELECT DISTINCT with WHERE clause only supports restriction by partition key.");
    }
    for (auto&& def : selection->get_columns()) {
        if (!def->is_partition_key() && !def->is_static()) {
            throw exceptions::invalid_request_exception(format("SELECT DISTINCT queries must only request partition key columns and/or static columns (not {})",
                def->name_as_text()));
        }
    }

    // If it's a key range, we require that all partition key columns are selected so we don't have to bother
    // with post-query grouping.
    if (!restrictions->is_key_range()) {
        return;
    }

    for (auto&& def : schema->partition_key_columns()) {
        if (!selection->has_column(def)) {
            throw exceptions::invalid_request_exception(format("SELECT DISTINCT queries must request all the partition key columns (missing {})", def.name_as_text()));
        }
    }
}

void select_statement::handle_unrecognized_ordering_column(::shared_ptr<column_identifier> column)
{
    if (contains_alias(column)) {
        throw exceptions::invalid_request_exception(format("Aliases are not allowed in order by clause ('{}')", *column));
    }
    throw exceptions::invalid_request_exception(format("Order by on unknown column {}", *column));
}

select_statement::ordering_comparator_type
select_statement::get_ordering_comparator(schema_ptr schema,
                                          ::shared_ptr<selection::selection> selection,
                                          ::shared_ptr<restrictions::statement_restrictions> restrictions)
{
    if (!restrictions->key_is_in_relation()) {
        return {};
    }

    std::vector<std::pair<uint32_t, data_type>> sorters;
    sorters.reserve(_parameters->orderings().size());

    // If we order post-query (see orderResults), the sorted column needs to be in the ResultSet for sorting,
    // even if we don't
    // ultimately ship them to the client (CASSANDRA-4911).
    for (auto&& e : _parameters->orderings()) {
        auto&& raw = e.first;
        ::shared_ptr<column_identifier> column = raw->prepare_column_identifier(schema);
        const column_definition* def = schema->get_column_definition(column->name());
        if (!def) {
            handle_unrecognized_ordering_column(column);
        }
        auto index = selection->index_of(*def);
        if (index < 0) {
            index = selection->add_column_for_post_processing(*def);
        }

        sorters.emplace_back(index, def->type);
    }

    return [sorters = std::move(sorters)] (const result_row_type& r1, const result_row_type& r2) mutable {
        for (auto&& e : sorters) {
            auto& c1 = r1[e.first];
            auto& c2 = r2[e.first];
            auto type = e.second;

            if (bool(c1) != bool(c2)) {
                return bool(c2);
            }
            if (c1) {
                int result = type->compare(*c1, *c2);
                if (result != 0) {
                    return result < 0;
                }
            }
        }
        return false;
    };
}

bool select_statement::is_reversed(schema_ptr schema) {
    assert(_parameters->orderings().size() > 0);
    parameters::orderings_type::size_type i = 0;
    bool is_reversed_ = false;
    bool relation_order_unsupported = false;

    for (auto&& e : _parameters->orderings()) {
        ::shared_ptr<column_identifier> column = e.first->prepare_column_identifier(schema);
        bool reversed = e.second;

        auto def = schema->get_column_definition(column->name());
        if (!def) {
            handle_unrecognized_ordering_column(column);
        }

        if (!def->is_clustering_key()) {
            throw exceptions::invalid_request_exception(format("Order by is currently only supported on the clustered columns of the PRIMARY KEY, got {}", *column));
        }

        if (i != def->component_index()) {
            throw exceptions::invalid_request_exception(
                "Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY");
        }

        bool current_reverse_status = (reversed != def->type->is_reversed());

        if (i == 0) {
            is_reversed_ = current_reverse_status;
        }

        if (is_reversed_ != current_reverse_status) {
            relation_order_unsupported = true;
        }
        ++i;
    }

    if (relation_order_unsupported) {
        throw exceptions::invalid_request_exception("Unsupported order by relation");
    }

    return is_reversed_;
}

/** If ALLOW FILTERING was not specified, this verifies that it is not needed */
void select_statement::check_needs_filtering(::shared_ptr<restrictions::statement_restrictions> restrictions)
{
    // non-key-range non-indexed queries cannot involve filtering underneath
    if (!_parameters->allow_filtering() && (restrictions->is_key_range() || restrictions->uses_secondary_indexing())) {
        // We will potentially filter data if either:
        //  - Have more than one IndexExpression
        //  - Have no index expression and the column filter is not the identity
        if (restrictions->need_filtering()) {
            throw exceptions::invalid_request_exception(
                "Cannot execute this query as it might involve data filtering and "
                    "thus may have unpredictable performance. If you want to execute "
                    "this query despite the performance unpredictability, use ALLOW FILTERING");
        }
    }
}

/**
 * Adds columns that are needed for the purpose of filtering to the selection.
 * The columns that are added to the selection are columns that
 * are needed for filtering on the coordinator but are not part of the selection.
 * The columns are added with a meta-data indicating they are not to be returned
 * to the user.
 */
void select_statement::ensure_filtering_columns_retrieval(database& db,
                                        ::shared_ptr<selection::selection> selection,
                                        ::shared_ptr<restrictions::statement_restrictions> restrictions) {
    for (auto&& cdef : restrictions->get_column_defs_for_filtering(db)) {
        if (!selection->has_column(*cdef)) {
            selection->add_column_for_post_processing(*cdef);
        }
    }
}

bool select_statement::contains_alias(::shared_ptr<column_identifier> name) {
    return std::any_of(_select_clause.begin(), _select_clause.end(), [name] (auto raw) {
        return raw->alias && *name == *raw->alias;
    });
}

::shared_ptr<column_specification> select_statement::limit_receiver(bool per_partition) {
    sstring name = per_partition ? "[per_partition_limit]" : "[limit]";
    return ::make_shared<column_specification>(keyspace(), column_family(), ::make_shared<column_identifier>(name, true),
        int32_type);
}

}

}

namespace util {

shared_ptr<cql3::statements::raw::select_statement> build_select_statement(
            const sstring_view& cf_name,
            const sstring_view& where_clause,
            bool select_all_columns,
            const std::vector<column_definition>& selected_columns) {
    std::ostringstream out;
    out << "SELECT ";
    if (select_all_columns) {
        out << "*";
    } else {
        // If the column name is not entirely lowercase (or digits or _),
        // when output to CQL it must be quoted to preserve case as well
        // as non alphanumeric characters.
        auto cols = boost::copy_range<std::vector<sstring>>(selected_columns
                | boost::adaptors::transformed(std::mem_fn(&column_definition::name_as_cql_string)));
        out << join(", ", cols);
    }
    // Note that cf_name may need to be quoted, just like column names above.
    out << " FROM " << util::maybe_quote(sstring(cf_name)) << " WHERE " << where_clause << " ALLOW FILTERING";
    return do_with_parser(out.str(), std::mem_fn(&cql3_parser::CqlParser::selectStatement));
}

}

}

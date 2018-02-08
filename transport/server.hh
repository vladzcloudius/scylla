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

#pragma once

#include "auth/service.hh"
#include "core/reactor.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "service/migration_listener.hh"
#include "service/storage_proxy.hh"
#include "cql3/query_processor.hh"
#include "cql3/values.hh"
#include "auth/authenticator.hh"
#include "core/distributed.hh"
#include <seastar/core/semaphore.hh>
#include <memory>
#include <boost/intrusive/list.hpp>
#include <seastar/net/tls.hh>
#include <seastar/core/metrics_registration.hh>

namespace scollectd {

class registrations;

}

class database;

namespace cql_transport {

enum class cql_compression {
    none,
    lz4,
    snappy,
};

enum cql_frame_flags {
    compression = 0x01,
    tracing     = 0x02,
};

struct [[gnu::packed]] cql_binary_frame_v1 {
    uint8_t  version;
    uint8_t  flags;
    uint8_t  stream;
    uint8_t  opcode;
    net::packed<uint32_t> length;

    template <typename Adjuster>
    void adjust_endianness(Adjuster a) {
        return a(length);
    }
};

struct [[gnu::packed]] cql_binary_frame_v3 {
    uint8_t  version;
    uint8_t  flags;
    net::packed<uint16_t> stream;
    uint8_t  opcode;
    net::packed<uint32_t> length;

    template <typename Adjuster>
    void adjust_endianness(Adjuster a) {
        return a(stream, length);
    }
};

enum class cql_load_balance {
    none,
    round_robin,
};

cql_load_balance parse_load_balance(sstring value);

struct cql_query_state {
    service::query_state query_state;
    std::unique_ptr<cql3::query_options> options;

    cql_query_state(service::client_state& client_state)
        : query_state(client_state)
    { }
};

class cql_server {
private:
    class event_notifier;

    class load_balancer {
    private:
        ///
        /// Coordinator load balancing thresholds. Note that our "load" counters are inverted thus thresholds below and the
        /// corresponding logic are inverted too.
        ///

        using load_balancer_clock = lowres_clock;
        static const load_balancer_clock::duration load_balancer_period;
        static constexpr unsigned compute_shard_id = 1;

        struct balancing_state {
            // Load level above which the local shard will start offloading requests to remote nodes
            static constexpr double start_offload_threshold = 0.25; // corresponds to the load of 75%
            // Load when we want to start removing senders from the shard's loaders.
            static constexpr double start_backoff_threshold = 0.05; // corresponds to the load of 95%

            std::vector<double> loads;
            std::vector<std::unordered_set<unsigned>> loaders;
            std::vector<std::unordered_set<unsigned>> receivers;

            balancing_state()
                : loads(smp::count, 1.0)
                , loaders(smp::count)
                , receivers(smp::count)
            {}

            void build_pools();
            bool can_accept_more_load(unsigned idx) {
                return loads[idx] > start_offload_threshold && receivers[idx].empty();
            }
            std::vector<unsigned> get_pool(unsigned idx) const {
                auto& receivers_idx = receivers[idx];
                std::vector<unsigned> shards_pool(receivers_idx.size() + 1);
                shards_pool.clear();
                shards_pool.push_back(idx);
                std::copy(receivers_idx.begin(), receivers_idx.end(), std::back_inserter(shards_pool));
                return shards_pool;
            }
        };

        class shard_metric {
        private:
            // The smallest positive double value. We don't want the ewma value to become zero.
            static constexpr double min_ewma_latency_val = std::numeric_limits<double>::min();
            // Exponential decay parameter
            static constexpr double alfa = 0.25;
            // Increase the metric by 1% for each 100 outstanding requests
            static constexpr double queue_length_factor_base = 1.0001;

        private:
            // Contains the EWMA latency for the corresponding shard in the system from the point of view of the local shard.
            double _ewma_latency = min_ewma_latency_val;
            // The metric that depends on the current queue length: queue_length_factor_base^N, when N is a current queue length.
            size_t _queue_len_metric = 1;
            // This is a synthetic metric that is used for selecting the shard to process the next request.
            // The shard with the lowers metric valus is going to be selected.
            //
            // The metric's formula is: "latency" * queue_length_factor_base^num_outstanding_requests.
            size_t _value = _queue_len_metric;

        public:
            size_t value() const noexcept {
                return _value;
            }

            void decay_ewma() noexcept {
                //_ewma_latency = std::max((1 - alfa) * _ewma_latency, min_ewma_latency_val);
            }

            void update_ewma(double weighted_latency) {
                //_ewma_latency += alfa * weighted_latency;
            }

            void inc_queue_len() noexcept {
                ++_queue_len_metric;
            }

            void dec_queue_len() noexcept {
                --_queue_len_metric;
            }

            void recalculate_value() noexcept {
                _value = /*_ewma_latency * */_queue_len_metric;
            }
        };

        class shards_metric_comp {
        private:
            std::reference_wrapper<const std::vector<shard_metric>> _metric_ref;

        public:
            shards_metric_comp(const std::vector<shard_metric>& metric) : _metric_ref(metric) {}
            bool operator()(unsigned a, unsigned b) const {
                return _metric_ref.get()[a].value() < _metric_ref.get()[b].value();
            }
        };

        using sorted_shards_type = std::multiset<unsigned, shards_metric_comp>;

    public:
        using latency_clock = std::chrono::steady_clock;

        struct request_ctx {
            unsigned cpu;
            size_t budget = 0;
            latency_clock::time_point start_time;
            latency_clock::time_point end_time;
        };

        using request_ctx_ptr = lw_shared_ptr<request_ctx>;

    private:
        distributed<cql_server>& _cql_server;
        cql_load_balance _lb;
        stdx::optional<balancing_state> _state; // is initialized only on shard0
        std::vector<shard_metric> _shard_metric;
        // The two below contain the shard IDs of the shards that currently participate in the load balancing on the local shard.
        sorted_shards_type _sorted_shards;
        std::vector<unsigned> _receiving_shards;

        gate _loads_collector_timer_gate; // FIXME: rename
        timer<load_balancer_clock> _loads_collector_timer; // FIXME: rename
        gate _load_balance_timer_gate; // FIXME: rename
        timer<load_balancer_clock> _load_balance_timer; // FIXME: rename
        seastar::metrics::metric_groups _metrics;

    public:
        load_balancer(distributed<cql_server>& cql_server, cql_load_balance lb);
        future<> stop();
        request_ctx_ptr pick_request_cpu(size_t initial_budget);
        void mark_request_processing_end(request_ctx_ptr ctx, size_t response_body_size);
        void complete_request_handling(request_ctx_ptr ctx);
        sorted_shards_type::iterator get_sorted_iterator_for_id(unsigned id);

        /// \brief Rebalance the element pointed by the given iterator.
        /// \note \ref id_it is going to become invalid after the call.
        /// \param id_it Iterator to the element to rebalance.
        template <class Func>
        void rebalance_id(sorted_shards_type::iterator id_it, Func&& metric_updater);

    private:
        /// \brief Collect _load values from all shards. Runs on shard0 only.
        void collect_loads();

        /// \brief Build the pool of shards to offload requests processing to.
        ///
        /// - Offload the work only if the local shard is loaded above or equal to start_offload_threshold.
        /// - Don't offload to the remote shard if:
        ///   - Its load is above or equal the start_offload_threshold (namely the remote shard offloads work by itself) or
        ///   - When the "remote shard load"/"local shard load" ratio is less or equal to can_accept_requests_load_factor.
        void build_shards_pool();

        std::vector<unsigned> get_pool(unsigned idx) const {
            return _state->get_pool(idx);
        }
    };

    static constexpr cql_protocol_version_type current_version = cql_serialization_format::latest_version;

    std::vector<server_socket> _listeners;
    distributed<cql_server>& _parent;
    distributed<service::storage_proxy>& _proxy;
    distributed<cql3::query_processor>& _query_processor;
    size_t _max_request_size;
    semaphore _memory_available;
    seastar::metrics::metric_groups _metrics;
    std::unique_ptr<event_notifier> _notifier;
private:
    uint64_t _connects = 0;
    uint64_t _connections = 0;
    uint64_t _requests_served = 0;
    uint64_t _unpaged_queries = 0;
    uint64_t _requests_serving = 0;
    uint64_t _requests_blocked_memory = 0;
    uint64_t _requests_processing = 0;
    uint64_t _requests_processed = 0;
    load_balancer _lbalancer;
    auth::service& _auth_service;

public:
    cql_server(distributed<cql_server>& parent, distributed<service::storage_proxy>& proxy, distributed<cql3::query_processor>& qp, cql_load_balance lb, auth::service&);
    future<> listen(ipv4_addr addr, std::shared_ptr<seastar::tls::credentials_builder> = {}, bool keepalive = false);
    future<> do_accepts(int which, bool keepalive, ipv4_addr server_addr);
    future<> stop();
public:
    class response;
    using response_type = std::pair<shared_ptr<cql_server::response>, service::client_state>;
private:
    class fmt_visitor;
    friend class connection;
    friend class process_request_executor;
    class connection : public boost::intrusive::list_base_hook<> {
        struct processing_result {
            foreign_ptr<shared_ptr<cql_server::response>> cql_response;
            foreign_ptr<std::unique_ptr<sstring>> keyspace;
            foreign_ptr<shared_ptr<auth::authenticated_user>> user;
            service::client_state::auth_state auth_state;

            processing_result(response_type r)
                : cql_response(make_foreign(std::move(r.first)))
                , keyspace(r.second.is_dirty() ? make_foreign(std::make_unique<sstring>(std::move(r.second.get_raw_keyspace()))) : nullptr)
                , user(r.second.user_is_dirty() ? make_foreign(r.second.user()) : nullptr)
                , auth_state(r.second.get_auth_state())
            {}
        };

        distributed<cql_server>& _distributed_server;
        cql_server& _server;
        ipv4_addr _server_addr;
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
        seastar::gate _pending_requests_gate;
        future<> _ready_to_respond = make_ready_future<>();
        cql_protocol_version_type _version = 0;
        cql_compression _compression = cql_compression::none;
        cql_serialization_format _cql_serialization_format = cql_serialization_format::latest();
        service::client_state _client_state;
        std::unordered_map<uint16_t, cql_query_state> _query_states;

        enum class tracing_request_type : uint8_t {
            not_requested,
            no_write_on_close,
            write_on_close
        };
    public:
        connection(distributed<cql_server>& server, ipv4_addr server_addr, connected_socket&& fd, socket_address addr);
        ~connection();
        future<> process();
        future<> process_request();
        future<> shutdown();
    private:
        friend class process_request_executor;
        future<processing_result> process_request_one(bytes_view buf, uint8_t op, uint16_t stream, service::client_state client_state, tracing_request_type tracing_request);
        unsigned frame_size() const;
        void update_client_state(processing_result& r);
        cql_binary_frame_v3 parse_frame(temporary_buffer<char> buf);
        future<temporary_buffer<char>> read_and_decompress_frame(size_t length, uint8_t flags);
        future<std::experimental::optional<cql_binary_frame_v3>> read_frame();
        future<response_type> process_startup(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_auth_response(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_options(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_query(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_prepare(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_execute(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_batch(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_register(uint16_t stream, bytes_view buf, service::client_state client_state);

        shared_ptr<cql_server::response> make_unavailable_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t required, int32_t alive, const tracing::trace_state_ptr& tr_state);
        shared_ptr<cql_server::response> make_read_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, bool data_present, const tracing::trace_state_ptr& tr_state);
        shared_ptr<cql_server::response> make_read_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t numfailures, int32_t blockfor, bool data_present, const tracing::trace_state_ptr& tr_state);
        shared_ptr<cql_server::response> make_mutation_write_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, db::write_type type, const tracing::trace_state_ptr& tr_state);
        shared_ptr<cql_server::response> make_mutation_write_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t numfailures, int32_t blockfor, db::write_type type, const tracing::trace_state_ptr& tr_state);
        shared_ptr<cql_server::response> make_already_exists_error(int16_t stream, exceptions::exception_code err, sstring msg, sstring ks_name, sstring cf_name, const tracing::trace_state_ptr& tr_state);
        shared_ptr<cql_server::response> make_unprepared_error(int16_t stream, exceptions::exception_code err, sstring msg, bytes id, const tracing::trace_state_ptr& tr_state);
        shared_ptr<cql_server::response> make_error(int16_t stream, exceptions::exception_code err, sstring msg, const tracing::trace_state_ptr& tr_state);
        shared_ptr<cql_server::response> make_ready(int16_t stream, const tracing::trace_state_ptr& tr_state);
        shared_ptr<cql_server::response> make_supported(int16_t stream, const tracing::trace_state_ptr& tr_state);
        shared_ptr<cql_server::response> make_result(int16_t stream, shared_ptr<cql_transport::messages::result_message> msg, const tracing::trace_state_ptr& tr_state, bool skip_metadata = false);
        shared_ptr<cql_server::response> make_topology_change_event(const cql_transport::event::topology_change& event);
        shared_ptr<cql_server::response> make_status_change_event(const cql_transport::event::status_change& event);
        shared_ptr<cql_server::response> make_schema_change_event(const cql_transport::event::schema_change& event);
        shared_ptr<cql_server::response> make_autheticate(int16_t, const sstring&, const tracing::trace_state_ptr& tr_state);
        shared_ptr<cql_server::response> make_auth_success(int16_t, bytes, const tracing::trace_state_ptr& tr_state);
        shared_ptr<cql_server::response> make_auth_challenge(int16_t, bytes, const tracing::trace_state_ptr& tr_state);

        future<> write_response(foreign_ptr<shared_ptr<cql_server::response>>&& response, cql_compression compression = cql_compression::none);

        void check_room(bytes_view& buf, size_t n);
        void validate_utf8(sstring_view s);
        int8_t read_byte(bytes_view& buf);
        int32_t read_int(bytes_view& buf);
        int64_t read_long(bytes_view& buf);
        uint16_t read_short(bytes_view& buf);
        sstring read_string(bytes_view& buf);
        sstring_view read_string_view(bytes_view& buf);
        sstring_view read_long_string_view(bytes_view& buf);
        bytes_opt read_bytes(bytes_view& buf);
        bytes read_short_bytes(bytes_view& buf);
        cql3::raw_value read_value(bytes_view& buf);
        cql3::raw_value_view read_value_view(bytes_view& buf);
        void read_name_and_value_list(bytes_view& buf, std::vector<sstring_view>& names, std::vector<cql3::raw_value_view>& values);
        void read_string_list(bytes_view& buf, std::vector<sstring>& strings);
        void read_value_view_list(bytes_view& buf, std::vector<cql3::raw_value_view>& values);
        db::consistency_level read_consistency(bytes_view& buf);
        std::unordered_map<sstring, sstring> read_string_map(bytes_view& buf);
        std::unique_ptr<cql3::query_options> read_options(bytes_view& buf);
        std::unique_ptr<cql3::query_options> read_options(bytes_view& buf, uint8_t);

        void init_cql_serialization_format();

        friend event_notifier;
    };

    friend class type_codec;
private:
    bool _stopping = false;
    promise<> _all_connections_stopped;
    future<> _stopped = _all_connections_stopped.get_future();
    boost::intrusive::list<connection> _connections_list;
    uint64_t _total_connections = 0;
    uint64_t _current_connections = 0;
    uint64_t _connections_being_accepted = 0;
private:
    void maybe_idle() {
        if (_stopping && !_connections_being_accepted && !_current_connections) {
            _all_connections_stopped.set_value();
        }
    }
};

class cql_server::event_notifier : public service::migration_listener,
                                   public service::endpoint_lifecycle_subscriber
{
    std::set<cql_server::connection*> _topology_change_listeners;
    std::set<cql_server::connection*> _status_change_listeners;
    std::set<cql_server::connection*> _schema_change_listeners;
    std::unordered_map<gms::inet_address, event::status_change::status_type> _last_status_change;
public:
    event_notifier();
    ~event_notifier();
    void register_event(cql_transport::event::event_type et, cql_server::connection* conn);
    void unregister_connection(cql_server::connection* conn);

    virtual void on_create_keyspace(const sstring& ks_name) override;
    virtual void on_create_column_family(const sstring& ks_name, const sstring& cf_name) override;
    virtual void on_create_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) override;
    virtual void on_create_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;

    virtual void on_update_keyspace(const sstring& ks_name) override;
    virtual void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool columns_changed) override;
    virtual void on_update_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override;
    virtual void on_update_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;

    virtual void on_drop_keyspace(const sstring& ks_name) override;
    virtual void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override;
    virtual void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override;
    virtual void on_drop_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;

    virtual void on_join_cluster(const gms::inet_address& endpoint) override;
    virtual void on_leave_cluster(const gms::inet_address& endpoint) override;
    virtual void on_up(const gms::inet_address& endpoint) override;
    virtual void on_down(const gms::inet_address& endpoint) override;
    virtual void on_move(const gms::inet_address& endpoint) override;
};

using response_type = cql_server::response_type;

}

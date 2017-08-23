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

#pragma once

#include <unordered_map>
#include <vector>
#include <list>
#include <chrono>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_mutex.hh>
#include "cql3/query_processor.hh"
#include "gms/gossiper.hh"
#include "db/commitlog/commitlog.hh"
#include "utils/loading_shared_values.hh"

namespace bi = boost::intrusive;

namespace db {
namespace hints {

using node_to_hint_store_factory_type = utils::loading_shared_values<gms::inet_address, db::commitlog>;
using hints_store_ptr = node_to_hint_store_factory_type::entry_ptr;
using hint_entry_reader = commitlog_entry_reader;
using timer_clock_type = seastar::lowres_clock;

class manager : public seastar::async_sharded_service<manager> {
private:
    class end_point_hints_manager : public bi::unordered_set_base_hook<bi::store_hash<true>> {
    public:
        using key_type = gms::inet_address;

    private:
        key_type _key;
        manager& _shard_manager;
        database& _db;
        service::storage_proxy& _proxy;
        gms::gossiper& _gossiper;
        hints_store_ptr _hints_store_anchor;
        std::list<sstring> _segments_to_replay;
        db::replay_position _last_not_complete_rp;
        seastar::shared_mutex _file_update_mutex;
        int _replayed_segments_count = 0;
        std::unordered_set<db::replay_position> _rps_set; // number of elements in this set is never going to be greater than the maximum send queue length
        bool _segment_replay_ok = true;
        bool _can_hint = true;
        bool _ep_state_is_not_normal = false;
        bool _stopping = false;
        const boost::filesystem::path _hints_dir;
        uint64_t _hints_in_progress = 0;

    public:
        end_point_hints_manager(const key_type& key, manager& shard_manager);

        const key_type& end_point_key() const noexcept {
            return _key;
        }

        /// \brief Get the corresponding hints_store object. Create it if needed.
        /// \note Must be called under the \ref _file_update_mutex.
        /// \return The corresponding hints_store object.
        future<hints_store_ptr> get_or_load();

        /// \brief Flush the pending hints to the disk and start sending if we can (see can_send()).
        ///
        ///  - Flush hints aggregated to far to the storage.
        ///  - If the _segments_to_replay is empty this function is going to repopulate it with hints' files aggregated
        ///    so far.
        ///  - If we can - start sending.
        ///
        /// \return Ready future then sending is over.
        future<> on_timer();

        /// \brief Store a single mutation hint.
        /// \param s column family descriptor
        /// \param fm frozen mutation object
        /// \return Ready future when hint is stored.
        future<> store_hint(schema_ptr s, lw_shared_ptr<const frozen_mutation> fm);

        /// \brief Populates the _segments_to_replay list.
        ///  Populates the _segments_to_replay list with the names of the files in the <manager hints files directory> directory
        ///  in the order they should be sent out.
        ///
        /// \return Ready future when end point hints manager is initialized.
        future<> populate_segments_to_replay();

        /// \brief Waits till all writers complete and shuts down the hints store.
        /// \return Ready future when the store is shut down.
        future<> stop() noexcept;

        /// \return Number of in-flight (towards the file) hints.
        uint64_t hints_in_progress() const noexcept {
            return _hints_in_progress;
        }

        bool can_hint() const noexcept {
            return _can_hint;
        }

        void allow_hints() noexcept {
            _can_hint = true;
        }

        void forbid_hints() noexcept {
            _can_hint = false;
        }

        seastar::shared_mutex& file_update_mutex() {
            return _file_update_mutex;
        }

    public:
        struct key_eq {
           bool operator()(const key_type& k, const end_point_hints_manager& c) const {
               return k == c.end_point_key();
           }

           bool operator()(const end_point_hints_manager& c, const key_type& k) const {
               return c.end_point_key() == k;
           }
        };

        friend bool operator==(const end_point_hints_manager& a, const end_point_hints_manager& b){
            return a.end_point_key() == b.end_point_key();
        }

        friend std::size_t hash_value(const end_point_hints_manager& v) {
            return std::hash<key_type>()(v.end_point_key());
        }

    private:
        /// \brief Creates a new hints store object.
        ///
        /// - Creates a hints store directory if doesn't exist: <shard_hints_dir>/<ep_key>
        /// - Creates a store object.
        /// - Populate _segments_to_replay if it's empty.
        ///
        /// \return A new hints store object.
        future<commitlog> add_store() noexcept;

        /// \brief Send hints collected so far.
        ///
        /// Send hints aggregated so far. This function is going to try to deplete
        /// the _segments_to_replay list. Once it's empty it's going to be repopulated during the next send_hints() call
        /// with the new hints files if any.
        ///
        /// send_hints() is going to stop sending if it sends for too long (longer than the timer period). In this case it's
        /// going to return and next send_hints() is going to continue from the point the previous call left.
        ///
        /// \return future that is going to resolve when sending is complete.
        future<> send_hints();

        /// \brief Perform a single (hint) mutation send atempt.
        ///
        /// If the original destination end point is still a replica for the given mutation - send the mutation directly
        /// to it, otherwise execute the mutation "from scratch" with CL=ANY.
        ///
        /// \param m mutation to send
        /// \param natural_endpoints current replicas for the given mutation
        /// \return future that resolves when the operation is complete
        future<> do_send_one_hint(mutation m, const std::vector<gms::inet_address>& natural_endpoints) noexcept;

        /// \brief Try to send the (hint) mutation out until it either succeeds or we have to stop trying.
        ///
        /// We are going to re-try sending the mutation until it either succeeds, we run out of time or we are no longer
        /// able to send (see can_send()).
        ///
        /// \param m mutation to send
        /// \param timeout time we are allowed to retry sending the mutation
        /// \return future that resolves when the mutation is sent of when we have to stop. In the later case it's going
        /// to cary the exception with the corresponding error.
        future<> send_one_hint(mutation m, timer_clock_type::duration timeout);

        /// \brief Checks if we can still send hints.
        /// \return TRUE if the destination Node is either ALIVE or has left the NORMAL state (e.g. has been decommissioned).
        bool can_send() noexcept;


        /// \brief Flushes all hints written so far to the disk.
        ///  - Repopulates the _segments_to_replay list if needed.
        ///
        /// \return Ready future when the procedure above completes.
        future<> flush_current_hints();

        /// \brief Get the last modification time stamp for a given file.
        /// \param fname File name
        /// \return The last modification time stamp for \param fname.
        static future<timespec> get_last_file_modification(const sstring& fname);
    };

private:
    using ep_managers_set_type = bi::unordered_set<end_point_hints_manager, bi::power_2_buckets<true>, bi::compare_hash<true>>;
    using bi_set_bucket_traits = typename ep_managers_set_type::bucket_traits;
    using ep_key_type = typename end_point_hints_manager::key_type;

    class space_watchdog {
    private:
        static const std::chrono::seconds _watchdog_period;

    private:
        std::unordered_set<ep_key_type> _eps_with_pending_hints;
        size_t _total_size = 0;
        manager& _shard_manager;
        seastar::gate _gate;
        seastar::timer<timer_clock_type> _timer;
        int _files_count = 0;

    public:
        space_watchdog(manager& shard_manager);
        future<> stop() noexcept;
        void start();

    private:
        /// \brief Check that hints don't occupy too much disk space.
        ///
        /// Verifies that the whole \ref manager::_hints_dir occupies less than \ref manager::max_shard_disk_space_size.
        ///
        /// If it does, stop all end point managers that have more than one hints file - we don't want some DOWN Node to
        /// prevent hints to other Nodes from being generated (e.g. due to some temporary overload and timeout).
        ///
        /// This is a simplistic implementation of a manager for a limited shared resource with a minimum guarantied share for all
        /// participants.
        ///
        /// This implementation guaranties at least a single hint share for all end point managers.
        void on_timer();

        /// \brief Scan files in a single end point directory.
        ///
        /// Add sizes of files in the directory to _total_size. If number of files is greater than 1 add this end point ID
        /// to _eps_with_pending_hints so that we may block it if _total_size value becomes greater than the maximum allowed
        /// value.
        ///
        /// \param path directory to scan
        /// \param ep_name end point ID (as a string)
        /// \return future that resolves when scanning is complete
        future<> scan_one_ep_dir(boost::filesystem::path path, ep_key_type ep_name);
    };

public:
    static const std::string FILENAME_PREFIX;
    static const std::chrono::seconds hints_timer_period;
    static const std::chrono::seconds hint_file_write_timeout;
    static size_t max_shard_disk_space_size;

private:
    static constexpr size_t _initial_buckets_count = 16;
    static constexpr uint64_t _max_size_of_hints_in_progress = 10 * 1024 * 1024; // 10MB
    static constexpr size_t _hint_segment_size_in_mb = 32;
    static constexpr size_t _max_hints_per_ep_size_mb = 128; // 4 files 32MB each
    static constexpr size_t _max_hints_send_queue_length = 128;
    const boost::filesystem::path _hints_dir;

    node_to_hint_store_factory_type _store_factory;
    seastar::gate _hints_write_gate;
    seastar::gate _hints_timer_gate;
    std::unordered_set<sstring> _hinted_dcs;
    seastar::timer<timer_clock_type> _timer;
    std::unordered_map<table_schema_version, column_mapping> _schema_ver_to_column_mapping;
    shared_ptr<service::storage_proxy> _proxy_anchor;
    shared_ptr<gms::gossiper> _gossiper_anchor;
    locator::snitch_ptr& _local_snitch_ptr;
    int64_t _max_hint_window_us = 0;
    database& _local_db;

    // Limit the maximum size of in-flight (being sent) hints by 10% of the shard memory.
    // Also don't allow more than 128 in-flight hints to limit the collateral memory consumption as well.
    const size_t _max_send_in_flight_memory;
    const size_t _min_send_hint_budget;
    seastar::semaphore _send_limiter;

    space_watchdog _space_watchdog;

    std::vector<typename ep_managers_set_type::bucket_type> _buckets;
    ep_managers_set_type _ep_managers;

    struct stats {
        uint64_t size_of_hints_in_progress = 0;
        uint64_t written = 0;
        uint64_t errors = 0;
        uint64_t dropped = 0;
        uint64_t sent = 0;
    } _stats;

    seastar::metrics::metric_groups _metrics;

public:
    manager(sstring hints_directory, std::vector<sstring> hinted_dcs, int64_t max_hint_window_ms, distributed<database>& db);
    ~manager();
    static future<> start();
    future<> stop();
    bool store_hint(gms::inet_address ep, schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept;

    static seastar::sharded<manager>& manager_instance() {
        // FIXME: leaked intentionally to avoid shutdown problems, see #293
        static seastar::sharded<manager>* manager_inst = new seastar::sharded<manager>();

        return *manager_inst;
    }

    static manager& get_local_manager_instance() {
        return manager_instance().local();
    }

    static future<> create(sstring hints_dir, std::vector<sstring> hinted_dcs, int64_t max_hint_window_ms, distributed<database>& db);

    /// \brief Check if a hint may be generated to the give end point
    /// \param ep end point to check
    /// \return true if we should generate the hint to the given end point if it becomes unavailable
    bool can_hint_for(ep_key_type ep) const noexcept;

    /// \brief Check if there aren't too many in-flight hints for the given end point
    /// \param ep end point to check
    /// \return TRUE if we are allowed to generate hint to the given end point but there are too many in-flight hints
    bool too_many_in_flight_hints_for(ep_key_type ep) const noexcept;

    /// \brief Check if DC \param ep belongs to is "hintable"
    /// \param ep End point identificator
    /// \return TRUE if hints are allowed to be generated to \param ep.
    bool check_dc_for(ep_key_type ep) const noexcept;

    /// \return Size of mutations of hints in-flight (to the disk) at the moment.
    uint64_t size_of_hints_in_progress() const noexcept {
        return _stats.size_of_hints_in_progress;
    }

    /// \brief Get the number of in-flight (to the disk) hints to a given end point.
    /// \param ep End point identificator
    /// \return Number of hints in-flight to \param ep.
    uint64_t hints_in_progress_for(ep_key_type ep) const noexcept {
        auto it = find_ep_manager(ep);
        if (it == ep_managers_end()) {
            return 0;
        }
        return it->hints_in_progress();
    }

private:
    static future<> rebalance() {
        // TODO
        return make_ready_future<>();
    }

    node_to_hint_store_factory_type& store_factory() {
        return _store_factory;
    }

    const boost::filesystem::path& hints_dir() const {
        return _hints_dir;
    }

    service::storage_proxy& local_storage_proxy() const noexcept {
        return *_proxy_anchor;
    }

    gms::gossiper& local_gossiper() const noexcept {
        return *_gossiper_anchor;
    }

    database& local_db() noexcept {
        return _local_db;
    }

    /// \brief Restore a mutation object from the hints file entry.
    /// \param buf hints file entry
    /// \return The mutation object representing the original mutation stored in the hints file.
    mutation get_mutation(temporary_buffer<char>& buf);

    end_point_hints_manager& get_ep_manager(ep_key_type ep);
    bool have_ep_manager(ep_key_type ep) const noexcept;
    future<> start_one();
    void on_timer();

    /// \brief Get a reference to the column_mapping object for a given frozen mutation.
    /// \param fm Frozen mutation object
    /// \param hr hint entry reader object
    /// \return
    const column_mapping& get_column_mapping(const frozen_mutation& fm, const hint_entry_reader& hr);

private:
    /// Hash table related methods /////////////////////////////////////////////////////////////////////////////////////
    void rehash_before_insert() noexcept;
    size_t buckets_count() const noexcept {
        return _buckets.size();
    }

    static void destroy_ep_manager(end_point_hints_manager* val) {
        delete val;
    }

    ep_managers_set_type::iterator find_ep_manager(ep_key_type ep_key) noexcept {
        return _ep_managers.find(ep_key, std::hash<ep_key_type>(), typename end_point_hints_manager::key_eq());
    }

    ep_managers_set_type::const_iterator find_ep_manager(ep_key_type ep_key) const noexcept {
        return _ep_managers.find(ep_key, std::hash<ep_key_type>(), typename end_point_hints_manager::key_eq());
    }

    ep_managers_set_type::iterator ep_managers_end() noexcept {
        return _ep_managers.end();
    }

    ep_managers_set_type::const_iterator ep_managers_end() const noexcept {
        return _ep_managers.end();
    }
};

}
}

namespace std { // for prepared_statements_cache log printouts
template <typename T>
inline std::ostream& operator<<(std::ostream& os, const std::list<T>& l) {
    if (l.empty()) {
        os << "{}";
        return os;
    }

    os << "{" << *l.begin();
    std::for_each(std::next(l.begin()), l.end(), [&l, &os] (const T& v) {
        os << ", " << v;
    });
    os << "}";

    return os;
}
}
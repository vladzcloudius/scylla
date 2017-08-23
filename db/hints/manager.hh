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
        seastar::shared_mutex _write_mutex;
        int _replayed_segments_count = 0;
        const boost::filesystem::path _hints_dir;
        const boost::filesystem::path _upload_dir;
        uint64_t _hints_in_progress = 0;
        bool _stopping = false;

    public:
        end_point_hints_manager(const key_type& key, manager& shard_manager);

        const key_type& end_point_key() const noexcept {
            return _key;
        }

        /// \brief Get the corresponding hints_store object. Create it if needed.
        /// \note Must be called under the \ref _write_mutex.
        /// \return The corresponding hints_store object.
        future<hints_store_ptr> get_or_load();

        /// \brief Flush the pending hints to the disk and start sending if the destination Node is UP
        ///
        ///  - Flush hints aggregated to far to the storage.
        ///  - If the _segments_to_replay is empty this function is going to repopulate it with hints' files aggregated
        ///    so far.
        ///  - If the destination end point is UP - start sending.
        ///
        /// \return Ready future then sending is over.
        future<> on_timer();

        /// \brief Store a single mutation hint.
        /// \param s column family descriptor
        /// \param fm frozen mutation object
        /// \return Ready future when hint is stored.
        future<> store_hint(schema_ptr s, lw_shared_ptr<const frozen_mutation> fm);

        /// \brief Populates the _segments_to_replay list.
        ///  - Moves all existing hints files to the <manager hints files directory>/upload directory.
        ///  - Populates the _segments_to_replay list with the names of the files in the <manager hints files directory>/upload directory
        ///    in the order they should be sent out.
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
        /// - Moves all pre-existing hints files to the <shard_hints_dir>/<ep_key>/upload directory.
        /// - Populate _segments_to_replay.
        ///
        /// \return A new hints store object.
        future<commitlog> add_store() noexcept;

        /// \brief Send hints collected so far.
        ///
        /// Send hints aggregated in the <hints directory>/upload sub-directory. This function is going to try to deplete
        /// the _segments_to_replay list. Once it's empty it's going to be repopulated during the next send_hints() call
        /// with the new hints files if any.
        ///
        /// send_hints() is going to stop sending if it sends for too long (longer than the timer period). In this case it's
        /// going to return and next send_hints() is going to continue from the point the previous call left.
        ///
        /// \return future that is going to resolve when sending is complete.
        future<> send_hints();


        /// \brief Flushes all hints written so far to the disk if _segments_to_replay is empty.
        ///
        /// If _segments_to_replay is empty and the underlying hints store is initialized:
        ///  - Flushes all hints written so far to the disk.
        ///  - Moves the hints files to the "upload" directory.
        ///  - Repopulates the _segments_to_replay list.
        ///
        /// \return Ready future when the procedure above completes.
        future<> flush_current_hints();

        /// \brief Get the last modification time stamp for a given file.
        /// \param fname File name
        /// \return The last modification time stamp for \param fname.
        static future<timespec> get_last_file_modification(const sstring& fname);
    };
public:
    using timer_clock_type = seastar::lowres_clock;
    using ep_managers_set_type = bi::unordered_set<end_point_hints_manager, bi::power_2_buckets<true>, bi::compare_hash<true>>;
    using bi_set_bucket_traits = typename ep_managers_set_type::bucket_traits;
    using ep_key_type = typename end_point_hints_manager::key_type;

    static const std::string FILENAME_PREFIX;
    static const std::chrono::seconds hints_timer_period;
    static const std::chrono::seconds hint_file_write_timeout;

private:
    static constexpr size_t _initial_buckets_count = 16;
    static constexpr uint64_t _max_hints_in_progress = 128; // origin multiplies by FBUtilities.getAvailableProcessors() but we already sharded
    static constexpr size_t _max_hints_per_ep_size_mb = 128; // 4 files 32MB each
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

    std::vector<typename ep_managers_set_type::bucket_type> _buckets;
    ep_managers_set_type _ep_managers;
    struct stats {
        uint64_t total_hints_in_progress = 0;
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

    /// \return Number of hints in-flight (to the disk) at the moment.
    uint64_t total_hints_in_progress() const noexcept {
        return _stats.total_hints_in_progress;
    }

    /// \brief Get the number of in-flight (to the disk) hints to a given end point.
    /// \param ep End point identificator
    /// \return Number of hints in-flight to \param ep.
    uint64_t hints_in_progress_for(ep_key_type ep) const noexcept {
        auto it = _ep_managers.find(ep, std::hash<ep_key_type>(), typename end_point_hints_manager::key_eq());
        if (it == _ep_managers.end()) {
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
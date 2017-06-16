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
 * Modified by ScyllaDB
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

#include <memory>

#include "utils/data_output.hh"
#include <seastar/core/shared_future.hh>
#include <seastar/core/gate.hh>
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "replay_position.hh"
#include "utils/flush_queue.hh"
#include "log.hh"
#include "schema.hh"

namespace seastar { class file; }

#include "seastarx.hh"

namespace db {

class config;
class rp_set;
class rp_handle;
class entry_writer;

/*
 * Commit Log tracks every write operation into the system. The aim of
 * the commit log is to be able to successfully recover data that was
 * not stored to disk via the Memtable.
 *
 * This impl is cassandra log format compatible (for what it is worth).
 * The behaviour is similar, but not 100% identical as "stock cl".
 *
 * Files are managed with "normal" file writes (as normal as seastar
 * gets) - no mmapping. Data is kept in internal buffers which, when
 * full, are written to disk (see below). Files are also flushed
 * periodically (or always), ensuring all data is written + writes are
 * complete.
 *
 * In BATCH mode, every write to the log will also send the data to disk
 * + issue a flush and wait for both to complete.
 *
 * In PERIODIC mode, most writes will only add to the internal memory
 * buffers. If the mem buffer is saturated, data is sent to disk, but we
 * don't wait for the write to complete. However, if periodic (timer)
 * flushing has not been done in X ms, we will write + flush to file. In
 * which case we wait for it.
 *
 * The commitlog does not guarantee any ordering between "add" callers
 * (due to the above). The actual order in the commitlog is however
 * identified by the replay_position returned.
 *
 * Like the stock cl, the log segments keep track of the highest dirty
 * (added) internal position for a given table id (cf_id_type / UUID).
 * Code should ensure to use discard_completed_segments with UUID +
 * highest rp once a memtable has been flushed. This will allow
 * discarding used segments. Failure to do so will keep stuff
 * indefinately.
 */
class commitlog {
public:
    using timeout_clock = lowres_clock;

    class segment_manager;
    class segment;

    friend class rp_handle;
private:
    ::shared_ptr<segment_manager> _segment_manager;
public:
    enum class sync_mode {
        PERIODIC, BATCH
    };
    struct config {
        config() = default;
        config(const config&) = default;
        config(const db::config&);

        sstring commit_log_location;
        uint64_t commitlog_total_space_in_mb = 0;
        uint64_t commitlog_segment_size_in_mb = 32;
        uint64_t commitlog_sync_period_in_ms = 10 * 1000; //TODO: verify default!
        // Max number of segments to keep in pre-alloc reserve.
        // Not (yet) configurable from scylla.conf.
        uint64_t max_reserve_segments = 12;
        // Max active writes/flushes. Default value
        // zero means try to figure it out ourselves
        uint64_t max_active_writes = 0;
        uint64_t max_active_flushes = 0;

        sync_mode mode = sync_mode::PERIODIC;
        std::string fname_prefix = descriptor::FILENAME_PREFIX;
    };

    struct descriptor {
    private:
        descriptor(std::pair<uint64_t, uint32_t> p, const std::string& fname_prefix = FILENAME_PREFIX);
    public:
        static const std::string SEPARATOR;
        static const std::string FILENAME_PREFIX;
        static const std::string FILENAME_EXTENSION;

        descriptor(descriptor&&) = default;
        descriptor(const descriptor&) = default;
        descriptor(segment_id_type i, uint32_t v = 1, const std::string& fname_prefix = FILENAME_PREFIX);
        descriptor(replay_position p);
        descriptor(sstring filename, const std::string& fname_prefix = FILENAME_PREFIX);

        sstring filename() const;
        operator replay_position() const;

        const segment_id_type id;
        const uint32_t ver;
        const std::string filename_prefix = FILENAME_PREFIX;
    };

    commitlog(commitlog&&) noexcept;
    ~commitlog();

    /**
     * Commitlog is created via a factory func.
     * This of course because it needs to access disk to get up to speed.
     * Optionally, could have an "init" func and require calling this.
     */
    static future<commitlog> create_commitlog(config);


    /**
     * Note: To be able to keep impl out of header file,
     * actual data writing is done via a std::function.
     * If it is proven that this has unacceptable overhead, this can be replace
     * by iter an interface or move segments and stuff into the header. But
     * I hope not.
     *
     * A serializing func is provided along with a parameter indicating the size
     * of data to be written. (See add).
     * Don't write less, absolutely don't write more...
     */
    using output = data_output;
    using serializer_func = std::function<void(output&)>;

    /**
     * Add a "Mutation" to the commit log.
     *
     * Resolves with timed_out_error when timeout is reached.
     *
     * @param mutation_func a function that writes 'size' bytes to the log, representing the mutation.
     */
    future<rp_handle> add(const cf_id_type& id, size_t size, timeout_clock::time_point timeout, serializer_func mutation_func);

    /**
     * Template version of add.
     * Resolves with timed_out_error when timeout is reached.
     * @param mu an invokable op that generates the serialized data. (Of size bytes)
     */
    template<typename _MutationOp>
    future<rp_handle> add_mutation(const cf_id_type& id, size_t size, timeout_clock::time_point timeout, _MutationOp&& mu) {
        return add(id, size, timeout, [mu = std::forward<_MutationOp>(mu)](output& out) {
            mu(out);
        });
    }

    /**
     * Template version of add.
     * @param mu an invokable op that generates the serialized data. (Of size bytes)
     */
    template<typename _MutationOp>
    future<rp_handle> add_mutation(const cf_id_type& id, size_t size, _MutationOp&& mu) {
        return add_mutation(id, size, timeout_clock::time_point::max(), std::forward<_MutationOp>(mu));
    }

    /**
     * Add an entry to the commit log.
     * Resolves with timed_out_error when timeout is reached.
     * @param entry_writer a writer responsible for writing the entry
     */
    future<rp_handle> add_entry(const cf_id_type& id, shared_ptr<entry_writer> writer, timeout_clock::time_point timeout);

    /**
     * Modifies the per-CF dirty cursors of any commit log segments for the column family according to the position
     * given. Discards any commit log segments that are no longer used.
     *
     * @param cfId    the column family ID that was flushed
     * @param rp_set  the replay positions of the flush
     */
    void discard_completed_segments(const cf_id_type&, const rp_set&);

    void discard_completed_segments(const cf_id_type&);

    /**
     * A 'flush_handler' is invoked when the CL determines that size on disk has
     * exceeded allowable threshold. It is called once for every currently active
     * CF id with the highest replay_position which we would prefer to free "until".
     * I.e. a the highest potentially freeable position in the CL.
     *
     * Whatever the callback does to help (or not) this desire is up to him.
     * This is called synchronously, so callee might want to instigate async ops
     * in the background.
     *
     */
    typedef std::function<void(cf_id_type, replay_position)> flush_handler;
    typedef uint64_t flush_handler_id;

    class flush_handler_anchor {
    public:
        friend class commitlog;
        ~flush_handler_anchor();
        flush_handler_anchor(flush_handler_anchor&&);
        flush_handler_anchor(const flush_handler_anchor&) = delete;

        flush_handler_id release(); // disengage anchor - danger danger.
        void unregister();

    private:
        flush_handler_anchor(commitlog&, flush_handler_id);

        commitlog & _cl;
        flush_handler_id _id;
    };

    flush_handler_anchor add_flush_handler(flush_handler);
    void remove_flush_handler(flush_handler_id);

    /**
     * Returns a vector of the segment names
     */
    std::vector<sstring> get_active_segment_names() const;

    /**
     * Returns a vector of segment paths which were
     * preexisting when this instance of commitlog was created.
     *
     * The list will be empty when called for the second time.
     */
    std::vector<sstring> get_segments_to_replay();

    uint64_t get_total_size() const;
    uint64_t get_completed_tasks() const;
    uint64_t get_flush_count() const;
    uint64_t get_pending_tasks() const;
    uint64_t get_pending_flushes() const;
    uint64_t get_pending_allocations() const;
    uint64_t get_flush_limit_exceeded_count() const;
    uint64_t get_num_segments_created() const;
    uint64_t get_num_segments_destroyed() const;
    /**
     * Get number of inactive (finished), segments lingering
     * due to still being dirty
     */
    uint64_t get_num_dirty_segments() const;
    /**
     * Get number of active segments, i.e. still being allocated to
     */
    uint64_t get_num_active_segments() const;

    /**
     * Returns the largest amount of data that can be written in a single "mutation".
     */
    size_t max_record_size() const;

    /**
     * Return max allowed pending writes (per this shard)
     */
    uint64_t max_active_writes() const;
    /**
     * Return max allowed pending flushes (per this shard)
     */
    uint64_t max_active_flushes() const;

    future<> clear();

    const config& active_config() const;

    /**
     * Issues disk sync on all (allocating) segments. I.e. ensures that
     * all data written up until this call is indeed on disk.
     * _However_, if you issue new "add" ops while this is executing,
     * those can/will be missed.
     */
    future<> sync_all_segments();
    /**
     * Shuts everything down and causes any
     * incoming writes to throw exceptions
     */
    future<> shutdown();
    /**
     * Ensure segments are released, even if we don't free the
     * commitlog proper. (hint, our shutdown is "partial")
     */
    future<> release();

    future<std::vector<descriptor>> list_existing_descriptors() const;
    future<std::vector<descriptor>> list_existing_descriptors(const sstring& dir) const;

    future<std::vector<sstring>> list_existing_segments() const;
    future<std::vector<sstring>> list_existing_segments(const sstring& dir) const;

    typedef std::function<future<>(temporary_buffer<char>, replay_position)> commit_load_reader_func;

    class segment_data_corruption_error: public std::runtime_error {
    public:
        segment_data_corruption_error(std::string msg, uint64_t s)
                : std::runtime_error(msg), _bytes(s) {
        }
        uint64_t bytes() const {
            return _bytes;
        }
    private:
        uint64_t _bytes;
    };

    static subscription<temporary_buffer<char>, replay_position> read_log_file(file, commit_load_reader_func, position_type = 0);
    static future<std::unique_ptr<subscription<temporary_buffer<char>, replay_position>>> read_log_file(
            const sstring&, commit_load_reader_func, position_type = 0);
private:
    commitlog(config);
};

class db::commitlog::segment_manager : public ::enable_shared_from_this<segment_manager> {
public:
    config cfg;
    std::vector<sstring> _segments_to_replay;
    const uint64_t max_size;
    const uint64_t max_mutation_size;
    // Divide the size-on-disk threshold by #cpus used, since we assume
    // we distribute stuff more or less equally across shards.
    const uint64_t max_disk_size; // per-shard

    bool _shutdown = false;
    std::experimental::optional<shared_promise<>> _shutdown_promise = {};

    // Allocation must throw timed_out_error by contract.
    using timeout_exception_factory = default_timeout_exception_factory;

    basic_semaphore<timeout_exception_factory> _flush_semaphore;

    seastar::metrics::metric_groups _metrics;

    // TODO: verify that we're ok with not-so-great granularity
    using clock_type = lowres_clock;
    using time_point = clock_type::time_point;
    using sseg_ptr = ::shared_ptr<segment>;

    using request_controller_type = basic_semaphore<timeout_exception_factory, commitlog::timeout_clock>;
    using request_controller_units = semaphore_units<timeout_exception_factory, commitlog::timeout_clock>;
    request_controller_type _request_controller;

    stdx::optional<shared_future<with_clock<commitlog::timeout_clock>>> _segment_allocating;

    void account_memory_usage(size_t size) {
        _request_controller.consume(size);
    }

    void notify_memory_written(size_t size) {
        _request_controller.signal(size);
    }

    future<rp_handle>
    allocate_when_possible(const cf_id_type& id, shared_ptr<entry_writer> writer, commitlog::timeout_clock::time_point timeout);

    struct stats {
        uint64_t cycle_count = 0;
        uint64_t flush_count = 0;
        uint64_t allocation_count = 0;
        uint64_t bytes_written = 0;
        uint64_t bytes_slack = 0;
        uint64_t segments_created = 0;
        uint64_t segments_destroyed = 0;
        uint64_t pending_flushes = 0;
        uint64_t flush_limit_exceeded = 0;
        uint64_t total_size = 0;
        uint64_t buffer_list_bytes = 0;
        uint64_t total_size_on_disk = 0;
        uint64_t requests_blocked_memory = 0;
    };

    stats totals;

    size_t pending_allocations() const {
        return _request_controller.waiters();
    }

    future<> begin_flush() {
        ++totals.pending_flushes;
        if (totals.pending_flushes >= cfg.max_active_flushes) {
            ++totals.flush_limit_exceeded;
            _clogger.trace("Flush ops overflow: {}. Will block.", totals.pending_flushes);
        }
        return _flush_semaphore.wait();
    }
    void end_flush() {
        _flush_semaphore.signal();
        --totals.pending_flushes;
    }
    segment_manager(config c);
    ~segment_manager() {
        _clogger.trace("Commitlog {} disposed", cfg.commit_log_location);
    }

    uint64_t next_id() {
        return ++_ids;
    }

    std::exception_ptr sanity_check_size(size_t size) {
        if (size > max_mutation_size) {
            return make_exception_ptr(std::invalid_argument(
                            "Mutation of " + std::to_string(size)
                                    + " bytes is too large for the maxiumum size of "
                                    + std::to_string(max_mutation_size)));
        }
        return nullptr;
    }

    future<> init();
    future<sseg_ptr> new_segment();
    future<sseg_ptr> active_segment(commitlog::timeout_clock::time_point timeout);
    future<sseg_ptr> allocate_segment(bool active);

    future<> clear();
    future<> sync_all_segments(bool shutdown = false);
    future<> shutdown();

    void create_counters();

    future<> orphan_all();

    void discard_unused_segments();
    void discard_completed_segments(const cf_id_type&);
    void discard_completed_segments(const cf_id_type&, const rp_set&);
    void on_timer();
    void sync();
    void arm(uint32_t extra = 0) {
        if (!_shutdown) {
            _timer.arm(std::chrono::milliseconds(cfg.commitlog_sync_period_in_ms + extra));
        }
    }

    std::vector<sstring> get_active_names() const;
    uint64_t get_num_dirty_segments() const;
    uint64_t get_num_active_segments() const;

    using buffer_type = temporary_buffer<char>;

    buffer_type acquire_buffer(size_t s);
    void release_buffer(buffer_type&&);

    future<std::vector<descriptor>> list_descriptors(sstring dir);

    flush_handler_id add_flush_handler(flush_handler h) {
        auto id = ++_flush_ids;
        _flush_handlers[id] = std::move(h);
        return id;
    }
    void remove_flush_handler(flush_handler_id id) {
        _flush_handlers.erase(id);
    }

    void flush_segments(bool = false);

private:
    future<> clear_reserve_segments();

    size_t max_request_controller_units() const;
    segment_id_type _ids = 0;
    std::vector<sseg_ptr> _segments;
    queue<sseg_ptr> _reserve_segments;
    std::vector<buffer_type> _temp_buffers;
    std::unordered_map<flush_handler_id, flush_handler> _flush_handlers;
    flush_handler_id _flush_ids = 0;
    replay_position _flush_position;
    timer<clock_type> _timer;
    future<> replenish_reserve();
    future<> _reserve_replenisher;
    seastar::gate _gate;
    uint64_t _new_counter = 0;
    logging::logger& _clogger;
};

class cf_holder {
public:
    virtual ~cf_holder() {};
    virtual void release_cf_count(const cf_id_type&) = 0;
};

/*
 * A single commit log file on disk. Manages creation of the file and writing mutations to disk,
 * as well as tracking the last mutation position of any "dirty" CFs covered by the segment file. Segment
 * files are initially allocated to a fixed size and can grow to accomidate a larger value if necessary.
 *
 * The IO flow is somewhat convoluted and goes something like this:
 *
 * Mutation path:
 *  - Adding data to the segment usually writes into the internal buffer
 *  - On EOB or overflow we issue a write to disk ("cycle").
 *      - A cycle call will acquire the segment read lock and send the
 *        buffer to the corresponding position in the file
 *  - If we are periodic and crossed a timing threshold, or running "batch" mode
 *    we might be forced to issue a flush ("sync") after adding data
 *      - A sync call acquires the write lock, thus locking out writes
 *        and waiting for pending writes to finish. It then checks the
 *        high data mark, and issues the actual file flush.
 *        Note that the write lock is released prior to issuing the
 *        actual file flush, thus we are allowed to write data to
 *        after a flush point concurrently with a pending flush.
 *
 * Sync timer:
 *  - In periodic mode, we try to primarily issue sync calls in
 *    a timer task issued every N seconds. The timer does the same
 *    operation as the above described sync, and resets the timeout
 *    so that mutation path will not trigger syncs and delay.
 *
 * Note that we do not care which order segment chunks finish writing
 * to disk, other than all below a flush point must finish before flushing.
 *
 * We currently do not wait for flushes to finish before issueing the next
 * cycle call ("after" flush point in the file). This might not be optimal.
 *
 * To close and finish a segment, we first close the gate object that guards
 * writing data to it, then flush it fully (including waiting for futures create
 * by the timer to run their course), and finally wait for it to
 * become "clean", i.e. get notified that all mutations it holds have been
 * persisted to sstables elsewhere. Once this is done, we can delete the
 * segment. If a segment (object) is deleted without being fully clean, we
 * do not remove the file on disk.
 *
 */
class db::commitlog::segment : public enable_shared_from_this<segment>, public cf_holder {
    friend class rp_handle;

    ::shared_ptr<segment_manager> _segment_manager;

    descriptor _desc;
    file _file;
    sstring _file_name;

    uint64_t _file_pos = 0;
    uint64_t _flush_pos = 0;
    uint64_t _buf_pos = 0;
    bool _closed = false;

    using buffer_type = segment_manager::buffer_type;
    using sseg_ptr = segment_manager::sseg_ptr;
    using clock_type = segment_manager::clock_type;
    using time_point = segment_manager::time_point;

    buffer_type _buffer;
    std::unordered_map<cf_id_type, uint64_t> _cf_dirty;
    time_point _sync_time;
    seastar::gate _gate;
    uint64_t _write_waiters = 0;
    utils::flush_queue<replay_position, std::less<replay_position>, clock_type> _pending_ops;

    uint64_t _num_allocs = 0;

    std::unordered_set<table_schema_version> _known_schema_versions;

    friend std::ostream& operator<<(std::ostream&, const segment&);
    friend class segment_manager;

    future<> begin_flush() {
        // This is maintaining the semantica of only using the write-lock
        // as a gate for flushing, i.e. once we've begun a flush for position X
        // we are ok with writes to positions > X
        return _segment_manager->begin_flush();
    }

    void end_flush() {
        _segment_manager->end_flush();
    }

public:
    struct cf_mark {
        const segment& s;
    };
    friend std::ostream& operator<<(std::ostream&, const cf_mark&);

    // The commit log entry overhead in bytes (int: length + int: head checksum + int: tail checksum)
    static constexpr size_t entry_overhead_size = 3 * sizeof(uint32_t);
    static constexpr size_t segment_overhead_size = 2 * sizeof(uint32_t);
    static constexpr size_t descriptor_header_size = 5 * sizeof(uint32_t);
    static constexpr uint32_t segment_magic = ('S'<<24) |('C'<< 16) | ('L' << 8) | 'C';

    // The commit log (chained) sync marker/header size in bytes (int: length + int: checksum [segmentId, position])
    static constexpr size_t sync_marker_size = 2 * sizeof(uint32_t);

    static constexpr size_t alignment = 4096;
    // TODO : tune initial / default size
    static constexpr size_t default_size = align_up<size_t>(128 * 1024, alignment);

    segment(::shared_ptr<segment_manager> m, const descriptor& d, file && f, bool active);
    ~segment();

    bool is_schema_version_known(schema_ptr s) {
        return _known_schema_versions.count(s->version());
    }
    void add_schema_version(schema_ptr s) {
        _known_schema_versions.emplace(s->version());
    }
    void forget_schema_versions() {
        _known_schema_versions.clear();
    }

    void release_cf_count(const cf_id_type& cf) override {
        mark_clean(cf, 1);
        if (can_delete()) {
            _segment_manager->discard_unused_segments();
        }
    }

    bool must_sync();
    /**
     * Finalize this segment and get a new one
     */
    future<sseg_ptr> finish_and_get_new(commitlog::timeout_clock::time_point timeout) {
        _closed = true;
        sync();
        return _segment_manager->active_segment(timeout);
    }
    void reset_sync_time() {
        _sync_time = clock_type::now();
    }
    // See class comment for info
    future<sseg_ptr> sync(bool shutdown = false);
    // See class comment for info
    future<sseg_ptr> flush(uint64_t pos = 0);
    future<sseg_ptr> do_flush(uint64_t pos);

    /**
     * Allocate a new buffer
     */
    void new_buffer(size_t s);

    bool buffer_is_empty() const {
        return _buf_pos <= segment_overhead_size
                        || (_file_pos == 0 && _buf_pos <= (segment_overhead_size + descriptor_header_size));
    }
    /**
     * Send any buffer contents to disk and get a new tmp buffer
     */
    // See class comment for info
    future<sseg_ptr> cycle(bool flush_after = false);

    future<sseg_ptr> batch_cycle(timeout_clock::time_point timeout);

    /**
     * Add a "mutation" to the segment.
     */
    future<rp_handle> allocate(const cf_id_type& id, shared_ptr<entry_writer> writer, segment_manager::request_controller_units permit, commitlog::timeout_clock::time_point timeout);

    position_type position() const {
        return position_type(_file_pos + _buf_pos);
    }

    size_t size_on_disk() const {
        return _file_pos;
    }

    // ensures no more of this segment is writeable, by allocating any unused section at the end and marking it discarded
    // a.k.a. zero the tail.
    size_t clear_buffer_slack();
    void mark_clean(const cf_id_type& id, uint64_t count) {
        auto i = _cf_dirty.find(id);
        if (i != _cf_dirty.end()) {
            assert(i->second >= count);
            i->second -= count;
            if (i->second == 0) {
                _cf_dirty.erase(i);
            }
        }
    }
    void mark_clean(const cf_id_type& id) {
        _cf_dirty.erase(id);
    }
    void mark_clean() {
        _cf_dirty.clear();
    }
    bool is_still_allocating() const {
        return !_closed && position() < _segment_manager->max_size;
    }
    bool is_clean() const {
        return _cf_dirty.empty();
    }
    bool is_unused() const {
        return !is_still_allocating() && is_clean();
    }
    bool is_flushed() const {
        return position() <= _flush_pos;
    }
    bool can_delete() const {
        return is_unused() && is_flushed();
    }
    bool contains(const replay_position& pos) const {
        return pos.id == _desc.id;
    }
    sstring get_segment_name() const {
        return _desc.filename();
    }
};
}

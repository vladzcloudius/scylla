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

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/parent_from_member.hpp>

#include "core/memory.hh"
#include <seastar/core/thread.hh>
#include <seastar/util/noncopyable_function.hh>

#include "mutation_reader.hh"
#include "mutation_partition.hh"
#include "utils/logalloc.hh"
#include "utils/phased_barrier.hh"
#include "utils/histogram.hh"
#include "partition_version.hh"
#include "utils/estimated_histogram.hh"
#include "tracing/tracing.hh"
#include <seastar/core/metrics_registration.hh>
#include "flat_mutation_reader.hh"

namespace bi = boost::intrusive;

class row_cache;
class memtable_entry;
class cache_tracker;

namespace cache {

class autoupdating_underlying_reader;
class cache_streamed_mutation;
class cache_flat_mutation_reader;
class read_context;
class lsa_manager;

}

// Intrusive set entry which holds partition data.
//
// TODO: Make memtables use this format too.
class cache_entry {
    // We need auto_unlink<> option on the _cache_link because when entry is
    // evicted from cache via LRU we don't have a reference to the container
    // and don't want to store it with each entry.
    using cache_link_type = bi::set_member_hook<bi::link_mode<bi::auto_unlink>>;

    schema_ptr _schema;
    dht::decorated_key _key;
    partition_entry _pe;
    // True when we know that there is nothing between this entry and the previous one in cache
    struct {
        bool _continuous : 1;
        bool _dummy_entry : 1;
    } _flags{};
    cache_link_type _cache_link;
    friend class size_calculator;

    flat_mutation_reader do_read(row_cache&, cache::read_context& reader);
public:
    friend class row_cache;
    friend class cache_tracker;

    struct dummy_entry_tag{};
    struct incomplete_tag{};
    struct evictable_tag{};

    cache_entry(dummy_entry_tag)
        : _key{dht::token(), partition_key::make_empty()}
    {
        _flags._dummy_entry = true;
    }

    // Creates an entry which is fully discontinuous, except for the partition tombstone.
    cache_entry(incomplete_tag, schema_ptr s, const dht::decorated_key& key, tombstone t)
        : cache_entry(s, key, mutation_partition::make_incomplete(*s, t))
    { }

    cache_entry(schema_ptr s, const dht::decorated_key& key, const mutation_partition& p)
        : _schema(std::move(s))
        , _key(key)
        , _pe(partition_entry::make_evictable(*_schema, mutation_partition(p)))
    { }

    cache_entry(schema_ptr s, dht::decorated_key&& key, mutation_partition&& p)
        : cache_entry(evictable_tag(), s, std::move(key),
            partition_entry::make_evictable(*s, std::move(p)))
    { }

    // It is assumed that pe is fully continuous
    // pe must be evictable.
    cache_entry(evictable_tag, schema_ptr s, dht::decorated_key&& key, partition_entry&& pe) noexcept
        : _schema(std::move(s))
        , _key(std::move(key))
        , _pe(std::move(pe))
    { }

    cache_entry(cache_entry&&) noexcept;
    ~cache_entry();

    static cache_entry& container_of(partition_entry& pe) {
        return *boost::intrusive::get_parent_from_member(&pe, &cache_entry::_pe);
    }

    // Called when all contents have been evicted.
    // This object should unlink and destroy itself from the container.
    void on_evicted(cache_tracker&) noexcept;
    // Evicts contents of this entry.
    // The caller is still responsible for unlinking and destroying this entry.
    void evict(cache_tracker&) noexcept;
    const dht::decorated_key& key() const { return _key; }
    dht::ring_position_view position() const {
        if (is_dummy_entry()) {
            return dht::ring_position_view::max();
        }
        return _key;
    }
    const partition_entry& partition() const { return _pe; }
    partition_entry& partition() { return _pe; }
    const schema_ptr& schema() const { return _schema; }
    schema_ptr& schema() { return _schema; }
    flat_mutation_reader read(row_cache&, cache::read_context&);
    flat_mutation_reader read(row_cache&, cache::read_context&, utils::phased_barrier::phase_type);
    bool continuous() const { return _flags._continuous; }
    void set_continuous(bool value) { _flags._continuous = value; }

    bool is_dummy_entry() const { return _flags._dummy_entry; }

    struct compare {
        dht::ring_position_less_comparator _c;

        compare(schema_ptr s)
            : _c(*s)
        {}

        bool operator()(const dht::decorated_key& k1, const cache_entry& k2) const {
            return _c(k1, k2.position());
        }

        bool operator()(dht::ring_position_view k1, const cache_entry& k2) const {
            return _c(k1, k2.position());
        }

        bool operator()(const cache_entry& k1, const cache_entry& k2) const {
            return _c(k1.position(), k2.position());
        }

        bool operator()(const cache_entry& k1, const dht::decorated_key& k2) const {
            return _c(k1.position(), k2);
        }

        bool operator()(const cache_entry& k1, dht::ring_position_view k2) const {
            return _c(k1.position(), k2);
        }

        bool operator()(dht::ring_position_view k1, dht::ring_position_view k2) const {
            return _c(k1, k2);
        }
    };

    friend std::ostream& operator<<(std::ostream&, cache_entry&);
};

// Tracks accesses and performs eviction of cache entries.
class cache_tracker final {
public:
    using lru_type = bi::list<rows_entry,
        bi::member_hook<rows_entry, rows_entry::lru_link_type, &rows_entry::_lru_link>,
        bi::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.
public:
    friend class row_cache;
    friend class cache::read_context;
    friend class cache::autoupdating_underlying_reader;
    friend class cache::cache_streamed_mutation;
    friend class cache::cache_flat_mutation_reader;
    struct stats {
        uint64_t partition_hits;
        uint64_t partition_misses;
        uint64_t row_hits;
        uint64_t row_misses;
        uint64_t partition_insertions;
        uint64_t row_insertions;
        uint64_t static_row_insertions;
        uint64_t concurrent_misses_same_key;
        uint64_t partition_merges;
        uint64_t rows_processed_from_memtable;
        uint64_t rows_dropped_from_memtable;
        uint64_t rows_merged_from_memtable;
        uint64_t partition_evictions;
        uint64_t partition_removals;
        uint64_t row_evictions;
        uint64_t row_removals;
        uint64_t partitions;
        uint64_t rows;
        uint64_t mispopulations;
        uint64_t underlying_recreations;
        uint64_t underlying_partition_skips;
        uint64_t underlying_row_skips;
        uint64_t reads;
        uint64_t reads_with_misses;
        uint64_t reads_done;
        uint64_t pinned_dirty_memory_overload;

        uint64_t active_reads() const {
            return reads - reads_done;
        }
    };
private:
    stats _stats{};
    seastar::metrics::metric_groups _metrics;
    logalloc::region _region;
    lru_type _lru;
private:
    void setup_metrics();
public:
    cache_tracker();
    ~cache_tracker();
    void clear();
    void touch(rows_entry&);
    void insert(cache_entry&);
    void insert(partition_entry&) noexcept;
    void insert(partition_version&) noexcept;
    void insert(rows_entry&) noexcept;
    void on_remove(rows_entry&) noexcept;
    void unlink(rows_entry&) noexcept;
    void clear_continuity(cache_entry& ce);
    void on_partition_erase();
    void on_partition_merge();
    void on_partition_hit();
    void on_partition_miss();
    void on_partition_eviction();
    void on_row_eviction();
    void on_row_hit();
    void on_row_miss();
    void on_miss_already_populated();
    void on_mispopulate();
    void on_row_processed_from_memtable() { ++_stats.rows_processed_from_memtable; }
    void on_row_dropped_from_memtable() { ++_stats.rows_dropped_from_memtable; }
    void on_row_merged_from_memtable() { ++_stats.rows_merged_from_memtable; }
    void pinned_dirty_memory_overload(uint64_t bytes);
    allocation_strategy& allocator();
    logalloc::region& region();
    const logalloc::region& region() const;
    uint64_t partitions() const { return _stats.partitions; }
    const stats& get_stats() const { return _stats; }
};

// Returns a reference to shard-wide cache_tracker.
cache_tracker& global_cache_tracker();

//
// A data source which wraps another data source such that data obtained from the underlying data source
// is cached in-memory in order to serve queries faster.
//
// Cache populates itself automatically during misses.
//
// All updates to the underlying mutation source must be performed through one of the synchronizing methods.
// Those are the methods which accept external_updater, e.g. update(), invalidate().
// All synchronizers have strong exception guarantees. If they fail, the set of writes represented by
// cache didn't change.
// Synchronizers can be invoked concurrently with each other and other operations on cache.
//
class row_cache final {
public:
    using phase_type = utils::phased_barrier::phase_type;
    using partitions_type = bi::set<cache_entry,
        bi::member_hook<cache_entry, cache_entry::cache_link_type, &cache_entry::_cache_link>,
        bi::constant_time_size<false>, // we need this to have bi::auto_unlink on hooks
        bi::compare<cache_entry::compare>>;
    friend class cache::autoupdating_underlying_reader;
    friend class single_partition_populating_reader;
    friend class cache_entry;
    friend class cache::cache_streamed_mutation;
    friend class cache::cache_flat_mutation_reader;
    friend class cache::lsa_manager;
    friend class cache::read_context;
    friend class partition_range_cursor;
    friend class cache_tester;

    // A function which adds new writes to the underlying mutation source.
    // All invocations of external_updater on given cache instance are serialized internally.
    // Must have strong exception guarantees. If throws, the underlying mutation source
    // must be left in the state in which it was before the call.
    using external_updater = seastar::noncopyable_function<void()>;
public:
    struct stats {
        utils::timed_rate_moving_average hits;
        utils::timed_rate_moving_average misses;
        utils::timed_rate_moving_average reads_with_misses;
        utils::timed_rate_moving_average reads_with_no_misses;
    };
private:
    cache_tracker& _tracker;
    stats _stats{};
    schema_ptr _schema;
    partitions_type _partitions; // Cached partitions are complete.

    // The snapshots used by cache are versioned. The version number of a snapshot is
    // called the "population phase", or simply "phase". Between updates, cache
    // represents the same snapshot.
    //
    // Update doesn't happen atomically. Before it completes, some entries reflect
    // the old snapshot, while others reflect the new snapshot. After update
    // completes, all entries must reflect the new snapshot. There is a race between the
    // update process and populating reads. Since after the update all entries must
    // reflect the new snapshot, reads using the old snapshot cannot be allowed to
    // insert data which will no longer be reached by the update process. The whole
    // range can be therefore divided into two sub-ranges, one which was already
    // processed by the update and one which hasn't. Each key can be assigned a
    // population phase which determines to which range it belongs, as well as which
    // snapshot it reflects. The methods snapshot_of() and phase_of() can
    // be used to determine this.
    //
    // In general, reads are allowed to populate given range only if the phase
    // of the snapshot they use matches the phase of all keys in that range
    // when the population is committed. This guarantees that the range will
    // be reached by the update process or already has been in its entirety.
    // In case of phase conflict, current solution is to give up on
    // population. Since the update process is a scan, it's sufficient to
    // check when committing the population if the start and end of the range
    // have the same phases and that it's the same phase as that of the start
    // of the range at the time when reading began.

    mutation_source _underlying;
    phase_type _underlying_phase = 0;
    mutation_source_opt _prev_snapshot;

    // Positions >= than this are using _prev_snapshot, the rest is using _underlying.
    stdx::optional<dht::ring_position> _prev_snapshot_pos;

    snapshot_source _snapshot_source;

    // There can be at most one update in progress.
    seastar::semaphore _update_sem = {1};

    logalloc::allocating_section _update_section;
    logalloc::allocating_section _populate_section;
    logalloc::allocating_section _read_section;
    flat_mutation_reader create_underlying_reader(cache::read_context&, mutation_source&, const dht::partition_range&);
    flat_mutation_reader make_scanning_reader(const dht::partition_range&, lw_shared_ptr<cache::read_context>);
    void on_partition_hit();
    void on_partition_miss();
    void on_row_hit();
    void on_row_miss();
    void on_static_row_insert();
    void on_mispopulate();
    void upgrade_entry(cache_entry&);
    void invalidate_locked(const dht::decorated_key&);
    void invalidate_unwrapped(const dht::partition_range&);
    void clear_now() noexcept;

    struct previous_entry_pointer {
        stdx::optional<dht::decorated_key> _key;

        previous_entry_pointer() = default; // Represents dht::ring_position_view::min()
        previous_entry_pointer(dht::decorated_key key) : _key(std::move(key)) {};

        // TODO: store iterator here to avoid key comparison
    };

    template<typename CreateEntry, typename VisitEntry>
    //requires requires(CreateEntry create, VisitEntry visit, partitions_type::iterator it) {
    //        { create(it) } -> partitions_type::iterator;
    //        { visit(it) } -> void;
    //    }
    //
    // Must be run under reclaim lock
    cache_entry& do_find_or_create_entry(const dht::decorated_key& key, const previous_entry_pointer* previous,
                                 CreateEntry&& create_entry, VisitEntry&& visit_entry);

    // Ensures that partition entry for given key exists in cache and returns a reference to it.
    // Prepares the entry for reading. "phase" must match the current phase of the entry.
    //
    // Since currently every entry has to have a complete tombstone, it has to be provided here.
    // The entry which is returned will have the tombstone applied to it.
    //
    // Must be run under reclaim lock
    cache_entry& find_or_create(const dht::decorated_key& key, tombstone t, row_cache::phase_type phase, const previous_entry_pointer* previous = nullptr);

    partitions_type::iterator partitions_end() {
        return std::prev(_partitions.end());
    }

    // Only active phases are accepted.
    // Reference valid only until next deferring point.
    mutation_source& snapshot_for_phase(phase_type);

    // Returns population phase for given position in the ring.
    // snapshot_for_phase() can be called to obtain mutation_source for given phase, but
    // only until the next deferring point.
    // Should be only called outside update().
    phase_type phase_of(dht::ring_position_view);

    struct snapshot_and_phase {
        mutation_source& snapshot;
        phase_type phase;
    };

    // Optimized version of:
    //
    //  { snapshot_for_phase(phase_of(pos)), phase_of(pos) };
    //
    snapshot_and_phase snapshot_of(dht::ring_position_view pos);

    // Merges the memtable into cache with configurable logic for handling memtable entries.
    // The Updater gets invoked for every entry in the memtable with a lower bound iterator
    // into _partitions (cache_i), and the memtable entry.
    // It is invoked inside allocating section and in the context of cache's allocator.
    // All memtable entries will be removed.
    template <typename Updater>
    future<> do_update(external_updater, memtable& m, Updater func);

    // Clears given memtable invalidating any affected cache elements.
    void invalidate_sync(memtable&) noexcept;

    // A function which updates cache to the current snapshot.
    // It's responsible for advancing _prev_snapshot_pos between deferring points.
    //
    // Must have strong failure guarantees. Upon failure, it should still leave the cache
    // in a state consistent with the update it is performing.
    using internal_updater = std::function<future<>()>;

    // Atomically updates the underlying mutation source and synchronizes the cache.
    //
    // Strong failure guarantees. If returns a failed future, the underlying mutation
    // source was and cache are not modified.
    //
    // internal_updater is only kept alive until its invocation returns.
    future<> do_update(external_updater eu, internal_updater iu) noexcept;
public:
    ~row_cache();
    row_cache(schema_ptr, snapshot_source, cache_tracker&, is_continuous = is_continuous::no);
    row_cache(row_cache&&) = default;
    row_cache(const row_cache&) = delete;
    row_cache& operator=(row_cache&&) = default;
public:
    // Implements mutation_source for this cache, see mutation_reader.hh
    // User needs to ensure that the row_cache object stays alive
    // as long as the reader is used.
    // The range must not wrap around.
    flat_mutation_reader make_reader(schema_ptr,
                                     const dht::partition_range&,
                                     const query::partition_slice&,
                                     const io_priority_class& = default_priority_class(),
                                     tracing::trace_state_ptr trace_state = nullptr,
                                     streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
                                     mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::no);

    flat_mutation_reader make_reader(schema_ptr s, const dht::partition_range& range = query::full_partition_range) {
        auto& full_slice = s->full_slice();
        return make_reader(std::move(s), range, full_slice);
    }

    const stats& stats() const { return _stats; }
public:
    // Populate cache from given mutation, which must be fully continuous.
    // Intended to be used only in tests.
    // Can only be called prior to any reads.
    void populate(const mutation& m, const previous_entry_pointer* previous = nullptr);

    // Synchronizes cache with the underlying data source from a memtable which
    // has just been flushed to the underlying data source.
    // The memtable can be queried during the process, but must not be written.
    // After the update is complete, memtable is empty.
    future<> update(external_updater, memtable&);

    // Like update(), synchronizes cache with an incremental change to the underlying
    // mutation source, but instead of inserting and merging data, invalidates affected ranges.
    // Can be thought of as a more fine-grained version of invalidate(), which invalidates
    // as few elements as possible.
    future<> update_invalidating(external_updater, memtable&);

    // Refreshes snapshot. Must only be used if logical state in the underlying data
    // source hasn't changed.
    void refresh_snapshot();

    // Moves given partition to the front of LRU if present in cache.
    void touch(const dht::decorated_key&);

    // Detaches current contents of given partition from LRU, so
    // that they are not evicted by memory reclaimer.
    void unlink_from_lru(const dht::decorated_key&);

    // Synchronizes cache with the underlying mutation source
    // by invalidating ranges which were modified. This will force
    // them to be re-read from the underlying mutation source
    // during next read overlapping with the invalidated ranges.
    //
    // The ranges passed to invalidate() must include all
    // data which changed since last synchronization. Failure
    // to do so may result in reads seeing partial writes,
    // which would violate write atomicity.
    //
    // Guarantees that readers created after invalidate()
    // completes will see all writes from the underlying
    // mutation source made prior to the call to invalidate().
    future<> invalidate(external_updater, const dht::decorated_key&);
    future<> invalidate(external_updater, const dht::partition_range& = query::full_partition_range);
    future<> invalidate(external_updater, dht::partition_range_vector&&);

    // Evicts entries from given range in cache.
    //
    // Note that this does not synchronize with the underlying source,
    // it is assumed that the underlying source didn't change.
    // If it did, use invalidate() instead.
    void evict(const dht::partition_range& = query::full_partition_range);

    size_t partitions() const {
        return _partitions.size();
    }
    const cache_tracker& get_cache_tracker() const {
        return _tracker;
    }
    cache_tracker& get_cache_tracker() {
        return _tracker;
    }

    void set_schema(schema_ptr) noexcept;
    const schema_ptr& schema() const;

    friend std::ostream& operator<<(std::ostream&, row_cache&);

    friend class just_cache_scanning_reader;
    friend class scanning_and_populating_reader;
    friend class range_populating_reader;
    friend class cache_tracker;
    friend class mark_end_as_continuous;
};

namespace cache {

class lsa_manager {
    row_cache &_cache;
public:
    lsa_manager(row_cache &cache) : _cache(cache) {}

    template<typename Func>
    decltype(auto) run_in_read_section(const Func &func) {
        return _cache._read_section(_cache._tracker.region(), [&func]() {
            return with_linearized_managed_bytes([&func]() {
                return func();
            });
        });
    }

    template<typename Func>
    decltype(auto) run_in_update_section(const Func &func) {
        return _cache._update_section(_cache._tracker.region(), [&func]() {
            return with_linearized_managed_bytes([&func]() {
                return func();
            });
        });
    }

    template<typename Func>
    void run_in_update_section_with_allocator(Func &&func) {
        return _cache._update_section(_cache._tracker.region(), [this, &func]() {
            return with_linearized_managed_bytes([this, &func]() {
                return with_allocator(_cache._tracker.region().allocator(), [this, &func]() mutable {
                    return func();
                });
            });
        });
    }

    logalloc::region &region() { return _cache._tracker.region(); }

    logalloc::allocating_section &read_section() { return _cache._read_section; }
};

}

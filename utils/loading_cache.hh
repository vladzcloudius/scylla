/*
 * Copyright (C) 2016 ScyllaDB
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

#include <chrono>
#include <unordered_map>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>

#include <seastar/core/timer.hh>
#include <seastar/core/gate.hh>

#include "utils/exceptions.hh"
#include "utils/loading_shared_values.hh"

namespace bi = boost::intrusive;

namespace utils {
// Simple variant of the "LoadingCache" used for permissions in origin.

typedef lowres_clock loading_cache_clock_type;
typedef bi::list_base_hook<bi::link_mode<bi::auto_unlink>> auto_unlink_list_hook;

template<typename Tp, typename Key, typename EntrySize, typename Hash, typename EqualPred, typename LoadingSharedValuesStats>
class timestamped_val : public auto_unlink_list_hook, public bi::unordered_set_base_hook<bi::store_hash<true>> {
public:
    typedef bi::list<timestamped_val, bi::constant_time_size<false>> lru_list_type;
    typedef typename utils::loading_shared_values<Key, Tp, Hash, EqualPred, LoadingSharedValuesStats, 256> loading_values_type;
    typedef typename loading_values_type::entry_ptr value_ptr;
    typedef Tp value_type;

private:
    value_ptr _value_ptr;
    loading_cache_clock_type::time_point _loaded;
    loading_cache_clock_type::time_point _last_read;
    lru_list_type& _lru_list; /// MRU item is at the front, LRU - at the back
    size_t& _cache_size;
    size_t _size = 0;

public:
    struct key_eq {
       bool operator()(const Key& k, const timestamped_val& c) const {
           return EqualPred()(k, c.key());
       }

       bool operator()(const timestamped_val& c, const Key& k) const {
           return EqualPred()(c.key(), k);
       }
    };

    timestamped_val(value_ptr val_ptr, lru_list_type& lru_list, size_t& cache_size)
        : _value_ptr(std::move(val_ptr))
        , _loaded(loading_cache_clock_type::now())
        , _last_read(_loaded)
        , _lru_list(lru_list)
        , _cache_size(cache_size)
        , _size(EntrySize()(*_value_ptr))
    {
        _cache_size += _size;
    }

    ~timestamped_val() {
        _cache_size -= _size;
    }

    timestamped_val& operator=(Tp new_val) {
        *_value_ptr = std::move(new_val);
        _loaded = loading_cache_clock_type::now();
        _cache_size -= _size;
        _size = EntrySize()(*_value_ptr);
        _cache_size += _size;
        return *this;
    }

    const Tp& value() {
        _last_read = loading_cache_clock_type::now();
        touch();
        return *_value_ptr;
    }

    loading_cache_clock_type::time_point last_read() const noexcept {
        return _last_read;
    }

    loading_cache_clock_type::time_point loaded() const noexcept {
        return _loaded;
    }

    const Key& key() const {
        return loading_values_type::to_key(_value_ptr);
    }

    friend bool operator==(const timestamped_val& a, const timestamped_val& b){
        return EqualPred()(a.key(), b.key());
    }

    friend std::size_t hash_value(const timestamped_val& v) {
        return Hash()(v.key());
    }

private:
    /// Set this item as the most recently used item.
    /// The MRU item is going to be at the front of the _lru_list, the LRU item - at the back.
    void touch() noexcept {
        auto_unlink_list_hook::unlink();
        _lru_list.push_front(*this);
    }
};

template <typename Tp>
struct simple_entry_size {
    size_t operator()(const Tp& val) {
        return 1;
    }
};

template<typename Key,
         typename Tp,
         typename EntrySize = simple_entry_size<Tp>,
         typename Hash = std::hash<Key>,
         typename EqualPred = std::equal_to<Key>,
         typename LoadingSharedValuesStats = utils::do_nothing_loading_shared_values_stats,
         typename Alloc = std::allocator<timestamped_val<Tp, Key, EntrySize, Hash, EqualPred, LoadingSharedValuesStats>>>
class loading_cache {
private:
    typedef timestamped_val<Tp, Key, EntrySize, Hash, EqualPred, LoadingSharedValuesStats> ts_value_type;
    typedef bi::unordered_set<ts_value_type, bi::power_2_buckets<true>, bi::compare_hash<true>> set_type;
    typedef typename ts_value_type::loading_values_type loading_values_type;
    typedef typename ts_value_type::value_ptr value_ptr;
    typedef typename ts_value_type::lru_list_type lru_list_type;
    typedef typename set_type::bucket_traits bi_set_bucket_traits;

    static constexpr size_t initial_buckets_count = loading_values_type::initial_buckets_count;

public:
    typedef Tp value_type;
    typedef Key key_type;
    typedef typename set_type::iterator iterator;

    template<typename Func>
    loading_cache(size_t max_size, std::chrono::milliseconds expiry, std::chrono::milliseconds refresh, logging::logger& logger, Func&& load)
                : _buckets(initial_buckets_count)
                , _set(bi_set_bucket_traits(_buckets.data(), _buckets.size()))
                , _max_size(max_size)
                , _expiry(expiry)
                , _refresh(refresh)
                , _logger(logger)
                , _load(std::forward<Func>(load)) {

        // If expiration period is zero - caching is disabled
        if (!caching_enabled()) {
            return;
        }

        // Sanity check: if expiration period is given then non-zero refresh period and maximal size are required
        if (_refresh == std::chrono::milliseconds(0) || _max_size == 0) {
            throw exceptions::configuration_exception("loading_cache: caching is enabled but refresh period and/or max_size are zero");
        }

        _timer_period = std::min(_expiry, _refresh);
        _timer.set_callback([this] { on_timer(); });
        _timer.arm(_timer_period);
    }

    ~loading_cache() {
        _set.clear_and_dispose([] (ts_value_type* ptr) { loading_cache::destroy_ts_value(ptr); });
    }

    future<Tp> get(const Key& k) {
        // If caching is disabled - always load in the foreground
        if (!caching_enabled()) {
            return _load(k);
        }

        iterator i = _set.find(k, Hash(), typename ts_value_type::key_eq());
        if (i == _set.end()) {
            return _loading_values.get_or_load(k, _load).then([this, k] (value_ptr v_ptr) {
                // check again since it could have already been inserted
                iterator i = _set.find(k, Hash(), typename ts_value_type::key_eq());
                if (i == _set.end()) {
                    _logger.trace("{}: storing the value for the first time", k);
                    ts_value_type* new_ts_val = Alloc().allocate(1);
                    new(new_ts_val) ts_value_type(std::move(v_ptr), _lru_list, _current_size);
                    rehash_before_insert();
                    _set.insert(*new_ts_val);
                    return make_ready_future<Tp>(new_ts_val->value());
                }

                return make_ready_future<Tp>(i->value());
            });
        }

        return make_ready_future<Tp>(i->value());
    }

    future<> stop() {
        return _timer_reads_gate.close().finally([this] { _timer.cancel(); });
    }

private:
    bool caching_enabled() const {
        return _expiry != std::chrono::milliseconds(0);
    }

    static void destroy_ts_value(ts_value_type* val) {
        val->~ts_value_type();
        Alloc().deallocate(val, 1);
    }

    future<> reload(ts_value_type& ts_val) {
        return _load(ts_val.key()).then_wrapped([this, &ts_val] (auto&& f) {
            // The exceptions are related to the load operation itself.
            // We should ignore them for the background reads - if
            // they persist the value will age and will be reloaded in
            // the forground. If the foreground READ fails the error
            // will be propagated up to the user and will fail the
            // corresponding query.
            try {
                ts_val = f.get0();
            } catch (std::exception& e) {
                _logger.debug("{}: reload failed: {}", ts_val.key(), e.what());
            } catch (...) {
                _logger.debug("{}: reload failed: unknown error", ts_val.key());
            }
        });
    }

    void erase(iterator it) {
        _set.erase_and_dispose(it, [] (ts_value_type* ptr) { loading_cache::destroy_ts_value(ptr); });
        // no need to delete the item from _lru_list - it's auto-deleted
    }

    void drop_expired() {
        auto now = loading_cache_clock_type::now();
        _lru_list.remove_and_dispose_if([now, this] (const ts_value_type& v) {
            using namespace std::chrono;
            // An entry should be discarded if it hasn't been reloaded for too long or nobody cares about it anymore
            auto since_last_read = now - v.last_read();
            auto since_loaded = now - v.loaded();
            if (_expiry < since_last_read || _expiry < since_loaded) {
                _logger.trace("drop_expired(): {}: dropping the entry: _expiry {},  ms passed since: loaded {} last_read {}", v.key(), _expiry.count(), duration_cast<milliseconds>(since_loaded).count(), duration_cast<milliseconds>(since_last_read).count());
                return true;
            }
            return false;
        }, [this] (ts_value_type* p) {
            erase(_set.iterator_to(*p));
        });
    }

    // Shrink the cache to the _max_size discarding the least recently used items
    void shrink() {
        while (_current_size > _max_size) {
            using namespace std::chrono;
            ts_value_type& ts_val = *_lru_list.rbegin();
            _logger.trace("shrink(): {}: dropping the entry: ms since last_read {}", ts_val.key(), duration_cast<milliseconds>(loading_cache_clock_type::now() - ts_val.last_read()).count());
            erase(_set.iterator_to(ts_val));
        }
    }

    // Try to bring the load factors of both the _set and the _loading_values into a known range.
    void periodic_rehash() noexcept {
        try {
            _loading_values.rehash();
            sync_buckets_count();
        } catch (...) {
            // if rehashing fails - continue with the current buckets array
        }
    }

    void rehash_before_insert() noexcept {
        try {
            sync_buckets_count();
        } catch (...) {
            // if rehashing fails - continue with the current buckets array
        }
    }

    /// Try to keep the _set and _loading_values buckets counts synchronized because they contain the same
    /// number of elements, have the same InitialBucketsCount and the _loading_values is promised to have a less
    /// than 0.75 load factor.
    void sync_buckets_count() {
        if (_current_buckets_count != _loading_values.buckets_count()) {
            size_t new_buckets_count = _loading_values.buckets_count();
            std::vector<typename set_type::bucket_type> new_buckets(new_buckets_count);

            _set.rehash(bi_set_bucket_traits(new_buckets.data(), new_buckets.size()));
            _logger.trace("rehash(): buckets count changed: {} -> {}", _current_buckets_count, new_buckets_count);

            _buckets.swap(new_buckets);
            _current_buckets_count = new_buckets_count;
        }
    }

    void on_timer() {
        _logger.trace("on_timer(): start");

        auto timer_start_tp = loading_cache_clock_type::now();

        // Clean up items that were not touched for the whole _expiry period.
        drop_expired();

        // Remove the least recently used items if map is too big.
        shrink();

        // check if rehashing is needed and do it if it is.
        periodic_rehash();

        // Reload all those which vlaue needs to be reloaded.
        with_gate(_timer_reads_gate, [this, timer_start_tp] {
            return parallel_for_each(_set.begin(), _set.end(), [this, curr_time = timer_start_tp] (auto& ts_val) {
                _logger.trace("on_timer(): {}: checking the value age", ts_val.key());
                if (ts_val.loaded() + _refresh < curr_time) {
                    _logger.trace("on_timer(): {}: reloading the value", ts_val.key());
                    return this->reload(ts_val);
                }
                return now();
            }).finally([this, timer_start_tp] {
                _logger.trace("on_timer(): rearming");
                _timer.arm(timer_start_tp + _timer_period);
            });
        });
    }

    loading_values_type _loading_values;
    std::vector<typename set_type::bucket_type> _buckets;
    size_t _current_buckets_count = initial_buckets_count;
    set_type _set;
    lru_list_type _lru_list;
    size_t _current_size = 0;
    size_t _max_size;
    std::chrono::milliseconds _expiry;
    std::chrono::milliseconds _refresh;
    loading_cache_clock_type::duration _timer_period;
    logging::logger& _logger;
    std::function<future<Tp>(const Key&)> _load;
    timer<loading_cache_clock_type> _timer;
    seastar::gate _timer_reads_gate;
};

}


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

#pragma once

#include <vector>
#include <memory>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>
#include <boost/intrusive/unordered_set.hpp>
#include "seastarx.hh"

namespace bi = boost::intrusive;

namespace utils {

struct do_nothing_loading_shared_values_stats {
    static void inc_hits() {} // Increase the number of times entry was found ready
    static void inc_misses() {} // Increase the number of times entry was not found
    static void inc_blocks() {} // Increase the number of times entry was not ready (>= misses)
};

// Entries stay around as long as there is any live external reference (entry_ptr) to them.
// Supports asynchronous insertion, ensures that only one entry will be loaded.
template<typename Key,
         typename Tp,
         typename Hash = std::hash<Key>,
         typename EqualPred = std::equal_to<Key>,
         typename Stats = do_nothing_loading_shared_values_stats>
GCC6_CONCEPT( requires requires () {
    Stats::inc_hits();
    Stats::inc_misses();
    Stats::inc_blocks();
})
class loading_shared_values {
public:
    typedef Key key_type;
    typedef Tp value_type;

    static constexpr int initial_num_buckets = 256;
    static constexpr int max_num_buckets = 1024 * 1024;

private:
    class entry : public bi::unordered_set_base_hook<bi::store_hash<true>>, public enable_lw_shared_from_this<entry> {
    private:
        loading_shared_values& _parent;
        key_type _key;
        std::unique_ptr<value_type> _val_ptr;
        shared_promise<> _loaded;

    public:
        const key_type& key() const {
            return _key;
        }

        const value_type& value() const {
            return *_val_ptr;
        }

        value_type& value() {
            return *_val_ptr;
        }

        value_type&& move_value() {
            value_type v(std::move(*_val_ptr.release()));
            return std::move(v);
        }

        void set_value(value_type new_val) {
            _val_ptr = std::make_unique<value_type>(std::move(new_val));
        }

        shared_promise<>& loaded() {
            return _loaded;
        }

        struct key_eq {
           bool operator()(const key_type& k, const entry& c) const {
               return EqualPred()(k, c.key());
           }

           bool operator()(const entry& c, const key_type& k) const {
               return EqualPred()(c.key(), k);
           }
        };

        entry(loading_shared_values& parent, key_type k)
                : _parent(parent), _key(std::move(k)) {}

        ~entry() {
            _parent._set.erase(_parent._set.iterator_to(*this));
            _parent.rehash_after_erase();
        }

        friend bool operator==(const entry& a, const entry& b){
            return EqualPred()(a.key(), b.key());
        }

        friend std::size_t hash_value(const entry& v) {
            return Hash()(v.key());
        }
    };

public:
    typedef bi::unordered_set<entry, bi::power_2_buckets<true>, bi::compare_hash<true>> set_type;
    typedef typename set_type::bucket_traits bi_set_bucket_traits;

private:
    std::vector<typename set_type::bucket_type> _buckets;
    size_t _current_buckets_count = initial_num_buckets;
    set_type _set;

public:
    // Pointer to entry value
    class entry_ptr {
        lw_shared_ptr<entry> _e;
    public:
        using element_type = value_type;
        entry_ptr() = default;
        explicit entry_ptr(lw_shared_ptr<entry> e) : _e(std::move(e)) {}
        explicit operator bool() const { return bool(_e); }
        element_type& operator*() { return _e->value(); }
        const element_type& operator*() const { return _e->value(); }
        element_type* operator->() { return &_e->value(); }
        const element_type* operator->() const { return &_e->value(); }

        element_type release() {
            auto res = _e.owned() ? element_type(_e->move_value()) : element_type(_e->value());
            _e = {};
            return std::move(res);
        }

        friend class loading_shared_values;
    };

    static const key_type& to_key(const entry_ptr& e_ptr) {
        return e_ptr._e->key();
    }

    loading_shared_values()
            : _buckets(initial_num_buckets)
            , _set(bi_set_bucket_traits(_buckets.data(), _buckets.size()))
    {}
    loading_shared_values(loading_shared_values&&) = delete;
    loading_shared_values(const loading_shared_values&) = delete;
    ~loading_shared_values() {
        // assert(_set.size());
    }

    /// \brief
    /// Returns a future which resolves with a shared pointer to the entry for the given key.
    /// Always returns a valid pointer if succeeds.
    ///
    /// If entry is missing, the loader is invoked. If list is already loading, this invocation
    /// will wait for prior loading to complete and use its result when it's done.
    ///
    /// The loader object does not survive deferring, so the caller must deal with its liveness.
    template<typename Loader>
    future<entry_ptr> get_or_load(const key_type& key, Loader&& loader) {
        auto i = _set.find(key, Hash(), typename entry::key_eq());
        lw_shared_ptr<entry> e;
        if (i != _set.end()) {
            e = i->shared_from_this();
        } else {
            Stats::inc_misses();
            rehash_before_insert();
            e = make_lw_shared<entry>(*this, key);
            auto res = _set.insert(*e);
            assert(res.second);
            loader(key).then_wrapped([e] (future<value_type>&& f) mutable {
                if (f.failed()) {
                    e->loaded().set_exception(f.get_exception());
                } else {
                    e->set_value(f.get0());
                    e->loaded().set_value();
                }
            });
        }
        future<> f = e->loaded().get_shared_future();
        if (!f.available()) {
            Stats::inc_blocks();
            return f.then([e]() mutable {
                return entry_ptr(std::move(e));
            });
        } else {
            Stats::inc_hits();
            return make_ready_future<entry_ptr>(entry_ptr(std::move(e)));
        }
    }

private:
    void rehash_before_insert() {
        rehash(_set.size() + 1);
    }

    void rehash_after_erase() {
        rehash(_set.size());
    }

    void rehash(size_t new_size) {
        size_t new_buckets_count = 0;

        // Don't grow or shrink too fast even if there is a steep drop/growth in the number of elements in the set.
        // Exponential growth/backoff should be good enough.
        //
        // Try to keep the load factor between 0.25 and 1.0.
        if (new_size < _current_buckets_count / 4) {
            new_buckets_count = _current_buckets_count / 4;
        } else if (new_size > _current_buckets_count) {
            new_buckets_count = _current_buckets_count * 2;
        }

        if (new_buckets_count < initial_num_buckets || new_buckets_count > max_num_buckets) {
            return;
        }

        std::vector<typename set_type::bucket_type> new_buckets(new_buckets_count);
        _set.rehash(bi_set_bucket_traits(new_buckets.data(), new_buckets.size()));
        _buckets.swap(new_buckets);
        _current_buckets_count = new_buckets_count;
    }
};

}
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
#include "stdx.hh"

namespace bi = boost::intrusive;

namespace utils {

struct do_nothing_loading_shared_values_stats {
    static void inc_hits() {} // Increase the number of times entry was found ready
    static void inc_misses() {} // Increase the number of times entry was not found
    static void inc_blocks() {} // Increase the number of times entry was not ready (>= misses)
    static void inc_evictions() {} // Increase the number of times entry was evicted
};

// Entries stay around as long as there is any live external reference (entry_ptr) to them.
// Supports asynchronous insertion, ensures that only one entry will be loaded.
// InitialBucketsCount is required to be greater than zero. Otherwise a constructor will throw an
// std::invalid_argument exception.
template<typename Key,
         typename Tp,
         typename Hash = std::hash<Key>,
         typename EqualPred = std::equal_to<Key>,
         typename Stats = do_nothing_loading_shared_values_stats,
         size_t InitialBucketsCount = 16>
GCC6_CONCEPT( requires requires () {
    Stats::inc_hits();
    Stats::inc_misses();
    Stats::inc_blocks();
    Stats::inc_evictions();
})
class loading_shared_values {
public:
    using key_type = Key;
    using value_type = Tp;
    static constexpr size_t initial_buckets_count = InitialBucketsCount;

private:
    class entry : public bi::unordered_set_base_hook<bi::store_hash<true>>, public enable_lw_shared_from_this<entry> {
    private:
        loading_shared_values& _parent;
        key_type _key;
        stdx::optional<value_type> _val;
        shared_promise<> _loaded;

    public:
        const key_type& key() const {
            return _key;
        }

        const value_type& value() const {
            return *_val;
        }

        value_type& value() {
            return *_val;
        }

        value_type&& release() {
            return *std::move(_val);
        }

        void set_value(value_type new_val) {
            _val.emplace(std::move(new_val));
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
            Stats::inc_evictions();
        }

        friend bool operator==(const entry& a, const entry& b){
            return EqualPred()(a.key(), b.key());
        }

        friend std::size_t hash_value(const entry& v) {
            return Hash()(v.key());
        }
    };

public:
    using set_type = bi::unordered_set<entry, bi::power_2_buckets<true>, bi::compare_hash<true>>;
    using bi_set_bucket_traits = typename set_type::bucket_traits;

private:
    std::vector<typename set_type::bucket_type> _buckets;
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
        element_type& operator*() const { return _e->value(); }
        element_type* operator->() const { return &_e->value(); }

        element_type release() {
            auto res = _e.owned() ? element_type(_e->release()) : element_type(_e->value());
            _e = {};
            return std::move(res);
        }

        friend class loading_shared_values;
    };

    static const key_type& to_key(const entry_ptr& e_ptr) {
        return e_ptr._e->key();
    }

    /// \throw std::invalid_argument if InitialBucketsCount is zero
    loading_shared_values()
            : _buckets(InitialBucketsCount)
            , _set(bi_set_bucket_traits(_buckets.data(), _buckets.size()))
    {
        if (InitialBucketsCount == 0) {
            throw std::invalid_argument("initial buckets count should be greater than zero");
        }
    }
    loading_shared_values(loading_shared_values&&) = delete;
    loading_shared_values(const loading_shared_values&) = delete;
    ~loading_shared_values() {
         assert(!_set.size());
    }

    /// \brief
    /// Returns a future which resolves with a shared pointer to the entry for the given key.
    /// Always returns a valid pointer if succeeds.
    ///
    /// If entry is missing, the loader is invoked. If entry is already loading, this invocation
    /// will wait for prior loading to complete and use its result when it's done.
    ///
    /// The loader object does not survive deferring, so the caller must deal with its liveness.
    template<typename Loader>
    future<entry_ptr> get_or_load(const key_type& key, Loader&& loader) {
        static_assert(std::is_same<future<value_type>, std::result_of_t<Loader(const key_type&)>>::value, "Bad Loader signature");

        auto i = _set.find(key, Hash(), typename entry::key_eq());
        lw_shared_ptr<entry> e;
        future<> f = make_ready_future<>();
        if (i != _set.end()) {
            e = i->shared_from_this();
            f = e->loaded().get_shared_future();
        } else {
            Stats::inc_misses();
            e = make_lw_shared<entry>(*this, key);
            rehash_before_insert();
            _set.insert(*e);
            // get_shared_future() may throw, so make sure to call it before invoking the loader(key)
            f = e->loaded().get_shared_future();
            loader(key).then_wrapped([e] (future<value_type>&& f) mutable {
                if (f.failed()) {
                    e->loaded().set_exception(f.get_exception());
                } else {
                    e->set_value(f.get0());
                    e->loaded().set_value();
                }
            });
        }
        if (!f.available()) {
            Stats::inc_blocks();
            return f.then([e] () mutable {
                return entry_ptr(std::move(e));
            });
        } else if (f.failed()) {
            return make_exception_future<entry_ptr>(std::move(f).get_exception());
        } else {
            Stats::inc_hits();
            return make_ready_future<entry_ptr>(entry_ptr(std::move(e)));
        }
    }

    /// \brief Try to rehash the container so that the load factor is between 0.25 and 0.75.
    /// \throw May throw if allocation of a new buckets array throws.
    void rehash() {
        rehash<true>(_set.size());
    }

    size_t buckets_count() const {
        return _buckets.size();
    }

    size_t size() const {
        return _set.size();
    }

private:
    void rehash_before_insert() noexcept {
        try {
            rehash<false>(_set.size() + 1);
        } catch (...) {
            // if rehashing fails - continue with the current buckets array
        }
    }

    template <bool ShrinkingIsAllowed>
    void rehash(size_t new_size) {
        size_t new_buckets_count = 0;

        // Try to keep the load factor above 0.25 (when shrinking is allowed) and below 0.75.
        if (ShrinkingIsAllowed && new_size < buckets_count() / 4) {
            if (!new_size) {
                new_buckets_count = 1;
            } else {
                new_buckets_count = size_t(1) << log2floor(new_size * 4);
            }
        } else if (new_size > 3 * buckets_count() / 4) {
            new_buckets_count = buckets_count() * 2;
        }

        if (new_buckets_count < InitialBucketsCount) {
            return;
        }

        std::vector<typename set_type::bucket_type> new_buckets(new_buckets_count);
        _set.rehash(bi_set_bucket_traits(new_buckets.data(), new_buckets.size()));
        _buckets.swap(new_buckets);
    }
};

}

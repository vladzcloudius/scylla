/*
 * Copyright (C) 2017 ScyllaDB
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

#pragma once

#include "utils/loading_cache.hh"
#include "cql3/statements/prepared_statement.hh"

namespace cql3 {

typedef std::unique_ptr<statements::prepared_statement> prepared_cache_entry;

struct prepared_cache_entry_size {
    size_t operator()(const prepared_cache_entry& val) {
        // TODO: improve the size approximation
        return 1000;
    }
};

template<typename IdType, typename Stats>
class prepared_statements_cache {
private:
    typedef utils::loading_cache<IdType, prepared_cache_entry, false, prepared_cache_entry_size, std::hash<IdType>, std::equal_to<IdType>, Stats> cache_type;
    typedef typename cache_type::value_ptr cache_value_ptr;

    static constexpr int entry_expiry_ms = 120000; // 2 minutes

public:
    typedef statements::prepared_statement value_type;
    typedef typename value_type::checked_weak_ptr checked_weak_ptr;
    typedef typename cache_type::entry_is_too_big query_is_too_big;

private:
    cache_type _cache;

public:
    class iterator {
    private:
        typedef typename cache_type::iterator cache_iterator;
        cache_iterator _cache_it;

    public:
        iterator() = default;
        iterator(cache_iterator it) : _cache_it(std::move(it)) {}

        iterator& operator++() {
            ++_cache_it;
            return *this;
        }

        iterator operator++(int) {
            cache_iterator tmp(_cache_it);
            operator++();
            return tmp;
        }

        checked_weak_ptr operator*() {
            return (*_cache_it)->checked_weak_from_this();
        }

        checked_weak_ptr operator->() {
            return (*_cache_it)->checked_weak_from_this();
        }

        bool operator==(const iterator& other) {
            return _cache_it == other._cache_it;
        }

        bool operator!=(const iterator& other) {
            return !(*this == other);
        }
    };

    iterator end() {
        return _cache.end();
    }

    iterator begin() {
        _cache.begin();
    }

    prepared_statements_cache(logging::logger& logger)
        : _cache(memory::stats().total_memory() / 256, std::chrono::milliseconds(entry_expiry_ms), logger)
    {}

    template <typename LoadFunc>
    future<checked_weak_ptr> get(const IdType& key, LoadFunc&& load) {
        return _cache.get_ptr(key, std::forward<LoadFunc>(load)).then([] (cache_value_ptr v_ptr) {
            return make_ready_future<checked_weak_ptr>((*v_ptr)->checked_weak_from_this());
        });
    }

    iterator find(const IdType& key) {
        return _cache.find(key);
    }

    template <typename Pred>
    void remove_if(Pred&& pred) {
        _cache.remove_if([&pred] (const prepared_cache_entry& e) {
            return pred(e->statement);
        });
    }

    size_t entries_count() const {
        return _cache.entries_count();
    }

    size_t size() const {
        return _cache.size();
    }
};
}

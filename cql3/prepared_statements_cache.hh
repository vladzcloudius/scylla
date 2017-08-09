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

using prepared_cache_entry = std::unique_ptr<statements::prepared_statement>;

struct prepared_cache_entry_size {
    size_t operator()(const prepared_cache_entry& val) {
        // TODO: improve the size approximation
        return 10000;
    }
};

typedef bytes cql_prepared_id_type;
typedef int32_t thrift_prepared_id_type;

class prepared_cache_key_type {
public:
    using cache_key_type = std::pair<cql_prepared_id_type, int64_t>;

private:
    cache_key_type _key;

public:
    prepared_cache_key_type() = default;
    prepared_cache_key_type(cql_prepared_id_type cql_id) : _key(std::move(cql_id), std::numeric_limits<int64_t>::max()) {}
    prepared_cache_key_type(thrift_prepared_id_type thrift_id) : _key(cql_prepared_id_type(), thrift_id) {}

    cache_key_type& key() { return _key; }
    const cache_key_type& key() const { return _key; }

    static const cql_prepared_id_type& cql_id(const prepared_cache_key_type& key) {
        return key.key().first;
    }
    static thrift_prepared_id_type thrift_id(const prepared_cache_key_type& key) {
        return key.key().second;
    }
};

class prepared_statements_cache {
public:
    struct stats {
        uint64_t prepared_cache_evictions = 0;
    };

    static stats& shard_stats() {
        static thread_local stats _stats;
        return _stats;
    }

    struct prepared_cache_stats_updater {
        static void inc_hits() noexcept {}
        static void inc_misses() noexcept {}
        static void inc_blocks() noexcept {}
        static void inc_evictions() noexcept {
            ++shard_stats().prepared_cache_evictions;
        }
    };

private:
    using cache_key_type = typename prepared_cache_key_type::cache_key_type;
    using cache_type = utils::loading_cache<cache_key_type, prepared_cache_entry, utils::loading_cache_reload_enabled::no, prepared_cache_entry_size, utils::tuple_hash, std::equal_to<cache_key_type>, prepared_cache_stats_updater>;
    using cache_value_ptr = typename cache_type::value_ptr;

    static const std::chrono::minutes entry_expiry;

public:
    using value_type = statements::prepared_statement;
    using checked_weak_ptr = typename value_type::checked_weak_ptr;
    using statement_is_too_big = typename cache_type::entry_is_too_big;

private:
    cache_type _cache;

public:
    class iterator {
    private:
        using cache_iterator = typename cache_type::iterator;
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
        return _cache.begin();
    }

    prepared_statements_cache(logging::logger& logger)
        : _cache(memory::stats().total_memory() / 256, entry_expiry, logger)
    {}

    template <typename LoadFunc>
    future<checked_weak_ptr> get(const prepared_cache_key_type& key, LoadFunc&& load) {
        return _cache.get_ptr(key.key(), [load = std::forward<LoadFunc>(load)] (const cache_key_type&) { return load(); }).then([] (cache_value_ptr v_ptr) {
            return make_ready_future<checked_weak_ptr>((*v_ptr)->checked_weak_from_this());
        });
    }

    iterator find(const prepared_cache_key_type& key) {
        return _cache.find(key.key());
    }

    template <typename Pred>
    void remove_if(Pred&& pred) {
        static_assert(std::is_same<bool, std::result_of_t<Pred(::shared_ptr<cql_statement>)>>::value, "Bad Pred signature");

        _cache.remove_if([&pred] (const prepared_cache_entry& e) {
            return pred(e->statement);
        });
    }

    size_t size() const {
        return _cache.size();
    }

    size_t memory_footprint() const {
        return _cache.memory_footprint();
    }
};
}

namespace std { // for prepared_statements_cache log printouts
inline std::ostream& operator<<(std::ostream& os, const typename cql3::prepared_cache_key_type::cache_key_type& p) {
    os << "{cql_id: " << p.first << ", thrift_id: " << p.second << "}";
    return os;
}
}

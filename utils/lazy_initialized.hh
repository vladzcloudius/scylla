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

#include <type_traits>
#include <utility>

namespace utils {

template<typename T>
class lazy_initialized {
    bool _initialized;
    std::aligned_storage_t<sizeof(T)> _data;
public:
    ~lazy_initialized() {
        uninit();
    }

    template<typename... Args>
    void init(Args&& ... args) {
        new(&_data) T(std::forward<Args>(args)...);
        _initialized = true;
    }

    void uninit() {
        if (_initialized) {
            delete &get();
            _initialized = false;
        }
    }

    T* operator->() { return get(); }

    T& get() { return *reinterpret_cast<T*>(&_data); }

    explicit operator bool() const {
        return _initialized;
    }
};

}
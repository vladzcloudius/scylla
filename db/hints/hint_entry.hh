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

/*
 * Copyright (C) 2017 ScyllaDB
 */

#pragma once

#include "timestamp.hh"
#include "db/commitlog/commitlog_entry.hh"

class hint_entry {
private:
    api::timestamp_type _ts;
    api::timestamp_type _min_gc_gs;

public:
    commitlog_entry cl_entry;

public:
    hint_entry(api::timestamp_type ts, api::timestamp_type min_gc_gs, commitlog_entry cle)
        : _ts(ts)
        , _min_gc_gs(min_gc_gs)
        , cl_entry(std::move(cle))
    {}

    api::timestamp_type ts() const {
        return _ts;
    }

    api::timestamp_type min_gc_gs() const {
        return _min_gc_gs;
    }
};


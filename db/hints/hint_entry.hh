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

#include <seastar/core/lowres_clock.hh>
#include "timestamp.hh"
#include "db/commitlog/commitlog_entry.hh"

namespace db {
namespace hints {

typedef seastar::lowres_system_clock ts_clock;
// gc_grace_seconds clock type
typedef gc_clock gc_gs_clock;

}
}

class hint_entry {
private:
    db::hints::ts_clock::time_point _ts;
    db::hints::gc_gs_clock::time_point _min_gc_gs;

public:
    commitlog_entry cl_entry;

public:
    hint_entry(db::hints::ts_clock::time_point ts, db::hints::gc_gs_clock::time_point min_gc_gs, commitlog_entry cle)
        : _ts(ts)
        , _min_gc_gs(min_gc_gs)
        , cl_entry(std::move(cle))
    {}

    db::hints::ts_clock::time_point ts() const {
        return _ts;
    }

    db::hints::gc_gs_clock::time_point min_gc_gs() const {
        return _min_gc_gs;
    }
};


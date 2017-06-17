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

#include "mutation.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry_serializer_base.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/hints/hint_entry.hh"
#include "stdx.hh"

#include "idl/uuid.dist.hh"
#include "idl/keys.dist.hh"
#include "idl/frozen_mutation.dist.hh"
#include "idl/mutation.dist.hh"
#include "idl/commitlog.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/frozen_mutation.dist.impl.hh"
#include "idl/mutation.dist.impl.hh"
#include "idl/commitlog.dist.impl.hh"

namespace db {

namespace hints {

const commitlog::timeout_clock::duration max_write_duration_ms = std::chrono::milliseconds(2000);

using ts_clock = std::chrono::system_clock;
using gc_gs_clock = gc_clock;

struct serialize_one_hint_entry;

/// \class entry_writer
/// \brief This is a hint file writer.
class entry_writer : public commitlog_entry_writer_base<serialize_one_hint_entry> {
private:
    api::timestamp_type _ts;
    api::timestamp_type _min_gc_gs;

public:
    entry_writer(schema_ptr s, const frozen_mutation& fm)
        : commitlog_entry_writer_base(std::move(s), fm)
        , _ts(ts_clock::now().time_since_epoch().count())
        , _min_gc_gs((gc_gs_clock::now() + schema()->gc_grace_seconds()).time_since_epoch().count())
    {}

    virtual ~entry_writer() {}

    api::timestamp_type ts() const {
        return _ts;
    }

    api::timestamp_type min_gc_gs() const {
        return _min_gc_gs;
    }
};

/// \class serialize_one_hint_entry
/// \brief
/// This is a serializer for a hint file entry: it's similar to a commitlog entry
/// but it also adds two timestamps to each entry.
struct serialize_one_hint_entry {
    template <typename Output>
    static void do_serialize(Output& out, commitlog_entry_writer_base<serialize_one_hint_entry>* cl_e_wr) {
        entry_writer* h_e_wr = dynamic_cast<entry_writer*>(cl_e_wr);
        assert(h_e_wr);
        ser::writer_of_hint_entry<Output> idl_hint_writer(out);
        auto idl_commitlog_entry_writer = std::move(idl_hint_writer).write_ts(h_e_wr->ts()).write_min_gc_gs(h_e_wr->min_gc_gs()).start_cl_entry();
        write_commitlog_entry(std::move(idl_commitlog_entry_writer), h_e_wr->with_schema(), h_e_wr->schema(), h_e_wr->mutation()).end_cl_entry().end_hint_entry();
    }

    static size_t estimate_size(commitlog_entry_writer_base<serialize_one_hint_entry>* base_cl_writer) {
        return base_cl_writer->mutation().representation().size() + 2 * sizeof(api::timestamp_type);
    }
};

/// \struct entry_reader
/// \brief
/// Deserialize a single hint file entry.
struct entry_reader {
    static hint_entry deserialize(temporary_buffer<char> buffer) {
        seastar::simple_input_stream in(buffer.get(), buffer.size());
        return ser::deserialize(in, boost::type<hint_entry>());
    }
};

}
}

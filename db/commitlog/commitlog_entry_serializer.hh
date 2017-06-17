/*
 * Copyright 2017 ScyllaDB
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

#include "db/commitlog/commitlog_file_entry_serializer.hh"
#include "db/commitlog/commitlog_entry.hh"

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

/// \class serialize_one_commitlog_mutation
/// \brief This is a serializer for a classic commitlog entry.
struct serialize_one_commitlog_mutation {
    template <typename Output>
    static void do_serialize(Output& out, const commitlog_file_entry_writer<serialize_one_commitlog_mutation>* cl_e_wr) {
        write_commitlog_entry(ser::writer_of_commitlog_entry<Output>(out), cl_e_wr->with_schema(), cl_e_wr->schema(), cl_e_wr->mutation()).end_commitlog_entry();
    }

    static size_t estimate_size(const commitlog_file_entry_writer<serialize_one_commitlog_mutation>* cl_e_wr) {
        return cl_e_wr->mutation().representation().size();
    }
};

/// \class commitlog_mutation_writer
/// \brief A classic commitlog entry writer.
class commitlog_mutation_writer : public commitlog_file_entry_writer<serialize_one_commitlog_mutation> {
public:
    commitlog_mutation_writer (schema_ptr s, const frozen_mutation& fm) : commitlog_file_entry_writer(std::move(s), fm) {}
    virtual ~commitlog_mutation_writer () {}
};

/// \class commitlog_entry_reader
/// \brief A classic commitlog entry reader.
class commitlog_entry_reader {
    commitlog_entry _ce;
public:
    commitlog_entry_reader(const temporary_buffer<char>& buffer);

    const stdx::optional<column_mapping>& get_column_mapping() const { return _ce.mapping(); }
    const frozen_mutation& mutation() const { return _ce.mutation(); }
};
}

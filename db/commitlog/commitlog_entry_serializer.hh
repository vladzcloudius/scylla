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

#include "db/commitlog/commitlog_entry.hh"
#include "db/hints/hint_entry.hh"

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

struct entry_writer {
    virtual size_t exact_size() const = 0;
    // Returns segment-independent size of the entry. Must be <= than segment-dependant size.
    virtual size_t estimate_size() const = 0;
    virtual void write(data_output&) const = 0;
    virtual void set_with_schema(bool) {}
    virtual bool with_schema() const { return false; }
    virtual schema_ptr schema() const { return nullptr; }
};

/// \class commitlog_file_entry_writer
/// \brief
/// This class has a base functionality for the mutation commitlog writers.
/// It has a generic logic of encoding the schema information into the segment where and when required.
///
/// SerializerToOutputStream should be a class with the following static methods:
///    1) template <typename Output>
///       static void do_serialize(Output&, const commitlog_file_entry_writer<SerializerToOutputStream>*)
///
///       This method has to serialize the data of the given instance of commitlog_file_entry_writer
///       into output stream Output.
///
///    2) static size_t estimate_size(const commitlog_file_entry_writer<serialize_one_commitlog_entry>*)
///
///       This method returns an estimate for the entry size without the actual serialization.
///       The size of the serialized buffer should be greater or equal to the returned value but
///       the return value should be as close as possible to the serialized buffer size.
///
template <typename SerializerToOutputStream>
class commitlog_file_entry_writer : public entry_writer {
private:
    schema_ptr _schema;
    const frozen_mutation& _mutation;
    bool _with_schema = true;
    size_t _size = std::numeric_limits<size_t>::max();
private:
    template<typename Output>
    void serialize(Output&) const;
    void compute_size();
public:
    commitlog_file_entry_writer(schema_ptr s, const frozen_mutation& fm)
        : _schema(std::move(s)), _mutation(fm)
    {}

    virtual void set_with_schema(bool value) override {
        _with_schema = value;
        compute_exact_size();
    }

    virtual bool with_schema() const override {
        return _with_schema;
    }

    virtual schema_ptr schema() const override {
        return _schema;
    }

    virtual size_t exact_size() const override {
        assert(_size != std::numeric_limits<size_t>::max());
        return _size;
    }

    virtual size_t estimate_size() const override {
        return SerializerToOutputStream::estimate_size(this);
    }

    virtual void write(data_output& out) const override {
        seastar::simple_output_stream str(out.reserve(exact_size()), exact_size());
        SerializerToOutputStream::do_serialize(str, this);
    }

    const frozen_mutation& mutation() const {
        return _mutation;
    }

private:
    void compute_exact_size() {
        seastar::measuring_output_stream ms;
        SerializerToOutputStream::do_serialize(ms, this);
        _size = ms.size();
    }
};

/// Serialize (IDL) a single commitlog_entry
///
/// \tparam SerClWriter ser::XXX state for serializing a commitlog_entry object
/// \param wr instance of SerClWriter
/// \param with_schema TRUE if schema info should be encoded in this entry
/// \param s schema_ptr instance for the given mutation
/// \param fm mutation to encode
///
/// \return the ser::XXX state after the serialization of the commitlog_entry object
template<typename Writer>
auto write_commitlog_entry(Writer&& wr, bool with_schema, schema_ptr s, const frozen_mutation& fm) {
    return (with_schema ? std::forward<Writer>(wr).write_mapping(s->get_column_mapping()) : std::forward<Writer>(wr).skip_mapping())
           .write_mutation(fm);
}

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
class commitlog_entry_writer : public commitlog_file_entry_writer<serialize_one_commitlog_mutation> {
public:
    commitlog_entry_writer(schema_ptr s, const frozen_mutation& fm) : commitlog_file_entry_writer(std::move(s), fm) {}
    virtual ~commitlog_entry_writer() {}
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

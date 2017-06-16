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

struct i_commitlog_entry_writer {
    virtual size_t size(commitlog::segment&) = 0;
    // Returns segment-independent size of the entry. Must be <= than segment-dependant size.
    virtual size_t size() = 0;
    virtual void write(commitlog::segment&, commitlog::output&) = 0;
};

/// \class commitlog_entry_writer_base
/// \brief
/// This class has a base functionality for the mutation commitlog writers.
/// It has a generic logic of encoding the schema information into the segment where and when required.
///
/// SerializerToOutputStream should be a class with the following static methods:
///    1) template <typename Output>
///       static void do_serialize(Output&, commitlog_entry_writer_base<SerializerToOutputStream>*)
///
///       This method has to serialize the data of the given instance of commitlog_entry_writer_base
///       into output stream Output.
///
///    2) static size_t estimate_size(commitlog_entry_writer_base<serialize_one_commitlog_entry>*)
///
///       This method returns an estimate for the entry size without the actual serialization.
///       The size of the serialized buffer should be greater or equal to the returned value but
///       the return value should be as close as possible to the serialized buffer size.
///
template <typename SerializerToOutputStream>
class commitlog_entry_writer_base : public db::i_commitlog_entry_writer {
private:
    schema_ptr _schema;
    const frozen_mutation& _mutation;
    bool _with_schema = true;
    size_t _size = std::numeric_limits<size_t>::max();

public:
    bool with_schema() {
        return _with_schema;
    }

    schema_ptr schema() const {
        return _schema;
    }

    const frozen_mutation& mutation() const {
        return _mutation;
    }

protected:
    commitlog_entry_writer_base(schema_ptr s, const frozen_mutation& fm)
        : _schema(std::move(s))
        , _mutation(fm)
    {}

private:
    void set_with_schema(bool value) {
        _with_schema = value;
        compute_exact_size();
    }

    size_t exact_size() {
        assert(_size != std::numeric_limits<size_t>::max());
        return _size;
    }

    void compute_exact_size() {
        seastar::measuring_output_stream ms;
        SerializerToOutputStream::do_serialize(ms, this);
        _size = ms.size();
    }

    void write(data_output& out) {
        seastar::simple_output_stream str(out.reserve(exact_size()), exact_size());
        SerializerToOutputStream::do_serialize(str, this);
    }

public:
    virtual size_t size(db::commitlog::segment& seg) override  {
        set_with_schema(!seg.is_schema_version_known(schema()));
        return exact_size();
    }

    virtual void write(db::commitlog::segment& seg, db::commitlog::output& out) override {
        if (with_schema()) {
            seg.add_schema_version(schema());
        }
        write(out);
    }

    virtual size_t size() override {
        return SerializerToOutputStream::estimate_size(this);
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
template<typename SerClWriter>
auto write_commitlog_entry(SerClWriter&& wr, bool with_schema, schema_ptr s, const frozen_mutation& fm) {
    return (with_schema ? std::forward<SerClWriter>(wr).write_mapping(s->get_column_mapping()) : std::forward<SerClWriter>(wr).skip_mapping())
           .write_mutation(fm);
}

/// \class serialize_one_commitlog_entry
/// \brief This is a serializer for a classic commitlog entry.
struct serialize_one_commitlog_entry {
    template <typename Output>
    static void do_serialize(Output& out, commitlog_entry_writer_base<serialize_one_commitlog_entry>* cl_e_wr) {
        write_commitlog_entry(ser::writer_of_commitlog_entry<Output>(out), cl_e_wr->with_schema(), cl_e_wr->schema(), cl_e_wr->mutation()).end_commitlog_entry();
    }

    static size_t estimate_size(commitlog_entry_writer_base<serialize_one_commitlog_entry>* cl_e_wr) {
        return cl_e_wr->mutation().representation().size();
    }
};

/// \class commitlog_entry_writer
/// \brief A classic commitlog entry writer.
class commitlog_entry_writer : public commitlog_entry_writer_base<serialize_one_commitlog_entry> {
public:
    commitlog_entry_writer(schema_ptr s, const frozen_mutation& fm) : commitlog_entry_writer_base(std::move(s), fm) {}
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

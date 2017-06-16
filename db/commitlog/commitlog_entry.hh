/*
 * Copyright 2016 ScyllaDB
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

#include <experimental/optional>

#include "frozen_mutation.hh"
#include "schema.hh"
#include "utils/data_output.hh"
#include "stdx.hh"

class commitlog_entry {
    stdx::optional<column_mapping> _mapping;
    frozen_mutation _mutation;
public:
    commitlog_entry(stdx::optional<column_mapping> mapping, frozen_mutation&& mutation)
        : _mapping(std::move(mapping)), _mutation(std::move(mutation)) { }
    const stdx::optional<column_mapping>& mapping() const { return _mapping; }
    const frozen_mutation& mutation() const { return _mutation; }
};

namespace db {

struct entry_writer {
    virtual size_t size(commitlog::segment&) = 0;
    // Returns segment-independent size of the entry. Must be <= than segment-dependant size.
    virtual size_t size() = 0;
    virtual void write(commitlog::segment&, commitlog::output&) = 0;
};

class commitlog_entry_writer : public entry_writer {
    schema_ptr _schema;
    const frozen_mutation& _mutation;
    bool _with_schema = true;
    size_t _size = std::numeric_limits<size_t>::max();
private:
    template<typename Output>
    void serialize(Output&) const;
    void compute_size();
public:
    commitlog_entry_writer(schema_ptr s, const frozen_mutation& fm)
        : _schema(std::move(s)), _mutation(fm)
    {}

    void set_with_schema(bool value) {
        _with_schema = value;
        compute_size();
    }

    bool with_schema() {
        return _with_schema;
    }

    schema_ptr schema() const {
        return _schema;
    }

    size_t exact_size() const {
        assert(_size != std::numeric_limits<size_t>::max());
        return _size;
    }

    size_t estimate_size() const {
        return _mutation.representation().size();
    }

    void write(data_output& out) const;

public:
    virtual size_t size(commitlog::segment& seg) override;
    virtual size_t size() override;
    virtual void write(commitlog::segment& seg, commitlog::output& out) override;
};

template<typename Writer>
auto write_commitlog_entry(Writer&& wr, bool with_schema, schema_ptr s, const frozen_mutation& fm) {
    return (with_schema ? std::forward<Writer>(wr).write_mapping(s->get_column_mapping()) : std::forward<Writer>(wr).skip_mapping())
           .write_mutation(fm);
}

class commitlog_entry_reader {
    commitlog_entry _ce;
public:
    commitlog_entry_reader(const temporary_buffer<char>& buffer);

    const stdx::optional<column_mapping>& get_column_mapping() const { return _ce.mapping(); }
    const frozen_mutation& mutation() const { return _ce.mutation(); }
};
}

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

    virtual void set_with_schema(bool value) override {
        _with_schema = value;
        compute_size();
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
        return _mutation.representation().size();
    }

    virtual void write(data_output& out) const override;
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

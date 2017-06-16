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


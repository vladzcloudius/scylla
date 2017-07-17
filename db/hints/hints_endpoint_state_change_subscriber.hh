/*
 * Modified by ScyllaDB
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

#include "gms/i_endpoint_state_change_subscriber.hh"
#include "message/messaging_service.hh"

namespace db {
namespace hints {

// @note all callbacks should be called in seastar::async() context
class hints_endpoint_state_change_subscriber : public gms::i_endpoint_state_change_subscriber {
private:
    manager& _local_manager;

public:
    hints_endpoint_state_change_subscriber(manager& local_manager)
        : _local_manager(local_manager)
    {}

    void before_change(gms::inet_address endpoint, gms::endpoint_state cs, gms::application_state new_state_key,
                       const gms::versioned_value& new_value) override {
        // do nothing.
    }

    void on_join(gms::inet_address endpoint, gms::endpoint_state ep_state) override {
        auto internal_ip_state_opt = ep_state.get_application_state(gms::application_state::INTERNAL_IP);

        if (internal_ip_state_opt) {
            reconnect(endpoint, *internal_ip_state_opt);
        }
    }

    void
    on_change(gms::inet_address endpoint, gms::application_state state, const gms::versioned_value& value) override {
        if (state == gms::application_state::INTERNAL_IP) {
            reconnect(endpoint, value);
        }
    }

    void on_alive(gms::inet_address endpoint, gms::endpoint_state ep_state) override {
        auto internal_ip_state_opt = ep_state.get_application_state(gms::application_state::INTERNAL_IP);

        if (internal_ip_state_opt) {
            reconnect(endpoint, *internal_ip_state_opt);
        }
    }

    void on_dead(gms::inet_address endpoint, gms::endpoint_state ep_state) override {
        // do nothing.
    }

    void on_remove(gms::inet_address endpoint) override {
        // do nothing.
    }

    void on_restart(gms::inet_address endpoint, gms::endpoint_state state) override {
        // do nothing.
    }
};

}
}

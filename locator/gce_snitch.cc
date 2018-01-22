/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
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
 * Modified by ScyllaDB
 * Copyright (C) 2018 ScyllaDB
 */

#include <seastar/net/dns.hh>
#include "locator/gce_snitch.hh"

namespace locator {

class bad_gce_metadata_format : public std::exception {
private:
    sstring _msg;
public:
    explicit bad_gce_metadata_format(sstring what_arg) : _msg(std::move(what_arg)) {}
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

gce_snitch::gce_snitch(const sstring& fname, unsigned io_cpuid) : production_snitch_base(fname) {
    if (engine().cpu_id() == io_cpuid) {
        io_cpu_id() = io_cpuid;
    }
}

/**
 * Read GCE and property file configuration and distribute it among other shards
 *
 * @return
 */
future<> gce_snitch::load_config() {
    using namespace boost::algorithm;

    if (engine().cpu_id() == io_cpu_id()) {
        return gce_api_call(GCE_QUERY_SERVER_ADDR, ZONE_NAME_QUERY_REQ).then([this](sstring az){
            assert(az.size());

            std::vector<std::string> splits;

            // Split "us-central1-a" or "asia-east1-a" into "us-central1"/"a" and "asia-east1"/"a".
            split(splits, az, is_any_of("-"));
            if (splits.size() <= 1) {
                throw bad_gce_metadata_format(seastar::format("Bad GCE zone format: {}", az));
            }

            _my_rack = splits[splits.size() - 1];
            _my_dc = az.substr(0, az.size() - 1 - _my_rack.size());

            return read_property_file().then([this] (sstring datacenter_suffix) {
                _my_dc += datacenter_suffix;
                logger().info("GCESnitch using region: {}, zone: {}.", _my_dc, _my_rack);

                return _my_distributed->invoke_on_all(
                    [this] (snitch_ptr& local_s) {

                    // Distribute the new values on all CPUs but the current one
                    if (engine().cpu_id() != io_cpu_id()) {
                        local_s->set_my_dc(_my_dc);
                        local_s->set_my_rack(_my_rack);
                    }
                });
            });
        });
    }

    return make_ready_future<>();
}

future<> gce_snitch::start() {
    _state = snitch_state::initializing;

    return load_config().then([this] {
        set_snitch_ready();
    });
}

future<sstring> gce_snitch::gce_api_call(sstring addr, sstring cmd) {
    return seastar::net::dns::resolve_name(addr, net::inet_address::family::INET).then([this, cmd] (net::inet_address a) {
        return connect(make_ipv4_address(ipv4_addr(a, 80))).then([this, cmd] (connected_socket fd) {
            _sd = std::move(fd);
            _in = std::move(_sd.input());
            _out = std::move(_sd.output());
            _zone_req = sstring("GET ") + cmd +
                        sstring(" HTTP/1.1\r\nHost: metadata\r\n") +
                        sstring("Metadata-Flavor: Google") +
                        sstring("\r\n\r\n");

            return _out.write(_zone_req.c_str()).then([this] {
                return _out.flush();
            });
        }).then([this] {
            _parser.init();
            return _in.consume(_parser).then([this] {
                if (_parser.eof()) {
                    return make_exception_future<sstring>("Bad HTTP response");
                }

                // Read HTTP response header first
                auto _rsp = _parser.get_parsed_response();
                auto it = _rsp->_headers.find("Content-Length");
                if (it == _rsp->_headers.end()) {
                    return make_exception_future<sstring>("Error: HTTP response does not contain: Content-Length\n");
                }

                auto content_len = std::stoi(it->second);

                // Read HTTP response body
                return _in.read_exactly(content_len).then([this] (temporary_buffer<char> buf) {
                    using namespace boost::algorithm;
                    sstring res(buf.get(), buf.size());
                    std::vector<std::string> splits;
                    split(splits, res, is_any_of("/"));
                    return make_ready_future<sstring>(std::move(splits[splits.size() - 1]));
                });
            });
        });
    });

}

future<sstring> gce_snitch::read_property_file() {
    return load_property_file().then([this] {
        sstring dc_suffix;

        if (_prop_values.count(dc_suffix_property_key)) {
            dc_suffix = _prop_values[dc_suffix_property_key];
        }

        return dc_suffix;
    });
}

using registry_2_params = class_registrator<i_endpoint_snitch, gce_snitch, const sstring&, unsigned>;
static registry_2_params registrator2("org.apache.cassandra.locator.GoogleCloudSnitch");
static registry_2_params registrator2_short_name("GoogleCloudSnitch");


using registry_1_param = class_registrator<i_endpoint_snitch, gce_snitch, const sstring&>;
static registry_1_param registrator1("org.apache.cassandra.locator.GoogleCloudSnitch");
static registry_1_param registrator1_short_name("GoogleCloudSnitch");

using registry_default = class_registrator<i_endpoint_snitch, gce_snitch>;
static registry_default registrator_default("org.apache.cassandra.locator.GoogleCloudSnitch");
static registry_default registrator_default_short_name("GoogleCloudSnitch");

} // namespace locator

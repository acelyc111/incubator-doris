// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/action/mem_tracker_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <string>
#include <sstream>
#include <vector>

#include "common/configbase.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "runtime/mem_tracker.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

void MemTrackerAction::handle(HttpRequest* req) {
    std::ostringstream output;
    output << "<h1>Memory usage by subsystem</h1>\n";
    output << "<table data-toggle='table' "
              "       data-pagination='true' "
              "       data-search='true' "
              "       class='table table-striped'>\n";
    output << "<thead><tr>"
              "<th>Id</th>"
              "<th>Parent</th>"
              "<th>Limit</th>"
              "<th data-sorter='bytesSorter' "
              "    data-sortable='true' "
              ">Current Consumption</th>"
              "<th data-sorter='bytesSorter' "
              "    data-sortable='true' "
              ">Peak Consumption</th>";
    output << "<tbody>\n";

    std::vector<MemTracker*> trackers;
    MemTracker::get_all_trackers_under_root(&trackers);
    for (const auto& tracker : trackers) {
        string parent = tracker->parent() == nullptr ? "none" : tracker->parent()->label();
        string limit_str = tracker->limit() == -1 ? "none" : std::to_string(tracker->limit());
        string current_consumption_str = std::to_string(tracker->consumption());
        string peak_consumption_str = std::to_string(tracker->peak_consumption());
        output << strings::Substitute("<tr><td>$0</td><td>$1</td><td>$2</td>" // id, parent, limit
                                      "<td>$3</td><td>$4</td></tr>\n", // current, peak
                                      tracker->label(), parent, limit_str, current_consumption_str,
                                      peak_consumption_str);
    }
    output << "</tbody></table>\n";

    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, output.str());
}

} // namespace doris

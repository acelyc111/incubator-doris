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

#include "http/default_path_handlers.h"

#include <gperftools/malloc_extension.h>
#include <sys/stat.h>

#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <fstream>
#include <sstream>

#include "common/configbase.h"
#include "common/logging.h"
#include "gutil/strings/human_readable.h"
#include "http/web_page_handler.h"
#include "runtime/mem_tracker.h"
#include "util/debug_util.h"
#include "util/logging.h"
#include "util/pretty_printer.h"

using std::vector;
using std::shared_ptr;
using std::string;

namespace doris {

// Writes the last config::web_log_bytes of the INFO logfile to a webpage
// Note to get best performance, set GLOG_logbuflevel=-1 to prevent log buffering
void logs_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    /*std::string logfile;
    get_full_log_filename(google::INFO, &logfile);
    (*output) << "<h2>INFO logs</h2>" << std::endl;
    (*output) << "Log path is: " << logfile << std::endl;

    struct stat file_stat;

    if (stat(logfile.c_str(), &file_stat) == 0) {
        long size = file_stat.st_size;
        long seekpos = size < config::web_log_bytes ? 0L : size - config::web_log_bytes;
        std::ifstream log(logfile.c_str(), std::ios::in);
        // Note if the file rolls between stat and seek, this could fail
        // (and we could wind up reading the whole file). But because the
        // file is likely to be small, this is unlikely to be an issue in
        // practice.
        log.seekg(seekpos);
        (*output) << "<br/>Showing last " << config::web_log_bytes << " bytes of log" << std::endl;
        (*output) << "<br/><pre>" << log.rdbuf() << "</pre>";

    } else {
        (*output) << "<br/>Couldn't open INFO log file: " << logfile;
    }*/

    (*output) << "<br/>Couldn't open INFO log file: ";
}

// Registered to handle "/varz", and prints out all command-line flags and their values
void config_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "<h2>Configurations</h2>";
    (*output) << "<pre>";
    for (const auto& it : *(config::full_conf_map)) {
        (*output) << it.first << "=" << it.second << std::endl;
    }
    (*output) << "</pre>";
}

// Registered to handle "/memz", and prints out memory allocation statistics.
void mem_usage_handler(std::shared_ptr<MemTracker> mem_tracker, const WebPageHandler::ArgumentMap& args,
                       std::stringstream* output) {
    if (mem_tracker != nullptr) {
        (*output) << "<pre>"
                  << "Mem Limit: " << PrettyPrinter::print(mem_tracker->limit(), TUnit::BYTES)
                  << std::endl
                  << "Mem Consumption: "
                  << PrettyPrinter::print(mem_tracker->consumption(), TUnit::BYTES) << std::endl
                  << "</pre>";
    } else {
        (*output) << "<pre>"
                  << "No process memory limit set."
                  << "</pre>";
    }

    (*output) << "<pre>";
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    (*output) << "Memory tracking is not available with address sanitizer builds.";
#else
    char buf[2048];
    MallocExtension::instance()->GetStats(buf, 2048);
    // Replace new lines with <br> for html
    std::string tmp(buf);
    boost::replace_all(tmp, "\n", "<br>");
    (*output) << tmp << "</pre>";
#endif
}

// Registered to handle "/mem-trackers", and prints out memory tracker information.
static void MemTrackersHandler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    *output << "<h1>Memory usage by subsystem</h1>\n";
    *output << "<table data-toggle='table' "
               "       data-pagination='true' "
               "       data-search='true' "
               "       class='table table-striped'>\n";
    *output << "<thead><tr>"
               "<th>Id</th>"
               "<th>Parent</th>"
               "<th>Limit</th>"
               "<th data-sorter='bytesSorter' "
               "    data-sortable='true' "
               ">Current Consumption</th>"
               "<th data-sorter='bytesSorter' "
               "    data-sortable='true' "
               ">Peak Consumption</th>";
    *output << "<tbody>\n";

    vector<shared_ptr<MemTracker>> trackers;
    MemTracker::ListTrackers(&trackers);
    for (const shared_ptr<MemTracker>& tracker : trackers) {
        string parent = tracker->parent() == nullptr ? "none" : tracker->parent()->id();
        string limit_str = tracker->limit() == -1 ? "none" :
                           HumanReadableNumBytes::ToString(tracker->limit());
        string current_consumption_str = HumanReadableNumBytes::ToString(tracker->consumption());
        string peak_consumption_str = HumanReadableNumBytes::ToString(tracker->peak_consumption());
        (*output) << Substitute("<tr><td>$0</td><td>$1</td><td>$2</td>" // id, parent, limit
                                "<td>$3</td><td>$4</td></tr>\n", // current, peak
                                tracker->id(), parent, limit_str, current_consumption_str,
                                peak_consumption_str);
    }
    *output << "</tbody></table>\n";
}

void add_default_path_handlers(WebPageHandler* web_page_handler, std::shared_ptr<MemTracker> process_mem_tracker) {
    web_page_handler->register_page("/logs", "Logs", logs_handler, true /* is_on_nav_bar */);
    web_page_handler->register_page("/varz", "Configs", config_handler, true /* is_on_nav_bar */);
    web_page_handler->register_page("/memz", "Memory (total)",
        boost::bind<void>(&mem_usage_handler, process_mem_tracker, _1, _2), true /* is_on_nav_bar */);
    web_page_handler->register_page("/mem-trackers", "Memory (detail)", MemTrackersHandler, true /* is_on_nav_bar */);
}

} // namespace doris

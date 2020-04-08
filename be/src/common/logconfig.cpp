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

#include "util/logging.h"

#include <iostream>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <mutex>
#include <glog/logging.h>
#include <glog/vlog_is_on.h>
#include "common/config.h"

namespace doris {

static bool logging_initialized = false;

static std::mutex logging_mutex;

static bool iequals(const std::string& a, const std::string& b)
{
    unsigned int sz = a.size();
    if (b.size() != sz) {
        return false;
    }
    for (unsigned int i = 0; i < sz; ++i) {
        if (tolower(a[i]) != tolower(b[i])) {
            return false;
        }
    }
    return true;
}       

bool init_glog(const char* basename, bool install_signal_handler) {

    std::lock_guard<std::mutex> logging_lock(logging_mutex);

    if (logging_initialized) {
        return true;
    }

    if (install_signal_handler) {
        google::InstallFailureSignalHandler();
    }

    // don't log to stderr
    FLAGS_stderrthreshold = 5;
    // set glog log dir
    FLAGS_log_dir = config::sys_log_dir;
    // 0 means buffer INFO only
    FLAGS_logbuflevel = 0;
    // buffer log messages for at most this many seconds
    FLAGS_logbufsecs = 30;  
    // set roll num
    FLAGS_log_filenum_quota = config::sys_log_roll_num;
    
    // set log level
    if (!convert_log_level(config::sys_log_level, &FLAGS_minloglevel)) {
        std::cerr << "sys_log_level needs to be INFO, WARNING, ERROR or FATAL" << std::endl;
        return false;
    }

    // set log buffer level
    // defalut is 0
    std::string& logbuflevel = config::log_buffer_level;
    if (iequals(logbuflevel, "-1")) {
        FLAGS_logbuflevel = -1;
    } else if (iequals(logbuflevel, "0")) {
        FLAGS_logbuflevel = 0;
    }

    // set log roll mode
    std::string& rollmode = config::sys_log_roll_mode;
    std::string sizeflag = "SIZE-MB-";
    bool ok = false;
    if (rollmode.compare("TIME-DAY") == 0) {
        FLAGS_log_split_method = "day";
        ok = true;
    } else if (rollmode.compare("TIME-HOUR") == 0) {
        FLAGS_log_split_method = "hour";
        ok = true;
    } else if (rollmode.substr(0, sizeflag.length()).compare(sizeflag) == 0) {
        FLAGS_log_split_method = "size";
        std::string sizestr = rollmode.substr(sizeflag.size(), rollmode.size() - sizeflag.size());
        if (sizestr.size() != 0)  {
            char* end = NULL;
            errno = 0;
            const char* sizecstr = sizestr.c_str();
            int64_t ret64 = strtoll(sizecstr, &end, 10);
            if ((errno == 0) && (end == sizecstr + strlen(sizecstr))) {
                int32_t retval = static_cast<int32_t>(ret64);
                if (retval == ret64) {
                    FLAGS_max_log_size = retval;
                    ok = true;
                }
            }
        }
    } else {
        ok = false;
    }
    if (!ok) {
        std::cerr << "sys_log_roll_mode needs to be TIME-DAY, TIME-HOUR, SIZE-MB-nnn" << std::endl;
        return false;
    }

    // set verbose modules.
    FLAGS_v = -1;
    update_modules_log_level(config::sys_log_verbose_modules, config::sys_log_verbose_level);

    google::InitGoogleLogging(basename);

    logging_initialized = true;
 
    return true;
}

void shutdown_logging() {
    std::lock_guard<std::mutex> logging_lock(logging_mutex);
    google::ShutdownGoogleLogging();
}

bool convert_log_level(const std::string& str, int32_t* level) {
    if (iequals(str, "INFO")) {
        *level = 0;
        return true;
    } else if (iequals(str, "WARNING")) {
        *level = 1;
        return true;
    } else if (iequals(str, "ERROR")) {
        *level = 2;
        return true;
    } else if (iequals(str, "FATAL")) {
        *level = 3;
        return true;
    } else {
        return false;
    }
}

void update_modules_log_level(const std::vector<std::string>& modules, int32_t level) {
    for (const auto& module : modules) {
        if (!module.empty()) {
            google::SetVLOGLevel(module.c_str(), level);
        }
    }
}

} // namespace doris

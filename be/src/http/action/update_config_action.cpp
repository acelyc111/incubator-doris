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

#include "http/action/update_config_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <mutex>
#include <string>

#include "common/config.h"
#include "common/configbase.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "util/logging.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

void UpdateConfigAction::handle(HttpRequest* req) {
    LOG(INFO) << req->debug_string();

    Status s = _update_config(*(req->params()));
    std::string status(s.ok() ? "OK" : "BAD");
    std::string msg = s.to_string();

    rapidjson::Document root;
    root.SetObject();
    root.AddMember("status", rapidjson::Value(status.c_str(), status.size()), root.GetAllocator());
    root.AddMember("msg", rapidjson::Value(msg.c_str(), msg.size()), root.GetAllocator());
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);

    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}

Status UpdateConfigAction::_update_config(const std::map<std::string, std::string>& params) {
    if (params.size() != 1) {
        return Status::InvalidArgument("now only support to set a single config once");
    }

    DCHECK(params.size() == 1);
    const std::string& config = params.begin()->first;
    const std::string& new_value = params.begin()->second;
    Status s;
    if (config == "sys_log_level" || config == "sys_log_verbose_modules" ||
        config == "sys_log_verbose_level") {
        // Update glog configs.
        s = _update_log_config(config, new_value);
    } else {
        // The other configs can be updated by config::set_config directly.
        s = config::set_config(config, new_value);
    }
    if (s.ok()) {
        LOG(INFO) << "update config " << config << "=" << new_value << " success";
    } else {
        LOG(WARNING) << "update config " << config << "=" << new_value
                     << " failed, reason: " << s.to_string();
    }
    return s;
}

Status UpdateConfigAction::_update_log_config(const std::string& config,
                                              const std::string& new_value) {
    std::lock_guard<SpinLock> l(_lock);
    if (config == "sys_log_level") {
        int32_t new_level = 0;
        if (!convert_log_level(new_value, &new_level)) {
            return Status::InvalidArgument(
                    "invalid sys_log_level, valid value should be INFO, WARNING, ERROR or FATAL");
        }
        string result =
                google::SetCommandLineOption("minloglevel", std::to_string(new_level).c_str());
        DCHECK(!result.empty()); // result is not empty when SetCommandLineOption success.
        Status s = config::set_config(config, new_value);
        DCHECK(s.ok());
        return s;
    } else if (config == "sys_log_verbose_modules" || config == "sys_log_verbose_level") {
        update_modules_log_level(config::sys_log_verbose_modules, config::sys_log_verbose_level);
        Status s = config::set_config(config, new_value);
        DCHECK(s.ok());
        return s;
    }

    return Status::NotSupported(strings::Substitute("not support to update config $0", config));
}

} // namespace doris

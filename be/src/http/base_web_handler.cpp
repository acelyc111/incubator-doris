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

#include "http/base_web_handler.h"

namespace doris {

// Do a simple decision, only deal a few type
std::string BaseWebHandler::get_content_type(const std::string& file_name) {
    std::string file_ext = path_util::file_extension(file_name);
    LOG(INFO) << "file_name: " << file_name << "; file extension: [" << file_ext << "]";
    if (file_ext == std::string(".html")
        || file_ext == std::string(".htm")) {
        return std::string("text/html; charset=utf-8");
    } else if (file_ext == std::string(".js")) {
        return std::string("application/javascript; charset=utf-8");
    } else if (file_ext == std::string(".css")) {
        return std::string("text/css; charset=utf-8");
    } else if (file_ext == std::string(".txt")) {
        return std::string("text/plain; charset=utf-8");
    } else {
        return "text/plain; charset=utf-8";
    }
    return "";
}

void BaseWebHandler::do_file_response(const std::string& file_path, HttpRequest *req) {
    // read file content and send response
    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(WARNING) << "Failed to open file: " << file_path;
        HttpChannel::send_error(req, HttpStatus::NOT_FOUND);
        return;
    }
    struct stat st;
    auto res = fstat(fd, &st);
    if (res < 0) {
        close(fd);
        LOG(WARNING) << "Failed to open file: " << file_path;
        HttpChannel::send_error(req, HttpStatus::NOT_FOUND);
        return;
    }

    int64_t file_size = st.st_size;

    // TODO(lingbin): process "IF_MODIFIED_SINCE" header
    // TODO(lingbin): process "RANGE" header
    const std::string& range_header = req->header(HttpHeaders::RANGE);
    if (!range_header.empty()) {
        // analyse range header
    }

    req->add_output_header(HttpHeaders::CONTENT_TYPE, get_content_type(file_path).c_str());

    if (req->method() == HttpMethod::HEAD) {
        close(fd);
        req->add_output_header(HttpHeaders::CONTENT_LENGTH, std::to_string(file_size).c_str());
        HttpChannel::send_reply(req);
        return;
    }

    HttpChannel::send_file(req, fd, 0, file_size);
}

} // namespace doris

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

#pragma once

#include "http/http_handler.h"

namespace doris {

// Update BE config.
class UpdateConfigAction : public HttpHandler {
public:
    UpdateConfigAction() {}
    virtual ~UpdateConfigAction() {}

    void handle(HttpRequest* req) override;

private:
    Status _update_config(const std::map<std::string, std::string>& params);
    Status _update_log_config(const std::string& config, const std::string& new_value);

    // Protect non thread-safe configs not to update concurrently.
    SpinLock _lock;
};

} // namespace doris

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

#include "http/web_page_handler.h"

#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>

#include <mustache.h>

#include "common/config.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/easy_json.h"
#include "util/mem_info.h"

namespace doris {

static std::string s_html_content_type = "text/html";

WebPageHandler::WebPageHandler(EvHttpServer* server) : _http_server(server) {
    PageHandlerCallback default_callback =
            boost::bind<void>(boost::mem_fn(&WebPageHandler::root_handler), this, _1, _2);
    register_page("/", default_callback, false /* is_on_nav_bar */);
}

WebPageHandler::~WebPageHandler() {
    STLDeleteValues(&_page_map);
}

void WebPageHandler::register_page(const std::string& path, const PageHandlerCallback& callback, bool is_on_nav_bar) {
    boost::mutex::scoped_lock lock(_map_lock);
    auto map_iter = _page_map.find(path);
    if (map_iter == _page_map.end()) {
        // first time, register this to web server
        _http_server->register_handler(HttpMethod::GET, path, this);
        _page_map.insert[path] = new PathHandler(true /* is_styled */, is_on_nav_bar, "", callback);
    }
}

void WebPageHandler::handle(HttpRequest* req) {
    LOG(INFO) << req->debug_string();

    // Should we render with css styles?
    bool use_style = true;
    const auto& params = *req->params();
    if (params.find("raw") != params.end()) {
        use_style = false;
    }

    std::stringstream content;
    {
        boost::mutex::scoped_lock lock(_map_lock);
        auto iter = _page_map.find(req->raw_path());
        if (iter != _page_map.end()) {
            iter->second->callback()(*req->params(), &content);
        }
    }

    std::string output;
    if (use_style) {
        std::stringstream oss;
        RenderMainTemplate(content.str(), &oss);
        output = oss.str();
    } else {
        output = content.str();
    }

    req->add_output_header(HttpHeaders::CONTENT_TYPE, s_html_content_type.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, output);
#if 0
    HttpResponse response(HttpStatus::OK, s_html_content_type, &str);
    channel->send_response(response);
#endif
}

void WebPageHandler::root_handler(const ArgumentMap& args, std::stringstream* output) {
    (*output) << "<h2>Version</h2>";
    (*output) << "<pre>" << get_version_string(false) << "</pre>" << std::endl;
    (*output) << "<h2>Hardware Info</h2>";
    (*output) << "<pre>";
    (*output) << CpuInfo::debug_string();
    (*output) << MemInfo::debug_string();
    (*output) << DiskInfo::debug_string();
    (*output) << "</pre>";
}

static const char* const kMainTemplate = R"(
<!DOCTYPE html>
<html>
  <head>
    <title>Doris</title>
    <meta charset='utf-8'/>
    <link href='/bootstrap/css/bootstrap.min.css' rel='stylesheet' media='screen' />
    <link href='/bootstrap/css/bootstrap-table.min.css' rel='stylesheet' media='screen' />
    <script src='/jquery-3.2.1.min.js' defer></script>
    <script src='/bootstrap/js/bootstrap.min.js' defer></script>
    <script src='/bootstrap/js/bootstrap-table.min.js' defer></script>
    <script src='/kudu.js' defer></script>
    <link href='/kudu.css' rel='stylesheet' />
  </head>
  <body>

    <nav class="navbar navbar-default">
      <div class="container-fluid">
        <div class="navbar-header">
          <a class="navbar-brand" style="padding-top: 5px;" href="/">
            <img src="/doris-logo.png" width='61' height='45' alt="Doris" />
          </a>
        </div>
        <div id="navbar" class="navbar-collapse collapse">
          <ul class="nav navbar-nav">
           {{#path_handlers}}
            <li><a class="nav-link"href="{{path}}">{{alias}}</a></li>
           {{/path_handlers}}
          </ul>
        </div><!--/.nav-collapse -->
      </div><!--/.container-fluid -->
    </nav>
      {{^static_pages_available}}
      <div style="color: red">
        <strong>Static pages not available. Configure DORIS_HOME to fix page styling.</strong>
      </div>
      {{/static_pages_available}}
      {{{content}}}
    </div>
    {{#footer_html}}
    <footer class="footer"><div class="container text-muted">
      {{{.}}}
    </div></footer>
    {{/footer_html}}
  </body>
</html>
)";

void WebPageHandler::RenderMainTemplate(const std::string& content, std::stringstream* output) {
    static const std::string& footer = std::string("Version") + get_version_string(true);

    EasyJson ej;
    ej["static_pages_available"] = true;  // TODO(yingchun): static_pages_available();
    ej["content"] = content;
    ej["footer_html"] = footer;
    EasyJson path_handlers = ej.Set("path_handlers", EasyJson::kArray);
    for (const auto& handler : _page_map) {
        if (handler.second->is_on_nav_bar()) {
            EasyJson path_handler = path_handlers.PushBack(EasyJson::kObject);
            path_handler["path"] = handler.first;
            path_handler["alias"] = handler.second->alias();
        }
    }
    mustache::RenderTemplate(kMainTemplate, config::www_path, ej.value(), output);
}

} // namespace doris

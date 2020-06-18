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
#include "env/env.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "olap/file_helper.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/mem_info.h"

using strings::Substitute;

namespace doris {

static std::string s_html_content_type = "text/html";

WebPageHandler::WebPageHandler(EvHttpServer* server) : _http_server(server) {
    _http_server->register_static_file_handler(this);

    TemplatePageHandlerCallback root_callback =
            boost::bind<void>(boost::mem_fn(&WebPageHandler::root_handler), this, _1, _2);
    register_template_page("/", "Home", root_callback, false /* is_on_nav_bar */);
}

WebPageHandler::~WebPageHandler() {
    STLDeleteValues(&_page_map);
}

void WebPageHandler::register_template_page(const std::string& path, const string& alias,
                                            const TemplatePageHandlerCallback& callback, bool is_on_nav_bar) {
    string render_path = (path == "/") ? "/home" : path;
    auto wrapped_cb = [=](const ArgumentMap& args, std::stringstream* output) {
        EasyJson ej;
        callback(args, &ej);
        Render(render_path, ej, true /* is_styled */, output);
    };
    register_page(path, alias, wrapped_cb, is_on_nav_bar);
}

void WebPageHandler::register_page(const std::string& path, const string& alias,
                                   const PageHandlerCallback& callback, bool is_on_nav_bar) {
    boost::mutex::scoped_lock lock(_map_lock);
    auto map_iter = _page_map.find(path);
    if (map_iter == _page_map.end()) {
        // first time, register this to web server
        _http_server->register_handler(HttpMethod::GET, path, this);
        _page_map[path] = new PathHandler(true /* is_styled */, is_on_nav_bar, alias, callback);
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
        } else {
            do_file_response(req->raw_path(), req);
            return;
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

void WebPageHandler::root_handler(const ArgumentMap& args, EasyJson* output) {
    (*output)["version"] = get_version_string(false);
    (*output)["hardware"] = CpuInfo::debug_string() + MemInfo::debug_string() + DiskInfo::debug_string();
}

std::map<std::string, std::string> extension_to_content_type({
    { "css", "text/css" },
    { "html", "text/html" },
    { "png", "image/png" }
    });

/* Try to guess a good content-type for 'path' */
const std::string& guess_content_type(const std::string& path) {
    size_t pos = path.find_last_of('.');
    if (pos == std::string:npos) {
        return "application/misc";
    }

    std::string extension = path.substr(pos);
    auto iter = extension_to_content_type.find(extension);
    if (iter == extension_to_content_type.end()) {
        return "application/misc";
    }

    return iter->second;
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
    <script src='/doris.js' defer></script>
    <link href='/doris.css' rel='stylesheet' />
  </head>
  <body>

    <nav class="navbar navbar-default">
      <div class="container-fluid">
        <div class="navbar-header">
          <a class="navbar-brand" href="/">Doris</a>
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

std::string WebPageHandler::MustachePartialTag(const std::string& path) const {
    return Substitute("{{> $0.mustache}}", path);
}

bool WebPageHandler::MustacheTemplateAvailable(const std::string& path) const {
    if (config::www_path.empty()) {
        return false;
    }
    return true; //Env::Default()->FileExists(Substitute("$0$1.mustache", opts_.doc_root, path));
}

void WebPageHandler::RenderMainTemplate(const std::string& content, std::stringstream* output) {
    static const std::string& footer = std::string("<pre>Version") + get_version_string(true) + std::string("</pre>");

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

void WebPageHandler::Render(const string& path, const EasyJson& ej, bool use_style,
                            std::stringstream* output) {
    if (MustacheTemplateAvailable(path)) {
        mustache::RenderTemplate(MustachePartialTag(path), config::www_path, ej.value(), output);
    } else if (use_style) {
        (*output) << "<pre>" << ej.ToString() << "</pre>";
    } else {
        (*output) << ej.ToString();
    }
}

} // namespace doris

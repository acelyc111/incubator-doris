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

#include "util/metrics.h"

namespace doris {

MetricLabels MetricLabels::EmptyLabels;

std::ostream& operator<<(std::ostream& os, MetricType type) {
    switch (type) {
    case MetricType::COUNTER:
        os << "counter";
        break;
    case MetricType::GAUGE:
        os << "gauge";
        break;
    case MetricType::HISTOGRAM:
        os << "histogram";
        break;
    case MetricType::SUMMARY:
        os << "summary";
        break;
    case MetricType::UNTYPED:
        os << "untyped";
        break;
    default:
        os << "unknown";
        break;
    }
    return os;
}

const char* unit_name(MetricUnit unit) {
    switch (unit) {
    case MetricUnit::NANOSECONDS:
        return "nanoseconds";
    case MetricUnit::MICROSECONDS:
        return "microseconds";
    case MetricUnit::MILLISECONDS:
        return "milliseconds";
    case MetricUnit::SECONDS:
        return "seconds";
    case MetricUnit::BYTES:
        return "bytes";
    case MetricUnit::ROWS:
        return "rows";
    case MetricUnit::PERCENT:
        return "percent";
    case MetricUnit::REQUESTS:
        return "requests";
    case MetricUnit::OPERATIONS:
        return "operations";
    case MetricUnit::BLOCKS:
        return "blocks";
    case MetricUnit::ROWSETS:
        return "rowsets";
    case MetricUnit::CONNECTIONS:
        return "rowsets";
    default:
        return "nounit";
    }
}

void Metric::hide() {
    if (_registry == nullptr) {
        return;
    }
    _registry->deregister_metric(this);
    _registry = nullptr;
}

std::string MetricPrototype::to_string(const std::string& registry_name) const {
    std::stringstream ss;
    std::string full_name = registry_name + _name;

    ss << "# TYPE " << full_name << " " << type << "\n";
    switch (type) {
        case MetricType::COUNTER:
        case MetricType::GAUGE:
            ss << metric.first->name << metric.first->labels_string() << "\n";
            break;
        default:
            break;
    }

    return ss.str();
}

std::string MetricPrototype::labels_string() const {
    if (labels.empty()) {
        return std::string();
    }

    std::stringstream ss;
    ss << "{";
    int i = 0;
    for (const auto& label : labels) {
        if (i++ > 0) {
            ss << ",";
        }
        ss << label.first << "=\"" << label.second << "\"";
    }
    ss << "}";

    return ss.str();
}

void MetricEntity::register_metric(const MetricPrototype* metric_type, Metric* metric) {
    DCHECK(_metrics.find(metric_type) == _metrics.end());
    _metrics.emplace(metric_type, metric);
}

Metric* MetricEntity::get_metric(const std::string& name) const {
    MetricPrototype dummy(MetricType::UNTYPED, MetricUnit::NOUNIT, name, "");
    auto it = _metrics.find(&dummy);
    if (it == _metrics.end()) {
        return nullptr;
    }
    return it->second;
}

std::string MetricEntity::to_prometheus(const std::string& registry_name) const {
    std::stringstream ss;
    for (const auto& metric : _metrics) {
        ss << metric.first->to_string() << metric.second->to_string() << "\n";
    }
    return ss;
}

bool MetricCollector::add_metic(const MetricLabels& labels, Metric* metric) {
    if (empty()) {
        _type = metric->type();
    } else {
        if (metric->type() != _type) {
            return false;
        }
    }
    auto it = _metrics.emplace(labels, metric);
    return it.second;
}

void MetricCollector::remove_metric(Metric* metric) {
    for (auto& it : _metrics) {
        if (it.second == metric) {
            _metrics.erase(it.first);
            break;
        }
    }
}

Metric* MetricCollector::get_metric(const MetricLabels& labels) const {
    auto it = _metrics.find(labels);
    if (it != std::end(_metrics)) {
        return it->second;
    }
    return nullptr;
}

void MetricCollector::get_metrics(std::vector<Metric*>* metrics) {
    for (auto& it : _metrics) {
        metrics->push_back(it.second);
    }
}

MetricRegistry::~MetricRegistry() {
    {
        std::lock_guard<SpinLock> l(_lock);

        std::vector<Metric*> metrics;
        for (auto& it : _collectors) {
            it.second->get_metrics(&metrics);
        }
        for (auto metric : metrics) {
            _deregister_locked(metric);
        }
    }
    // All register metric will deregister
    DCHECK(_collectors.empty()) << "_collectors not empty, size=" << _collectors.size();
}

MetricEntity* MetricRegistry::register_entity(const std::string& name, const AttributeMap& attributes) {
    std::shared_ptr<MetricEntity> entity = std::make_shared<MetricEntity>(name, attributes);

    std::lock_guard<SpinLock> l(_lock);
    DCHECK(_entities.find(name) == _entities.end());
    _entities.insert(std::make_pair(name, entity));
    return entity.get();
}

void MetricRegistry::deregister_entity(const std::string& name) {
    std::lock_guard<SpinLock> l(_lock);
    _entities.erase(name);
}

std::shared_ptr<MetricEntity> MetricRegistry::get_entity(const std::string& name) {
    std::lock_guard<SpinLock> l(_lock);
    auto entity = _entities.find(name);
    if (entity == _entities.end()) {
        return std::shared_ptr<MetricEntity>();
    }
    return entity->second;
}

bool MetricRegistry::register_metric(const std::string& name,
                                     const MetricLabels& labels,
                                     Metric* metric) {
    metric->hide();
    std::lock_guard<SpinLock> l(_lock);
    MetricCollector* collector = nullptr;
    auto it = _collectors.find(name);
    if (it == std::end(_collectors)) {
        collector = new MetricCollector();
        _collectors.emplace(name, collector);
    } else {
        collector = it->second;
    }
    auto res = collector->add_metic(labels, metric);
    if (res) {
        metric->_registry = this;
    }
    return res;
}

void MetricRegistry::_deregister_locked(Metric* metric) {
    std::vector<std::string> to_erase;
    for (auto& it : _collectors) {
        it.second->remove_metric(metric);
        if (it.second->empty()) {
            to_erase.emplace_back(it.first);
        }
    }
    for (auto& name : to_erase) {
        auto it = _collectors.find(name);
        delete it->second;
        _collectors.erase(it);
    }
}

Metric* MetricRegistry::get_metric(const std::string& name, const MetricLabels& labels) const {
    std::lock_guard<SpinLock> l(_lock);
    auto it = _collectors.find(name);
    if (it != std::end(_collectors)) {
        return it->second->get_metric(labels);
    }
    return nullptr;
}

bool MetricRegistry::register_hook(const std::string& name, const std::function<void()>& hook) {
    std::lock_guard<SpinLock> l(_lock);
    auto it = _hooks.emplace(name, hook);
    return it.second;
}

void MetricRegistry::deregister_hook(const std::string& name) {
    std::lock_guard<SpinLock> l(_lock);
    _hooks.erase(name);
}

}

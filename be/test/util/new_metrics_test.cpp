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

#include <gtest/gtest.h>

#include <iostream>
#include <thread>

#include "common/config.h"
#include "util/logging.h"
#include "util/metrics.h"
#include "util/stopwatch.hpp"

namespace doris {

class MetricsTest : public testing::Test {
public:
    MetricsTest() { }
    virtual ~MetricsTest() {
    }
};

TEST_F(MetricsTest, Counter) {
    {
        IntCounter counter(MetricUnit::NOUNIT);
        ASSERT_EQ(0, counter.value());
        counter.increment(100);
        ASSERT_EQ(100, counter.value());

        ASSERT_STREQ("100", counter.to_string().c_str());
    }
    {
        DoubleCounter counter(MetricUnit::NOUNIT);
        ASSERT_EQ(0.0, counter.value());
        counter.increment(1.23);
        ASSERT_EQ(1.23, counter.value());

        ASSERT_STREQ("1.23", counter.to_string().c_str());
    }
}

void mt_updater(IntCounter* counter, std::atomic<uint64_t>* used_time) {
    sleep(1);
    MonotonicStopWatch watch;
    watch.start();
    for (int i = 0; i < 1000000L; ++i) {
        counter->increment(1);
    }
    uint64_t elapsed = watch.elapsed_time();
    used_time->fetch_add(elapsed);
}

TEST_F(MetricsTest, CounterPerf) {
    IntCounter counter(MetricUnit::NOUNIT);
    volatile int64_t sum = 0;

    {
        MonotonicStopWatch watch;
        watch.start();
        for (int i = 0; i < 100000000; ++i) {
            counter.increment(1);
        }
        uint64_t elapsed = watch.elapsed_time();
        LOG(INFO) << "counter elapsed: " << elapsed
                  << "ns, ns/iter:" << elapsed / 100000000;
    }
    {
        MonotonicStopWatch watch;
        watch.start();
        for (int i = 0; i < 100000000; ++i) {
            sum += 1;
        }
        uint64_t elapsed = watch.elapsed_time();
        LOG(INFO) << "value elapsed: " << elapsed
                  << "ns, ns/iter:" << elapsed / 100000000;
    }
    ASSERT_EQ(100000000, counter.value());
    ASSERT_EQ(100000000, sum);
    {
        IntCounter mt_counter(MetricUnit::NOUNIT);
        std::vector<std::thread> updaters;
        std::atomic<uint64_t> used_time(0);
        for (int i = 0; i < 8; ++i) {
            updaters.emplace_back(&mt_updater, &mt_counter, &used_time);
        }
        for (int i = 0; i < 8; ++i) {
            updaters[i].join();
        }
        LOG(INFO) << "mt_counter elapsed: " << used_time.load()
                  << "ns, ns/iter:" << used_time.load() / (8 * 1000000L);
        ASSERT_EQ(8 * 1000000L, mt_counter.value());
    }
}

TEST_F(MetricsTest, Gauge) {
    {
        IntGauge gauge(MetricUnit::NOUNIT);
        ASSERT_EQ(0, gauge.value());
        gauge.set_value(100);
        ASSERT_EQ(100, gauge.value());

        ASSERT_STREQ("100", gauge.to_string().c_str());
    }
    {
        DoubleGauge gauge(MetricUnit::NOUNIT);
        ASSERT_EQ(0.0, gauge.value());
        gauge.set_value(1.23);
        ASSERT_EQ(1.23, gauge.value());

        ASSERT_STREQ("1.23", gauge.to_string().c_str());
    }
}

TEST_F(MetricsTest, MetricPrototype) {
    {
        MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT, "fragment_requests_total",
                                      "Total fragment requests received.");

        ASSERT_EQ("fragment_requests_total", cpu_idle_type.simple_name());
        ASSERT_EQ("fragment_requests_total", cpu_idle_type.combine_name(""));
        ASSERT_EQ("doris_be_fragment_requests_total", cpu_idle_type.combine_name("doris_be"));
    }
    {
        MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT, "cpu_idle",
                                      "CPU's idle time percent", "cpu");

        ASSERT_EQ("cpu", cpu_idle_type.simple_name());
        ASSERT_EQ("cpu_idle", cpu_idle_type.combine_name(""));
        ASSERT_EQ("doris_be_cpu", cpu_idle_type.combine_name("doris_be"));
    }
}

TEST_F(MetricsTest, MetricEntityWithMetrics) {
    MetricEntity entity("test_entity");

    IntCounter cpu_idle;
    MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT, "cpu_idle");

    // Before register
    Metric* metric = entity.get_metric("cpu_idle");
    ASSERT_EQ(nullptr, metric);

    // Register
    entity.register_metric(&cpu_idle_type, &cpu_idle));
    cpu_idle.increment(12);

    metric = entity.get_metric("cpu_idle");
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ("12", metric->to_string());

    cpu_idle.increment(8);
    ASSERT_EQ("20", metric->to_string());

    // Deregister
    entity.degister_metric(&cpu_idle_type);

    // After deregister
    metric = entity.get_metric("cpu_idle");
    ASSERT_EQ(nullptr, metric);
}

TEST_F(MetricsTest, MetricEntityWithMetrics) {
    MetricEntity entity("test_entity");

    IntCounter cpu_idle;
    MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT, "cpu_idle");

    // Register
    entity.register_metric(&cpu_idle_type, &cpu_idle));
    entity.register_hook("test_hook", []() {
        cpu_idle.increment(6);
    });

    // Before hook
    Metric* metric = entity.get_metric("cpu_idle");
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ("0", metric->to_string());

    // Hook
    entity.trigger_hook_unlocked();
    ASSERT_EQ("6", metric->to_string());

    entity.trigger_hook_unlocked();
    ASSERT_EQ("12", metric->to_string());

    // Deregister hook
    entity.deregister_hook("test_hook");
    // Hook but no effect
    entity.trigger_hook_unlocked();
    ASSERT_EQ("12", metric->to_string());
}

TEST_F(MetricsTest, MetricRegistryRegister) {
    MetricRegistry registry("test_registry");

    // No entity
    ASSERT_EQ("", registry.to_prometheus());
    ASSERT_EQ("", registry.to_json());
    ASSERT_EQ("", registry.to_core_string());

    // Before register
    auto entity = registry.get_entity("test_entity");
    ASSERT_EQ(nullptr, entity);

    // Register
    registry.register_entity("test_entity", {});

    // After register
    entity = registry.get_entity("test_entity");
    ASSERT_NE(nullptr, entity);

    registry.deregister_entity("test_entity");
    entity = registry.get_entity("test_entity");
    ASSERT_EQ(nullptr, entity);
}

TEST_F(MetricsTest, MetricRegistryOutput) {
    MetricRegistry registry("test_registry");

    // No entity
    ASSERT_EQ("", registry.to_prometheus());
    ASSERT_EQ("", registry.to_json());
    ASSERT_EQ("", registry.to_core_string());

    // Before register
    auto entity = registry.get_entity("test_entity");
    ASSERT_EQ(nullptr, entity);

    // Register
    registry.register_entity("test_entity", {});

    // After register
    entity = registry.get_entity("test_entity");
    ASSERT_NE(nullptr, entity);

    registry.deregister_entity("test_entity");
    entity = registry.get_entity("test_entity");
    ASSERT_EQ(nullptr, entity);
}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

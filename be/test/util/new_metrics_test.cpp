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
    MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT, "cpu_idle",
                                  "CPU's idle time percent", "cpu", {{"mode", "idle"}}, true);
}

TEST_F(MetricsTest, MetricEntity) {
    MetricEntity entity("test");

    MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT, "cpu_idle",
                                  "CPU's idle time percent", "cpu", {{"mode", "idle"}}, true);
    IntCounter cpu_idle;
    entity.register_metric(&cpu_idle_type, &cpu_idle));
    cpu_idle.increment(12);

    IntCounter dummy(MetricUnit::PERCENT);
    ASSERT_FALSE(entity.register_metric("cpu_idle", &dummy));

    IntCounter memory_usage(MetricUnit::BYTES);
    memory_usage.increment(24);
    ASSERT_TRUE(entity.register_metric("memory_usage", &memory_usage));

    {
        TestMetricsVisitor visitor;
        entity.collect(&visitor);
        ASSERT_STREQ("test_cpu_idle 12\ntest_memory_usage 24\n", visitor.to_string().c_str());
    }
    entity.deregister_metric(&memory_usage);
    {
        TestMetricsVisitor visitor;
        entity.collect(&visitor);
        ASSERT_STREQ("test_cpu_idle 12\n", visitor.to_string().c_str());
    }
    // test get_metric
    ASSERT_TRUE(entity.get_metric("cpu_idle") != nullptr);
    ASSERT_TRUE(entity.get_metric("memory_usage") == nullptr);
}

TEST_F(MetricsTest, MetricRegistry2) {
    // TODO(yingchun): Add test for MetricEntity
    MetricRegistry registry("test");
    IntCounter cpu_idle(MetricUnit::PERCENT);
    cpu_idle.increment(12);
    ASSERT_TRUE(registry.register_metric("cpu_idle", &cpu_idle));

    {
        // memory_usage will deregister after this block
        IntCounter memory_usage(MetricUnit::BYTES);
        memory_usage.increment(24);
        ASSERT_TRUE(registry.register_metric("memory_usage", &memory_usage));
        TestMetricsVisitor visitor;
        registry.collect(&visitor);
        ASSERT_STREQ("test_cpu_idle 12\ntest_memory_usage 24\n", visitor.to_string().c_str());
    }

    {
        TestMetricsVisitor visitor;
        registry.collect(&visitor);
        ASSERT_STREQ("test_cpu_idle 12\n", visitor.to_string().c_str());
    }
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

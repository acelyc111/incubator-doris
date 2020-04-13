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

#ifndef DORIS_BE_SRC_COMMON_UTIL_DORIS_METRICS_H
#define DORIS_BE_SRC_COMMON_UTIL_DORIS_METRICS_H

#include <set>
#include <string>
#include <vector>
#include <unordered_map>

#include "util/metrics.h"
#include "util/system_metrics.h"

namespace doris {

class IntGaugeMetricsMap {
public:
    void set_metric(const std::string& key, int64_t val) {
        auto metric = metrics.find(key);
        if (metric != metrics.end()) {
            metric->second.set_value(val);
        }
    }

    IntGauge* set_key(const std::string& key) {
        metrics.emplace(key, IntGauge());
        return &metrics.find(key)->second;
    }

private:
    std::unordered_map<std::string, IntGauge> metrics;
};

#define REGISTER_GAUGE_DORIS_METRIC(name, func) \
  DorisMetrics::metrics()->register_metric(#name, &DorisMetrics::name); \
  DorisMetrics::metrics()->register_hook(#name, [&]() { \
      DorisMetrics::name.set_value(func());  \
});

class DorisMetrics {
public:
    // counters
    static IntCounter fragment_requests_total;
    static IntCounter fragment_request_duration_us;
    static IntCounter http_requests_total;
    static IntCounter http_request_duration_us;
    static IntCounter http_request_send_bytes;
    static IntCounter query_scan_bytes;
    static IntCounter query_scan_rows;
    static IntCounter ranges_processed_total;
    static IntCounter push_requests_success_total;
    static IntCounter push_requests_fail_total;
    static IntCounter push_request_duration_us;
    static IntCounter push_request_write_bytes;
    static IntCounter push_request_write_rows;
    static IntCounter create_tablet_requests_total;
    static IntCounter create_tablet_requests_failed;
    static IntCounter drop_tablet_requests_total;

    static IntCounter report_all_tablets_requests_total;
    static IntCounter report_all_tablets_requests_failed;
    static IntCounter report_tablet_requests_total;
    static IntCounter report_tablet_requests_failed;
    static IntCounter report_disk_requests_total;
    static IntCounter report_disk_requests_failed;
    static IntCounter report_task_requests_total;
    static IntCounter report_task_requests_failed;

    static IntCounter schema_change_requests_total;
    static IntCounter schema_change_requests_failed;
    static IntCounter create_rollup_requests_total;
    static IntCounter create_rollup_requests_failed;
    static IntCounter storage_migrate_requests_total;
    static IntCounter delete_requests_total;
    static IntCounter delete_requests_failed;
    static IntCounter clone_requests_total;
    static IntCounter clone_requests_failed;

    static IntCounter finish_task_requests_total;
    static IntCounter finish_task_requests_failed;

    static IntCounter base_compaction_request_total;
    static IntCounter base_compaction_request_failed;
    static IntCounter cumulative_compaction_request_total;
    static IntCounter cumulative_compaction_request_failed;

    static IntCounter base_compaction_deltas_total;
    static IntCounter base_compaction_bytes_total;
    static IntCounter cumulative_compaction_deltas_total;
    static IntCounter cumulative_compaction_bytes_total;

    static IntCounter publish_task_request_total;
    static IntCounter publish_task_failed_total;

    static IntCounter meta_write_request_total;
    static IntCounter meta_write_request_duration_us;
    static IntCounter meta_read_request_total;
    static IntCounter meta_read_request_duration_us;

    // Counters for segment_v2
    // -----------------------
    // total number of segments read
    static IntCounter segment_read_total;
    // total number of rows in queried segments (before index pruning)
    static IntCounter segment_row_total;
    // total number of rows selected by short key index
    static IntCounter segment_rows_by_short_key;
    // total number of rows selected by zone map index
    static IntCounter segment_rows_read_by_zone_map;

    static IntCounter txn_begin_request_total;
    static IntCounter txn_commit_request_total;
    static IntCounter txn_rollback_request_total;
    static IntCounter txn_exec_plan_total;
    static IntCounter stream_receive_bytes_total;
    static IntCounter stream_load_rows_total;

    static IntCounter memtable_flush_total;
    static IntCounter memtable_flush_duration_us;

    // Gauges
    static IntGauge memory_pool_bytes_total;
    static IntGauge process_thread_num;
    static IntGauge process_fd_num_used;
    static IntGauge process_fd_num_limit_soft;
    static IntGauge process_fd_num_limit_hard;
    static IntGaugeMetricsMap disks_total_capacity;
    static IntGaugeMetricsMap disks_avail_capacity;
    static IntGaugeMetricsMap disks_data_used_capacity;
    static IntGaugeMetricsMap disks_state;

    // the max compaction score of all tablets.
    // Record base and cumulative scores separately, because
    // we need to get the larger of the two.
    static IntGauge tablet_cumulative_max_compaction_score;
    static IntGauge tablet_base_max_compaction_score;

    // The following metrics will be calculated
    // by metric calculator
    static IntGauge push_request_write_bytes_per_second;
    static IntGauge query_scan_bytes_per_second;
    static IntGauge max_disk_io_util_percent;
    static IntGauge max_network_send_bytes_rate;
    static IntGauge max_network_receive_bytes_rate;

    // Metrics related with BlockManager
    static IntCounter readable_blocks_total;
    static IntCounter writable_blocks_total;
    static IntCounter blocks_created_total;
    static IntCounter blocks_deleted_total;
    static IntCounter bytes_read_total;
    static IntCounter bytes_written_total;
    static IntCounter disk_sync_total;
    static IntGauge blocks_open_reading;
    static IntGauge blocks_open_writing;

    static IntCounter blocks_push_remote_duration_us;

    // Size of some global containers
    static UIntGauge rowset_count_generated_and_in_use;
    static UIntGauge unused_rowsets_count;
    static UIntGauge broker_count;
    static UIntGauge data_stream_receiver_count;
    static UIntGauge fragment_endpoint_count;
    static UIntGauge active_scan_context_count;
    static UIntGauge plan_fragment_count;
    static UIntGauge load_channel_count;
    static UIntGauge result_buffer_block_count;
    static UIntGauge result_block_queue_count;
    static UIntGauge routine_load_task_count;
    static UIntGauge small_file_cache_count;
    static UIntGauge stream_load_pipe_count;
    static UIntGauge brpc_endpoint_stub_count;
    static UIntGauge tablet_writer_count;


    static DorisMetrics* instance() {
        static DorisMetrics instance;
        return &instance;
    }

    // not thread-safe, call before calling metrics
    void initialize(
        const std::vector<std::string>& paths = std::vector<std::string>(),
        bool init_system_metrics = false,
        const std::set<std::string>& disk_devices = std::set<std::string>(),
        const std::vector<std::string>& network_interfaces = std::vector<std::string>());

    MetricRegistry* metrics() { return &_metrics; }
    SystemMetrics* system_metrics() { return &_system_metrics; }

private:
    // Don't allow constrctor
    DorisMetrics();

    void _update();
    void _update_process_thread_num();
    void _update_process_fd_num();

private:
    const char* _name;
    const char* _hook_name;

    MetricRegistry _metrics;
    SystemMetrics _system_metrics;
};

};

#endif

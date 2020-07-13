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

#include "util/doris_metrics.h"

#include <sys/types.h>
#include <unistd.h>

#include "env/env.h"
#include "util/debug_util.h"
#include "util/file_utils.h"
#include "util/system_metrics.h"

namespace doris {

DEFINE_COUNTER_METRIC_3ARG(fragment_requests_total, MetricUnit::REQUESTS, "Total fragment requests received.");
DEFINE_COUNTER_METRIC_2ARG(fragment_request_duration_us, MetricUnit::MICROSECONDS);
DEFINE_COUNTER_METRIC_2ARG(http_requests_total, MetricUnit::REQUESTS);
DEFINE_COUNTER_METRIC_2ARG(http_request_send_bytes, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_2ARG(query_scan_bytes, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_2ARG(query_scan_rows, MetricUnit::ROWS);
DEFINE_COUNTER_METRIC_5ARG(push_requests_success_total, MetricUnit::REQUESTS, "", push_requests_total, Labels({{"status", "SUCCESS"}}));
DEFINE_COUNTER_METRIC_5ARG(push_requests_fail_total, MetricUnit::REQUESTS, "", push_requests_total, Labels({{"status", "FAIL"}}));
DEFINE_COUNTER_METRIC_2ARG(push_request_duration_us, MetricUnit::MICROSECONDS);
DEFINE_COUNTER_METRIC_2ARG(push_request_write_bytes, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_2ARG(push_request_write_rows, MetricUnit::ROWS);

#define DEFINE_ENGINE_COUNTER_METRIC(name, type, status)  \
    DEFINE_COUNTER_METRIC_5ARG(name, MetricUnit::REQUESTS, "", engine_requests_total, Labels({{"type", #type}, {"status", #status}}));

DEFINE_ENGINE_COUNTER_METRIC(create_tablet_requests_total, create_tablet, total);
DEFINE_ENGINE_COUNTER_METRIC(create_tablet_requests_failed, create_tablet, failed);
DEFINE_ENGINE_COUNTER_METRIC(drop_tablet_requests_total, drop_tablet, total);
DEFINE_ENGINE_COUNTER_METRIC(report_all_tablets_requests_total, report_all_tablets, total);
DEFINE_ENGINE_COUNTER_METRIC(report_all_tablets_requests_failed, report_all_tablets, failed);
DEFINE_ENGINE_COUNTER_METRIC(report_tablet_requests_total, report_tablet, total);
DEFINE_ENGINE_COUNTER_METRIC(report_tablet_requests_failed, report_tablet, failed);
DEFINE_ENGINE_COUNTER_METRIC(report_disk_requests_total, report_disk, total);
DEFINE_ENGINE_COUNTER_METRIC(report_disk_requests_failed, report_disk, failed);
DEFINE_ENGINE_COUNTER_METRIC(report_task_requests_total, report_task, total);
DEFINE_ENGINE_COUNTER_METRIC(report_task_requests_failed, report_task, failed);
DEFINE_ENGINE_COUNTER_METRIC(schema_change_requests_total, schema_change, total);
DEFINE_ENGINE_COUNTER_METRIC(schema_change_requests_failed, schema_change, failed);
DEFINE_ENGINE_COUNTER_METRIC(create_rollup_requests_total, create_rollup, total);
DEFINE_ENGINE_COUNTER_METRIC(create_rollup_requests_failed, create_rollup, failed);
DEFINE_ENGINE_COUNTER_METRIC(storage_migrate_requests_total, storage_migrate, total);
DEFINE_ENGINE_COUNTER_METRIC(delete_requests_total, delete, total);
DEFINE_ENGINE_COUNTER_METRIC(delete_requests_failed, delete, failed);
DEFINE_ENGINE_COUNTER_METRIC(clone_requests_total, clone, total);
DEFINE_ENGINE_COUNTER_METRIC(clone_requests_failed, clone, failed);
DEFINE_ENGINE_COUNTER_METRIC(finish_task_requests_total, finish_task, total);
DEFINE_ENGINE_COUNTER_METRIC(finish_task_requests_failed, finish_task, failed);
DEFINE_ENGINE_COUNTER_METRIC(base_compaction_request_total, base_compaction, total);
DEFINE_ENGINE_COUNTER_METRIC(base_compaction_request_failed, base_compaction, failed);
DEFINE_ENGINE_COUNTER_METRIC(cumulative_compaction_request_total, cumulative_compaction, total);
DEFINE_ENGINE_COUNTER_METRIC(cumulative_compaction_request_failed, cumulative_compaction, failed);
DEFINE_ENGINE_COUNTER_METRIC(publish_task_request_total, publish, total);
DEFINE_ENGINE_COUNTER_METRIC(publish_task_failed_total, publish, failed);

DEFINE_COUNTER_METRIC_5ARG(base_compaction_deltas_total, MetricUnit::ROWSETS, "", compaction_deltas_total, Labels({{"type", "base"}}));
DEFINE_COUNTER_METRIC_5ARG(cumulative_compaction_deltas_total, MetricUnit::ROWSETS, "", compaction_deltas_total, Labels({{"type", "cumulative"}}));
DEFINE_COUNTER_METRIC_5ARG(base_compaction_bytes_total, MetricUnit::BYTES, "", compaction_bytes_total, Labels({{"type", "base"}}));
DEFINE_COUNTER_METRIC_5ARG(cumulative_compaction_bytes_total, MetricUnit::BYTES, "", compaction_bytes_total, Labels({{"type", "cumulative"}}));

DEFINE_COUNTER_METRIC_5ARG(meta_write_request_total, MetricUnit::REQUESTS, "", meta_request_total, Labels({{"type", "write"}}));
DEFINE_COUNTER_METRIC_5ARG(meta_read_request_total, MetricUnit::REQUESTS, "", meta_request_total, Labels({{"type", "read"}}));

DEFINE_COUNTER_METRIC_5ARG(meta_write_request_duration_us, MetricUnit::MICROSECONDS, "", meta_request_duration, Labels({{"type", "write"}}));
DEFINE_COUNTER_METRIC_5ARG(meta_read_request_duration_us, MetricUnit::MICROSECONDS, "", meta_request_duration, Labels({{"type", "read"}}));

DEFINE_COUNTER_METRIC_5ARG(segment_read_total, MetricUnit::OPERATIONS, "(segment_v2) total number of segments read", segment_read, Labels({{"type", "segment_total_read_times"}}));
DEFINE_COUNTER_METRIC_5ARG(segment_row_total, MetricUnit::ROWS, "(segment_v2) total number of rows in queried segments (before index pruning)", segment_read, Labels({{"type", "segment_total_row_num"}}));
DEFINE_COUNTER_METRIC_5ARG(segment_rows_by_short_key, MetricUnit::ROWS, "(segment_v2) total number of rows selected by short key index", segment_read, Labels({{"type", "segment_rows_by_short_key"}}));
DEFINE_COUNTER_METRIC_5ARG(segment_rows_read_by_zone_map, MetricUnit::ROWS, "(segment_v2) total number of rows selected by zone map index", segment_read, Labels({{"type", "segment_rows_read_by_zone_map"}}));

DEFINE_COUNTER_METRIC_5ARG(txn_begin_request_total, MetricUnit::OPERATIONS, "", txn_request, Labels({{"type", "begin"}}));
DEFINE_COUNTER_METRIC_5ARG(txn_commit_request_total, MetricUnit::OPERATIONS, "", txn_request, Labels({{"type", "commit"}}));
DEFINE_COUNTER_METRIC_5ARG(txn_rollback_request_total, MetricUnit::OPERATIONS, "", txn_request, Labels({{"type", "rollback"}}));
DEFINE_COUNTER_METRIC_5ARG(txn_exec_plan_total, MetricUnit::OPERATIONS, "", txn_request, Labels({{"type", "exec"}}));

DEFINE_COUNTER_METRIC_5ARG(stream_receive_bytes_total, MetricUnit::BYTES, "", stream_load, Labels({{"type", "receive_bytes"}}));
DEFINE_COUNTER_METRIC_5ARG(stream_load_rows_total, MetricUnit::ROWS, "", stream_load, Labels({{"type", "load_rows"}}));

DEFINE_COUNTER_METRIC_2ARG(load_rows, MetricUnit::ROWS);
DEFINE_COUNTER_METRIC_2ARG(load_bytes, MetricUnit::BYTES);

DEFINE_COUNTER_METRIC_2ARG(memtable_flush_total, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_2ARG(memtable_flush_duration_us, MetricUnit::MICROSECONDS);

DEFINE_GAUGE_METRIC_2ARG(memory_pool_bytes_total, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_2ARG(process_thread_num, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(process_fd_num_used, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(process_fd_num_limit_soft, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(process_fd_num_limit_hard, MetricUnit::NOUNIT);

DEFINE_GAUGE_METRIC_2ARG(tablet_cumulative_max_compaction_score, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(tablet_base_max_compaction_score, MetricUnit::NOUNIT);

DEFINE_GAUGE_METRIC_2ARG(push_request_write_bytes_per_second, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_2ARG(query_scan_bytes_per_second, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_2ARG(max_disk_io_util_percent, MetricUnit::PERCENT);
DEFINE_GAUGE_METRIC_2ARG(max_network_send_bytes_rate, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_2ARG(max_network_receive_bytes_rate, MetricUnit::BYTES);

DEFINE_COUNTER_METRIC_2ARG(readable_blocks_total, MetricUnit::BLOCKS);
DEFINE_COUNTER_METRIC_2ARG(writable_blocks_total, MetricUnit::BLOCKS);
DEFINE_COUNTER_METRIC_2ARG(blocks_created_total, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_2ARG(blocks_deleted_total, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_2ARG(bytes_read_total, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_2ARG(bytes_written_total, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_2ARG(disk_sync_total, MetricUnit::OPERATIONS);

DEFINE_GAUGE_METRIC_2ARG(blocks_open_reading, MetricUnit::BLOCKS);
DEFINE_GAUGE_METRIC_2ARG(blocks_open_writing, MetricUnit::BLOCKS);

DEFINE_GAUGE_METRIC_2ARG(rowset_count_generated_and_in_use, MetricUnit::ROWSETS);
DEFINE_GAUGE_METRIC_2ARG(unused_rowsets_count, MetricUnit::ROWSETS);
DEFINE_GAUGE_METRIC_2ARG(broker_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(data_stream_receiver_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(fragment_endpoint_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(active_scan_context_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(plan_fragment_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(load_channel_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(result_buffer_block_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(result_block_queue_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(routine_load_task_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(small_file_cache_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(stream_load_pipe_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(brpc_endpoint_stub_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_2ARG(tablet_writer_count, MetricUnit::NOUNIT);

DEFINE_GAUGE_METRIC_2ARG(active_scratch_dirs, MetricUnit::NOUNIT);

DorisMetrics::DorisMetrics() : _name("doris_be"), _hook_name("doris_metrics"), _metric_registry(_name) {
    _server_metric_entity = _metric_registry.register_entity("server", {});

    METRIC_REGISTER(_server_metric_entity, fragment_requests_total);
    METRIC_REGISTER(_server_metric_entity, fragment_request_duration_us);
    METRIC_REGISTER(_server_metric_entity, http_requests_total);
    METRIC_REGISTER(_server_metric_entity, http_request_send_bytes);
    METRIC_REGISTER(_server_metric_entity, query_scan_bytes);
    METRIC_REGISTER(_server_metric_entity, query_scan_rows);

    METRIC_REGISTER(_server_metric_entity, push_requests_success_total);
    METRIC_REGISTER(_server_metric_entity, push_requests_fail_total);
    METRIC_REGISTER(_server_metric_entity, push_request_duration_us);
    METRIC_REGISTER(_server_metric_entity, push_request_write_bytes);
    METRIC_REGISTER(_server_metric_entity, push_request_write_rows);

    // engine_requests_total
    METRIC_REGISTER(_server_metric_entity, create_tablet_requests_total);
    METRIC_REGISTER(_server_metric_entity, create_tablet_requests_failed);
    METRIC_REGISTER(_server_metric_entity, drop_tablet_requests_total);
    METRIC_REGISTER(_server_metric_entity, report_all_tablets_requests_total);
    METRIC_REGISTER(_server_metric_entity, report_all_tablets_requests_failed);
    METRIC_REGISTER(_server_metric_entity, report_tablet_requests_total);
    METRIC_REGISTER(_server_metric_entity, report_tablet_requests_failed);
    METRIC_REGISTER(_server_metric_entity, report_disk_requests_total);
    METRIC_REGISTER(_server_metric_entity, report_disk_requests_failed);
    METRIC_REGISTER(_server_metric_entity, report_task_requests_total);
    METRIC_REGISTER(_server_metric_entity, report_task_requests_failed);
    METRIC_REGISTER(_server_metric_entity, schema_change_requests_total);
    METRIC_REGISTER(_server_metric_entity, schema_change_requests_failed);
    METRIC_REGISTER(_server_metric_entity, create_rollup_requests_total);
    METRIC_REGISTER(_server_metric_entity, create_rollup_requests_failed);
    METRIC_REGISTER(_server_metric_entity, storage_migrate_requests_total);
    METRIC_REGISTER(_server_metric_entity, delete_requests_total);
    METRIC_REGISTER(_server_metric_entity, delete_requests_failed);
    METRIC_REGISTER(_server_metric_entity, clone_requests_total);
    METRIC_REGISTER(_server_metric_entity, clone_requests_failed);
    METRIC_REGISTER(_server_metric_entity, finish_task_requests_total);
    METRIC_REGISTER(_server_metric_entity, finish_task_requests_failed);
    METRIC_REGISTER(_server_metric_entity, base_compaction_request_total);
    METRIC_REGISTER(_server_metric_entity, base_compaction_request_failed);
    METRIC_REGISTER(_server_metric_entity, cumulative_compaction_request_total);
    METRIC_REGISTER(_server_metric_entity, cumulative_compaction_request_failed);
    METRIC_REGISTER(_server_metric_entity, publish_task_request_total);
    METRIC_REGISTER(_server_metric_entity, publish_task_failed_total);

    METRIC_REGISTER(_server_metric_entity, base_compaction_deltas_total);
    METRIC_REGISTER(_server_metric_entity, base_compaction_bytes_total);
    METRIC_REGISTER(_server_metric_entity, cumulative_compaction_deltas_total);
    METRIC_REGISTER(_server_metric_entity, cumulative_compaction_bytes_total);

    METRIC_REGISTER(_server_metric_entity, meta_write_request_total);
    METRIC_REGISTER(_server_metric_entity, meta_write_request_duration_us);
    METRIC_REGISTER(_server_metric_entity, meta_read_request_total);
    METRIC_REGISTER(_server_metric_entity, meta_read_request_duration_us);

    METRIC_REGISTER(_server_metric_entity, segment_read_total);
    METRIC_REGISTER(_server_metric_entity, segment_row_total);
    METRIC_REGISTER(_server_metric_entity, segment_rows_by_short_key);
    METRIC_REGISTER(_server_metric_entity, segment_rows_read_by_zone_map);

    METRIC_REGISTER(_server_metric_entity, txn_begin_request_total);
    METRIC_REGISTER(_server_metric_entity, txn_commit_request_total);
    METRIC_REGISTER(_server_metric_entity, txn_rollback_request_total);
    METRIC_REGISTER(_server_metric_entity, txn_exec_plan_total);
    METRIC_REGISTER(_server_metric_entity, stream_receive_bytes_total);
    METRIC_REGISTER(_server_metric_entity, stream_load_rows_total);

    METRIC_REGISTER(_server_metric_entity, memtable_flush_total);
    METRIC_REGISTER(_server_metric_entity, memtable_flush_duration_us);

    METRIC_REGISTER(_server_metric_entity, memory_pool_bytes_total);
    METRIC_REGISTER(_server_metric_entity, process_thread_num);
    METRIC_REGISTER(_server_metric_entity, process_fd_num_used);
    METRIC_REGISTER(_server_metric_entity, process_fd_num_limit_soft);
    METRIC_REGISTER(_server_metric_entity, process_fd_num_limit_hard);

    METRIC_REGISTER(_server_metric_entity, tablet_cumulative_max_compaction_score);
    METRIC_REGISTER(_server_metric_entity, tablet_base_max_compaction_score);

    METRIC_REGISTER(_server_metric_entity, push_request_write_bytes_per_second);
    METRIC_REGISTER(_server_metric_entity, query_scan_bytes_per_second);
    METRIC_REGISTER(_server_metric_entity, max_disk_io_util_percent);
    METRIC_REGISTER(_server_metric_entity, max_network_send_bytes_rate);
    METRIC_REGISTER(_server_metric_entity, max_network_receive_bytes_rate);

    METRIC_REGISTER(_server_metric_entity, readable_blocks_total);
    METRIC_REGISTER(_server_metric_entity, writable_blocks_total);
    METRIC_REGISTER(_server_metric_entity, blocks_created_total);
    METRIC_REGISTER(_server_metric_entity, blocks_deleted_total);
    METRIC_REGISTER(_server_metric_entity, bytes_read_total);
    METRIC_REGISTER(_server_metric_entity, bytes_written_total);
    METRIC_REGISTER(_server_metric_entity, disk_sync_total);
    METRIC_REGISTER(_server_metric_entity, blocks_open_reading);
    METRIC_REGISTER(_server_metric_entity, blocks_open_writing);

    METRIC_REGISTER(_server_metric_entity, rowset_count_generated_and_in_use);
    METRIC_REGISTER(_server_metric_entity, unused_rowsets_count);
    METRIC_REGISTER(_server_metric_entity, broker_count);
    METRIC_REGISTER(_server_metric_entity, data_stream_receiver_count);
    METRIC_REGISTER(_server_metric_entity, fragment_endpoint_count);
    METRIC_REGISTER(_server_metric_entity, active_scan_context_count);
    METRIC_REGISTER(_server_metric_entity, plan_fragment_count);
    METRIC_REGISTER(_server_metric_entity, load_channel_count);
    METRIC_REGISTER(_server_metric_entity, result_buffer_block_count);
    METRIC_REGISTER(_server_metric_entity, result_block_queue_count);
    METRIC_REGISTER(_server_metric_entity, routine_load_task_count);
    METRIC_REGISTER(_server_metric_entity, small_file_cache_count);
    METRIC_REGISTER(_server_metric_entity, stream_load_pipe_count);
    METRIC_REGISTER(_server_metric_entity, brpc_endpoint_stub_count);
    METRIC_REGISTER(_server_metric_entity, tablet_writer_count);

    METRIC_REGISTER(_server_metric_entity, active_scratch_dirs);

    METRIC_REGISTER(_server_metric_entity, load_rows);
    METRIC_REGISTER(_server_metric_entity, load_bytes);

    _metric_registry.register_hook(_hook_name, std::bind(&DorisMetrics::_update, this));
}

void DorisMetrics::initialize(
        bool init_system_metrics,
        const std::set<std::string>& disk_devices,
        const std::vector<std::string>& network_interfaces) {
    if (init_system_metrics) {
        _system_metrics.reset(new SystemMetrics(&_metric_registry, disk_devices, network_interfaces));
    }
}

void DorisMetrics::_update() {
    _update_process_thread_num();
    _update_process_fd_num();
}

// get num of thread of doris_be process
// from /proc/pid/task
void DorisMetrics::_update_process_thread_num() {
    int64_t pid = getpid();
    std::stringstream ss;
    ss << "/proc/" << pid << "/task/";

    int64_t count = 0;
    Status st = FileUtils::get_children_count(Env::Default(), ss.str(), &count);
    if (!st.ok()) {
        LOG(WARNING) << "failed to count thread num from: " << ss.str();
        process_thread_num.set_value(0);
        return;
    }

    process_thread_num.set_value(count);
}

// get num of file descriptor of doris_be process
void DorisMetrics::_update_process_fd_num() {
    int64_t pid = getpid();

    // fd used
    std::stringstream ss;
    ss << "/proc/" << pid << "/fd/";
    int64_t count = 0;
    Status st = FileUtils::get_children_count(Env::Default(), ss.str(), &count);
    if (!st.ok()) {
        LOG(WARNING) << "failed to count fd from: " << ss.str();
        process_fd_num_used.set_value(0);
        return;
    }
    process_fd_num_used.set_value(count);

    // fd limits
    std::stringstream ss2;
    ss2 << "/proc/" << pid << "/limits";
    FILE* fp = fopen(ss2.str().c_str(), "r");
    if (fp == nullptr) {
        char buf[64];
        LOG(WARNING) << "open " << ss2.str() << " failed, errno=" << errno
            << ", message=" << strerror_r(errno, buf, 64);
        return;
    }

    // /proc/pid/limits
    // Max open files            65536                65536                files
    int64_t values[2];
    size_t line_buf_size = 0;
    char* line_ptr = nullptr;
    while (getline(&line_ptr, &line_buf_size, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(line_ptr, "Max open files %" PRId64 " %" PRId64,
                         &values[0], &values[1]);
        if (num == 2) {
            process_fd_num_limit_soft.set_value(values[0]);
            process_fd_num_limit_hard.set_value(values[1]);
            break;
        }
    }

    if (line_ptr != nullptr) {
        free(line_ptr);
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
            << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

}

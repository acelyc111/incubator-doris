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

#include <sys/types.h>
#include <unistd.h>

#include "util/doris_metrics.h"

#include "env/env.h"

#include "util/debug_util.h"
#include "util/file_utils.h"
#include "util/system_metrics.h"

namespace doris {

const std::string DorisMetrics::s_registry_name = "doris_be";
const std::string DorisMetrics::s_hook_name = "doris_metrics";

DorisMetrics::DorisMetrics() : _metrics(s_registry_name) {
#define REGISTER_DORIS_METRIC(name) _metrics.register_metric(#name, &name)
    // You can put DorisMetrics's metrics initial code here
    REGISTER_DORIS_METRIC(fragment_requests_total);
    REGISTER_DORIS_METRIC(fragment_requests_duration_us);
    REGISTER_DORIS_METRIC(http_requests_total);
    REGISTER_DORIS_METRIC(http_requests_send_bytes);
    REGISTER_DORIS_METRIC(query_scan_bytes);
    REGISTER_DORIS_METRIC(query_scan_rows);
    REGISTER_DORIS_METRIC(push_requests_duration_us);
    REGISTER_DORIS_METRIC(push_requests_write_bytes);
    REGISTER_DORIS_METRIC(push_requests_write_rows);
    REGISTER_DORIS_METRIC(memtable_flush_total);
    REGISTER_DORIS_METRIC(memtable_flush_duration_us);
    REGISTER_DORIS_METRIC(memory_pool_bytes_total);
    REGISTER_DORIS_METRIC(process_thread_num);
    REGISTER_DORIS_METRIC(process_fd_num_used);
    REGISTER_DORIS_METRIC(process_fd_num_limit_soft);
    REGISTER_DORIS_METRIC(process_fd_num_limit_hard);
    REGISTER_DORIS_METRIC(tablet_cumulative_max_compaction_score);
    REGISTER_DORIS_METRIC(tablet_base_max_compaction_score);
    REGISTER_DORIS_METRIC(push_requests_write_bytes_per_second);
    REGISTER_DORIS_METRIC(query_scan_bytes_per_second);
    REGISTER_DORIS_METRIC(max_disk_io_util_percent);
    REGISTER_DORIS_METRIC(max_network_send_bytes_rate);
    REGISTER_DORIS_METRIC(max_network_receive_bytes_rate);
    REGISTER_DORIS_METRIC(readable_blocks_total);
    REGISTER_DORIS_METRIC(writable_blocks_total);
    REGISTER_DORIS_METRIC(blocks_created_total);
    REGISTER_DORIS_METRIC(blocks_deleted_total);
    REGISTER_DORIS_METRIC(bytes_read_total);
    REGISTER_DORIS_METRIC(bytes_written_total);
    REGISTER_DORIS_METRIC(disk_sync_total);
    REGISTER_DORIS_METRIC(blocks_open_reading);
    REGISTER_DORIS_METRIC(blocks_open_writing);
#enddef

#define REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(type)                                                       \
    _metrics.register_metric(                                                                                       \
        "engine_requests_total", MetricLabels().add("type", #type).add("status", "total"), &type##_requests_total); \
    _metrics.register_metric(                                                                                       \
        "engine_requests_total", MetricLabels().add("type", #type).add("status", "failed"), &type##_requests_failed)

    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(create_tablet);
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(drop_tablet);
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(report_all_tablets);
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(report_tablet);
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(report_disk);
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(report_task);;
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(schema_change);
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(create_rollup);
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(storage_migrate);
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(delete);
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(clone);
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(finish_task);
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(base_compaction);
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(cumulative_compaction);
    REGISTER_ENGINE_REQUEST_TOTAL_AND_FAILED_METRIC(publish);
#enddef

#define REGISTER_REQUEST_METRIC(request, label_type, label_value, metric)     \
    _metrics.register_metric(                                                 \
        #request, MetricLabels().add(#label_type, #label_type), &metric)

#define REGISTER_REQUEST_STATUS_METRIC(request)                               \
    REGISTER_REQUEST_METRIC(request, status, success, request##_success);     \
    REGISTER_REQUEST_METRIC(request, status, failed, request##_failed);

    // TODO(yingchun): Not copatibale with older version
    REGISTER_REQUEST_STATUS_METRIC(push_requests);
#enddef

#define REGISTER_REQUEST_TYPE2_METRIC(request, type1, type2)                  \
    REGISTER_REQUEST_METRIC(request, type, type1, type1##_##request);         \
    REGISTER_REQUEST_METRIC(request, type, type2, type2##_##request);

    REGISTER_REQUEST_TYPE2_METRIC(compaction_deltas_total, base, cumulative);
    REGISTER_REQUEST_TYPE2_METRIC(compaction_bytes_total, base, cumulative);
    REGISTER_REQUEST_TYPE2_METRIC(meta_requests_total, read, write);
    REGISTER_REQUEST_TYPE2_METRIC(meta_requests_duration_us, read, write);
#enddef

#define REGISTER_REQUEST_RTYPE2_METRIC(request, type1, type2)                 \
    REGISTER_REQUEST_METRIC(request, type, type1, request##_##type1);         \
    REGISTER_REQUEST_METRIC(request, type, type2, request##_##type2);

    REGISTER_REQUEST_RTYPE2_METRIC(stream_load, receive_bytes, load_rows);
#enddef

#define REGISTER_REQUEST_TYPE4_METRIC(request, type1, type2, type3, type4)    \
    REGISTER_REQUEST_METRIC(request, type, type1, request##_##type1);         \
    REGISTER_REQUEST_METRIC(request, type, type2, request##_##type2);         \
    REGISTER_REQUEST_METRIC(request, type, type2, request##_##type3);         \
    REGISTER_REQUEST_METRIC(request, type, type2, request##_##type4);

    REGISTER_REQUEST_TYPE4_METRIC(segment_read, times, rows, rows_by_short_key, rows_read_by_zone_map);
    REGISTER_REQUEST_TYPE4_METRIC(txn_request, begin, commit, rollback, exec);
#enddef
#enddef

    _metrics.register_metric("load_rows", &load_rows_total);
    _metrics.register_metric("load_bytes", &load_bytes_total);

    _metrics.register_hook(s_hook_name, std::bind(&DorisMetrics::_update, this));
}

void DorisMetrics::initialize(
        const std::vector<std::string>& paths,
        bool init_system_metrics,
        const std::set<std::string>& disk_devices,
        const std::vector<std::string>& network_interfaces) {
#define REGISTER_PATH_METRIC(path, metric, unit)                                 \
    IntGauge* gauge = metric.add_metric(path, unit);                             \
    _metrics.register_metric(#metric, MetricLabels().add("path", path), gauge)

    for (auto& path : paths) {
        REGISTER_PATH_METRIC(path, disks_total_capacity, MetricUnit::BYTES);
        REGISTER_PATH_METRIC(path, disks_avail_capacity, MetricUnit::BYTES);
        REGISTER_PATH_METRIC(path, disks_data_used_capacity, MetricUnit::BYTES);
        REGISTER_PATH_METRIC(path, disks_state, MetricUnit::NOUNIT);
    }
#enddef

    if (init_system_metrics) {
        _system_metrics.install(&_metrics, disk_devices, network_interfaces);
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

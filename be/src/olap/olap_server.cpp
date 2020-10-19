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

#include "olap/storage_engine.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cmath>
#include <ctime>
#include <string>

#include <gperftools/profiler.h>

#include "agent/cgroups_mgr.h"
#include "common/status.h"
#include "olap/cumulative_compaction.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "util/time.h"

using std::string;

namespace doris {

// TODO(yingchun): should be more graceful in the future refactor.
#define SLEEP_IN_BG_WORKER(seconds)               \
  int64_t left_seconds = (seconds);               \
  while (!_stop_bg_worker && left_seconds > 0) {  \
      sleep(1);                                   \
      --left_seconds;                             \
  }                                               \
  if (_stop_bg_worker) {                          \
      break;                                      \
  }

// number of running SCHEMA-CHANGE threads
volatile uint32_t g_schema_change_active_threads = 0;

Status StorageEngine::start_bg_threads() {
    _unused_rowset_monitor_thread =  std::thread(
        [this] {
            _unused_rowset_monitor_thread_callback(nullptr);
        });
    _unused_rowset_monitor_thread.detach();
    LOG(INFO) << "unused rowset monitor thread started";

    // start thread for monitoring the snapshot and trash folder
    _garbage_sweeper_thread = std::thread(
        [this] {
            _garbage_sweeper_thread_callback(nullptr);
        });
    _garbage_sweeper_thread.detach();
    LOG(INFO) << "garbage sweeper thread started";

    // start thread for monitoring the tablet with io error
    _disk_stat_monitor_thread = std::thread(
        [this] {
            _disk_stat_monitor_thread_callback(nullptr);
        });
    _disk_stat_monitor_thread.detach();
    LOG(INFO) << "disk stat monitor thread started";

    // convert store map to vector
    std::vector<DataDir*> data_dirs;
    for (auto& tmp_store : _store_map) {
        data_dirs.push_back(tmp_store.second);
    }

    int32_t max_thread_num = config::max_compaction_threads;
    int32_t min_thread_num = config::min_compaction_threads;
    ThreadPoolBuilder("CompactionTaskThreadPool")
            .set_min_threads(min_thread_num)
            .set_max_threads(max_thread_num)
            .build(&_compaction_thread_pool);

    // compaction tasks producer thread
    RETURN_IF_ERROR(Thread::create("StorageEngine", "compaction_tasks_producer_thread",
                                   [this]() { this->_compaction_tasks_producer_callback(); },
                                   &_compaction_tasks_producer_thread));
    LOG(INFO) << "compaction tasks producer thread started";

    // tablet checkpoint thread
    for (auto data_dir : data_dirs) {
        _tablet_checkpoint_threads.emplace_back(
        [this, data_dir] {
            _tablet_checkpoint_callback((void*)data_dir);
        });
    }
    for (auto& thread : _tablet_checkpoint_threads) {
        thread.detach();
    }
    LOG(INFO) << "tablet checkpint thread started";

    // fd cache clean thread
    _fd_cache_clean_thread = std::thread(
        [this] {
            _fd_cache_clean_callback(nullptr);
        });
    _fd_cache_clean_thread.detach();
    LOG(INFO) << "fd cache clean thread started";

    // path scan and gc thread
    if (config::path_gc_check) {
        for (auto data_dir : get_stores()) {
            _path_scan_threads.emplace_back(
            [this, data_dir] {
                _path_scan_thread_callback((void*)data_dir);
            });

            _path_gc_threads.emplace_back(
            [this, data_dir] {
                _path_gc_thread_callback((void*)data_dir);
            });
        }
        for (auto& thread : _path_scan_threads) {
            thread.detach();
        }
        for (auto& thread : _path_gc_threads) {
            thread.detach();
        }
        LOG(INFO) << "path scan/gc threads started. number:" << get_stores().size();
    }

    LOG(INFO) << "all storage engine's backgroud threads are started.";
    return Status::OK();
}

void* StorageEngine::_fd_cache_clean_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_stop_bg_worker) {
        int32_t interval = config::file_descriptor_cache_clean_interval;
        if (interval <= 0) {
            OLAP_LOG_WARNING("config of file descriptor clean interval is illegal: [%d], "
                             "force set to 3600", interval);
            interval = 3600;
        }
        SLEEP_IN_BG_WORKER(interval);

        _start_clean_fd_cache();
    }

    return nullptr;
}

void* StorageEngine::_garbage_sweeper_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    uint32_t max_interval = config::max_garbage_sweep_interval;
    uint32_t min_interval = config::min_garbage_sweep_interval;

    if (!(max_interval >= min_interval && min_interval > 0)) {
        OLAP_LOG_WARNING("garbage sweep interval config is illegal: [max=%d min=%d].",
                         max_interval, min_interval);
        min_interval = 1;
        max_interval = max_interval >= min_interval ? max_interval : min_interval;
        LOG(INFO) << "force reset garbage sweep interval. "
                  << "max_interval=" << max_interval
                  << ", min_interval=" << min_interval;
    }

    const double pi = 4 * std::atan(1);
    double usage = 1.0;
    // 程序启动后经过min_interval后触发第一轮扫描
    while (!_stop_bg_worker) {
        usage *= 100.0;
        // 该函数特性：当磁盘使用率<60%的时候，ratio接近于1；
        // 当使用率介于[60%, 75%]之间时，ratio急速从0.87降到0.27；
        // 当使用率大于75%时，ratio值开始缓慢下降
        // 当usage=90%时，ratio约为0.0057
        double ratio = (1.1 * (pi / 2 - std::atan(usage / 5 - 14)) - 0.28) / pi;
        ratio = ratio > 0 ? ratio : 0;
        uint32_t curr_interval = max_interval * ratio;
        // 此时的特性，当usage<60%时，curr_interval的时间接近max_interval，
        // 当usage > 80%时，curr_interval接近min_interval
        curr_interval = curr_interval > min_interval ? curr_interval : min_interval;
        SLEEP_IN_BG_WORKER(curr_interval);

        // 开始清理，并得到清理后的磁盘使用率
        OLAPStatus res = _start_trash_sweep(&usage);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("one or more errors occur when sweep trash."
                    "see previous message for detail. [err code=%d]", res);
            // do nothing. continue next loop.
        }
    }

    return nullptr;
}

void* StorageEngine::_disk_stat_monitor_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_stop_bg_worker) {
        _start_disk_stat_monitor();

        int32_t interval = config::disk_stat_monitor_interval;
        if (interval <= 0) {
            LOG(WARNING) << "disk_stat_monitor_interval config is illegal: " << interval
                         << ", force set to 1";
            interval = 1;
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_unused_rowset_monitor_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_stop_bg_worker) {
        start_delete_unused_rowset();

        int32_t interval = config::unused_rowset_monitor_interval;
        if (interval <= 0) {
            LOG(WARNING) << "unused_rowset_monitor_interval config is illegal: " << interval
                         << ", force set to 1";
            interval = 1;
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}



void* StorageEngine::_path_gc_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    LOG(INFO) << "try to start path gc thread!";

    while (!_stop_bg_worker) {
        LOG(INFO) << "try to perform path gc!";
        // perform path gc by rowset id
        ((DataDir*)arg)->perform_path_gc_by_rowsetid();

        int32_t interval = config::path_gc_check_interval_second;
        if (interval <= 0) {
            LOG(WARNING) << "path gc thread check interval config is illegal:" << interval
                         << "will be forced set to half hour";
            interval = 1800; // 0.5 hour
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_path_scan_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    LOG(INFO) << "try to start path scan thread!";

    while (!_stop_bg_worker) {
        LOG(INFO) << "try to perform path scan!";
        ((DataDir*)arg)->perform_path_scan();

        int32_t interval = config::path_scan_interval_second;
        if (interval <= 0) {
            LOG(WARNING) << "path gc thread check interval config is illegal:" << interval
                         << "will be forced set to one day";
            interval = 24 * 3600; // one day
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_tablet_checkpoint_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    LOG(INFO) << "try to start tablet meta checkpoint thread!";
    while (!_stop_bg_worker) {
        LOG(INFO) << "begin to do tablet meta checkpoint:" << ((DataDir*)arg)->path();
        int64_t start_time = UnixMillis();
        _tablet_manager->do_tablet_meta_checkpoint((DataDir*)arg);
        int64_t used_time = (UnixMillis() - start_time) / 1000;
        if (used_time < config::tablet_meta_checkpoint_min_interval_secs) {
            int64_t interval = config::tablet_meta_checkpoint_min_interval_secs - used_time;
            SLEEP_IN_BG_WORKER(interval);
        } else {
            sleep(1);
        }
    }

    return nullptr;
}

void StorageEngine::_compaction_tasks_producer_callback() {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    LOG(INFO) << "try to start compaction producer process!";

    std::vector<TTabletId> tablet_submitted;
    std::vector<DataDir*> data_dirs;
    for (auto& tmp_store : _store_map) {
        data_dirs.push_back(tmp_store.second);
        _tablet_submitted_compaction[tmp_store.second] = tablet_submitted;
    }

    int round = 0;
    CompactionType compaction_type;
    while (true) {
        if (!config::disable_auto_compaction) {
            if (round < config::cumulative_compaction_rounds_for_each_base_compaction_round) {
                compaction_type = CompactionType::CUMULATIVE_COMPACTION;
                round++;
            } else {
                compaction_type = CompactionType::BASE_COMPACTION;
                round = 0;
            }
            vector<TabletSharedPtr> tablets_compaction =
                    _compaction_tasks_generator(compaction_type, data_dirs);
            if (tablets_compaction.size() == 0) {
                _wakeup_producer_flag = 0;
                std::unique_lock<std::mutex> lock(_compaction_producer_sleep_mutex);
                // It is necessary to wake up the thread on timeout to prevent deadlock
                // in case of no running compaction task.
                _compaction_producer_sleep_cv.wait_for(lock, std::chrono::milliseconds(2000),
                                                       [=] { return _wakeup_producer_flag == 1; });
                continue;
            }
            for (const auto& tablet : tablets_compaction) {
                int64_t permits = tablet->calc_compaction_score(compaction_type);
                if (_permit_limiter.request(permits)) {
                    {
                        std::unique_lock<std::mutex> lock(_tablet_submitted_compaction_mutex);
                        _tablet_submitted_compaction[tablet->data_dir()].emplace_back(
                                tablet->tablet_id());
                    }
                    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
                        _compaction_thread_pool->submit_func([=]() {
                            CgroupsMgr::apply_system_cgroup();
                            _perform_cumulative_compaction(tablet);
                            _permit_limiter.release(permits);
                            std::unique_lock<std::mutex> lock(_tablet_submitted_compaction_mutex);
                            vector<TTabletId>::iterator it_tablet =
                                    find(_tablet_submitted_compaction[tablet->data_dir()].begin(),
                                         _tablet_submitted_compaction[tablet->data_dir()].end(),
                                         tablet->tablet_id());
                            if (it_tablet !=
                                _tablet_submitted_compaction[tablet->data_dir()].end()) {
                                _tablet_submitted_compaction[tablet->data_dir()].erase(it_tablet);
                                _wakeup_producer_flag = 1;
                                _compaction_producer_sleep_cv.notify_one();
                            }
                        });
                    } else {
                        _compaction_thread_pool->submit_func([=]() {
                            CgroupsMgr::apply_system_cgroup();
                            _perform_base_compaction(tablet);
                            _permit_limiter.release(permits);
                            std::unique_lock<std::mutex> lock(_tablet_submitted_compaction_mutex);
                            vector<TTabletId>::iterator it_tablet =
                                    find(_tablet_submitted_compaction[tablet->data_dir()].begin(),
                                         _tablet_submitted_compaction[tablet->data_dir()].end(),
                                         tablet->tablet_id());
                            if (it_tablet !=
                                _tablet_submitted_compaction[tablet->data_dir()].end()) {
                                _tablet_submitted_compaction[tablet->data_dir()].erase(it_tablet);
                                _wakeup_producer_flag = 1;
                                _compaction_producer_sleep_cv.notify_one();
                            }
                        });
                    }
                }
            }
        } else {
            sleep(config::check_auto_compaction_interval_seconds);
        }
    }
}

vector<TabletSharedPtr> StorageEngine::_compaction_tasks_generator(
        CompactionType compaction_type, std::vector<DataDir*> data_dirs) {
    vector<TabletSharedPtr> tablets_compaction;
    std::random_shuffle(data_dirs.begin(), data_dirs.end());
    for (auto data_dir : data_dirs) {
        std::unique_lock<std::mutex> lock(_tablet_submitted_compaction_mutex);
        if (_tablet_submitted_compaction[data_dir].size() >= config::compaction_task_num_per_disk) {
            continue;
        }
        if (!data_dir->reach_capacity_limit(0)) {
            TabletSharedPtr tablet = _tablet_manager->find_best_tablet_to_compaction(
                    compaction_type, data_dir, _tablet_submitted_compaction[data_dir]);
            if (tablet != nullptr) {
                tablets_compaction.emplace_back(tablet);
            }
        }
    }
    return tablets_compaction;
}
} // namespace doris

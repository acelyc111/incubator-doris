# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/udf_ga")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/udf_ga")

find_package(Boost 1.64.0 REQUIRED COMPONENTS serialization system)

add_library(firstTime SHARED first_time_value.cpp)
add_library(hash64 SHARED udf_hash64.cpp udf_xxhash64.cpp)
add_library(retention SHARED retention_info.cpp retention_count.cpp short_encode.cpp)
add_library(retentionv2 SHARED retention_common_v2.hpp retention_info_v2.cpp retention_count.cpp short_encode.cpp)
add_library(funnel SHARED funnel_info.cpp funnel_count.cpp short_encode.cpp)
add_library(pathAnalysis SHARED path_analysis_common.hpp path_analysis_process.cpp path_analysis_count.cpp)
target_link_libraries(pathAnalysis ${Boost_LIBRARIES})

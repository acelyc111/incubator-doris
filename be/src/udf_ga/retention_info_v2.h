#pragma once

#ifndef DORIS_BE_UDF_UDA_RETENTION_INFO
#define DORIS_BE_UDF_UDA_RETENTION_INFO

#include <iostream>
#include <list>
#include <set>
#include <vector>

#include "udf/udf.h"
#include "udf_ga/retention_common_v2.hpp"
#include "udf_ga/short_encode.h"

namespace doris_udf {
struct RetentionInfoAgg {
    short retention_range;
    uint64_t start_event;
    uint64_t end_event[end_event_parts];
};

void add_start_event(long start_time, const string& unit, long event_time, StringVal* agg_info);
void add_end_event(long start_time, const string& unit, long event_time, StringVal* agg_info);
short make_value(int row, int column);

void retention_info_init_v2(FunctionContext* context, StringVal* agg_info);
void retention_info_merge_v2(FunctionContext* context, const StringVal& src_agg_info,
                             StringVal* dst_agg_info);
StringVal retention_info_finalize_v2(FunctionContext* context, const StringVal& dst_agg_info);
void retention_info_update_v2(FunctionContext* context, BigIntVal& from_time, StringVal& unit,
                              TinyIntVal& retention_range, BigIntVal& event_time, IntVal& type,
                              StringVal* agg_info);
}  // namespace doris_udf

#endif

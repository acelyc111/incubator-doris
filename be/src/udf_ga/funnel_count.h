#pragma once

#ifndef DORIS_BE_UDF_UDA_FUNNEL_COUNT
#define DORIS_BE_UDF_UDA_FUNNEL_COUNT

#include <iostream>
#include <list>
#include <time.h>
#include <unordered_map>

#include "common/logging.h"
#include "udf/udf.h"
#include "udf_ga/funnel_info.h"

using namespace std;
namespace doris_udf {
struct FunnelCountAgg {
    unordered_map<int, long> _result_map;
};

void parse_from_info(StringVal* val, FunnelCountAgg* agg);
void merge(FunnelCountAgg* src, FunnelCountAgg* dst);
void parse_from_count(const StringVal* val, FunnelCountAgg* agg);
void convert_agg_to_string(FunctionContext* context, FunnelCountAgg* src, StringVal* val);

void funnel_count_init(FunctionContext* context, StringVal* val);
void funnel_count_update(FunctionContext* context, const StringVal& src, StringVal* val);
void funnel_count_merge(FunctionContext* context, const StringVal& src, StringVal* dst);
StringVal funnel_count_finalize(FunctionContext* context, const StringVal& val);
inline void parse_row_column(int v, int* row, int* column);
}

#endif

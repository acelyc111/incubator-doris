#pragma once

#ifndef DORIS_BE_UDF_UDA_RETENTION_COUNT
#define DORIS_BE_UDF_UDA_RETENTION_COUNT

#include "udf/udf.h"
#include "udf_ga/short_encode.h"
#include <iostream>
#include <list>
#include <set>
#include <string.h>
#include <unordered_map>
#include <vector>

#include "common/logging.h"

using namespace std;
#define array_size 40

namespace doris_udf {

struct RetentionCountAgg {
    unordered_map<short, long> _result_map;
};

inline void parse_row_column(int v, int* row, int* column);
void retention_count_init(FunctionContext* context, StringVal* val);
void retention_count_update(FunctionContext* context, StringVal& info, StringVal* count_agg);
void retention_count_merge(FunctionContext* context, const StringVal& src_agg_count,
                           StringVal* dst_agg_count);
void parse_from_count_v1(const StringVal* val, RetentionCountAgg* agg);
inline void parse_retention_row_column(int v, int* row, int* column);
StringVal retention_count_finalize(FunctionContext* context, const StringVal& agg_count);
short make_value_r(int row, int column);
}

#endif

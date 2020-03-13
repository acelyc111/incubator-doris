#pragma once

#ifndef DORIS_BE_UDF_UDA_RETENTION_INFO
#define DORIS_BE_UDF_UDA_RETENTION_INFO

#include <iostream>
#include <list>
#include <set>
#include <string.h>
#include <vector>

#include "udf/udf.h"
#include "udf_ga/short_encode.h"

using namespace std;
#define array_size 40

namespace doris_udf {

struct RetentionInfoAgg {
    long first_array[array_size];
    long second_array[array_size];
    bool same_event = false;
    RetentionInfoAgg() {}

    void reset() {
        for (int i = 0; i < array_size; i++) {
            first_array[i] = 0;
            second_array[i] = 0;
        }
    }
};
typedef int ts_type;

void add_first_event(long start_time, string unit, long event_time, StringVal* info_agg);
void add_second_event(long start_time, string unit, long event_time, StringVal* info_agg);
void set_same_event(StringVal* info_agg);

void init_stringval(FunctionContext* context, StringVal* info_agg);

short make_value(int row, int column);

void retention_info_init(FunctionContext* context, StringVal* info_val);
void retention_info_update(FunctionContext* context, BigIntVal& from_time, StringVal& unit,
                           BigIntVal& event_time, IntVal& type, StringVal* info_agg);
void retention_info_merge(FunctionContext* context, const StringVal& src_agg_info,
                          StringVal* dst_agg_info);
StringVal retention_info_finalize(FunctionContext* context, const StringVal& agg_info);
}

#endif

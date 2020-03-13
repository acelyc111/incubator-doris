#pragma once

#ifndef DORIS_BE_UDF_UDA_FIRST_TIME_VALUE
#define DORIS_BE_UDF_UDA_FIRST_TIME_VALUE

#include "udf/udf.h"

namespace doris_udf {

typedef struct _Agg {
    uint64_t ts;
    char value[512];
} Agg;

inline void local_copy(char* dst, char* src, size_t len);

void first_time_value_init(FunctionContext* context, StringVal* val);
void first_time_value_update(FunctionContext* context, const BigIntVal& ts, const StringVal& src,
                             const IntVal& skip, StringVal* val);
StringVal first_time_value_serialize(FunctionContext* context, const StringVal& val);
void first_time_value_merge(FunctionContext* context, const StringVal& src, StringVal* dst);
StringVal first_time_value_finalize(FunctionContext* context, const StringVal& val);
}

#endif

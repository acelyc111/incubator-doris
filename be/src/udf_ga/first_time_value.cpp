#include "udf_ga/first_time_value.h"
#include "common/logging.h"
#include <string>

namespace doris_udf {

void first_time_value_init(FunctionContext* context, StringVal* val) {
    val->is_null = false;
    val->ptr = (uint8_t*)new Agg();
    val->len = sizeof(Agg);
}

void copy(char* dst, const char* src, size_t len) {
    size_t i = 0;

    for (; i < len; i++) {
        dst[i] = src[i];
    }

    dst[i] = '\0';
}

void first_time_value_update(FunctionContext* context, const BigIntVal& ts, const StringVal& src,
                             const IntVal& skip, StringVal* val) {
    uint64_t _ts = ts.val;
    if (skip.val < 0) {
        return;
    }

    Agg* agg = reinterpret_cast<Agg*>(val->ptr);

    if (agg->ts == 0 || strlen(agg->value) == 0 || _ts < agg->ts) {
        agg->ts = _ts;
        if (src.len > 512) {
            LOG(ERROR) << "udaf first_time_value byfield value is larger than 512";
            return;
        }
        copy(agg->value, (const char*)src.ptr, src.len);
        // printf("byField copy value:%s, src len:%d\n", agg->value, src.len);
    }
}

StringVal first_time_value_serialize(FunctionContext* context, const StringVal& val) {
    StringVal rst;
    Agg* agg = reinterpret_cast<Agg*>(val.ptr);

    rst.append(context, (const uint8_t*)agg, sizeof(Agg));
    delete agg;

    return rst;
}

void first_time_value_merge(FunctionContext* context, const StringVal& src, StringVal* dst) {
    Agg* src_agg = reinterpret_cast<Agg*>(src.ptr);
    Agg* dst_agg = reinterpret_cast<Agg*>(dst->ptr);

    if ((dst_agg->ts == 0 || strlen(dst_agg->value) == 0) || src_agg->ts < dst_agg->ts) {
        dst_agg->ts = src_agg->ts;
        copy(dst_agg->value, src_agg->value, strlen(src_agg->value));
    }
}

StringVal first_time_value_finalize(FunctionContext* context, const StringVal& val) {
    Agg* src_agg = reinterpret_cast<Agg*>(val.ptr);
    StringVal rst;
    rst.append(context, (const uint8_t*)src_agg->value, strlen(src_agg->value));
    delete src_agg;
    return rst;
}
}

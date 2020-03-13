#include "udf_ga/retention_info.h"

namespace doris_udf {

void add_first_event(long start_time, string unit, long event_time, StringVal* info_agg) {
    int n = get_delta_periods(start_time, event_time, unit);
    if (n >= 0 && n < array_size) {
        long* ts = (long*)(info_agg->ptr + n * sizeof(long));
        if (*ts == 0 || *ts > event_time) {
            *ts = event_time;
        }
    }
}

void add_second_event(long start_time, string unit, long event_time, StringVal* info_agg) {
    int n = get_delta_periods(start_time, event_time, unit);
    if (n >= 0 && n < array_size) {
        long* ts = (long*)(info_agg->ptr + n * sizeof(long) + array_size * sizeof(long));
        if (*ts == 0 || *ts < event_time) {
            *ts = event_time;
        }
    }
}

void set_same_event(StringVal* info_agg) {
    long* same_event = (long*)(info_agg->ptr + 2 * array_size * sizeof(long));
    *same_event = 1;
}

void retention_info_init(FunctionContext* context, StringVal* info_val) {
    info_val->ptr = NULL;
    info_val->is_null = false;
    info_val->len = 0;
}

void init_stringval(FunctionContext* context, StringVal* info_agg) {
    if (info_agg->len == 0) {
        long buf[array_size] = {0};
        info_agg->append(context, (const uint8_t*)buf, size_t(array_size * sizeof(long)));
        info_agg->append(context, (const uint8_t*)buf, size_t(array_size * sizeof(long)));
        info_agg->append(context, (const uint8_t*)buf, sizeof(long));
    }
}

void retention_info_update(FunctionContext* context, BigIntVal& start_time, StringVal& unit,
                           BigIntVal& event_time, IntVal& type, StringVal* info_agg) {
    if (0 == type.val) return;
    init_stringval(context, info_agg);
    string unit_str((const char*)unit.ptr, unit.len);
    switch (type.val) {
        case 1:
            add_first_event(start_time.val, unit_str, event_time.val, info_agg);
            break;
        case 2:
            add_second_event(start_time.val, unit_str, event_time.val, info_agg);
            break;
        case 3:
            add_first_event(start_time.val, unit_str, event_time.val, info_agg);
            add_second_event(start_time.val, unit_str, event_time.val, info_agg);
            set_same_event(info_agg);
            break;
    }
}

void retention_info_merge(FunctionContext* context, const StringVal& src_agg_info,
                          StringVal* dst_agg_info) {
    init_stringval(context, dst_agg_info);
    if (src_agg_info.len == 0) return;
    for (int i = 0; i < array_size; i++) {
        long first_index = i * sizeof(long);
        long* dst_first = (long*)(dst_agg_info->ptr + first_index);
        long* src_first = (long*)(src_agg_info.ptr + first_index);
        // make value smaller
        if (*dst_first == 0 || (*src_first > 0 && *dst_first > *src_first)) {
            *dst_first = *src_first;
        }

        long second_index = i * sizeof(long) + array_size * sizeof(long);
        long* dst_second = (long*)(dst_agg_info->ptr + second_index);
        long* src_second = (long*)(src_agg_info.ptr + second_index);
        // make value bigger
        if (*dst_second == 0 || (*src_second > 0 && *dst_second < *src_second)) {
            *dst_second = *src_second;
        }
    }

    int same_event_index = array_size * sizeof(long) * 2;
    long* src_same_event = (long*)(src_agg_info.ptr + same_event_index);
    long* dst_same_event = (long*)(dst_agg_info->ptr + same_event_index);
    if (*src_same_event == 1) {
        *dst_same_event = 1;
    }
}

// column maybe negative value
short make_value(int row, int column) {
    short rst = 0;
    if (column < 0) {
        rst = (short)((-1) * (row * 100 + (-1) * column));
    } else {
        rst = (short)(row * 100 + column);
    }
    // printf("make_value, row:%d, column:%d, rst:%d\n", row, column, rst);
    return rst;
}

StringVal retention_info_finalize(FunctionContext* context, const StringVal& agg_info) {
    int same_event_index = array_size * sizeof(long) * 2;
    long* same_event = (long*)(agg_info.ptr + same_event_index);
    bool same = false;
    if (*same_event == 1) {
        same = true;
    }

    // warning!!!
    set<short> local_set;
    int row_max_index = array_size - 1, column_max_index = array_size - 1;
    for (; row_max_index >= 0; row_max_index--) {
        long* ts = (long*)(agg_info.ptr + row_max_index * sizeof(long));
        if (*ts == 0) {
            continue;
        } else {
            break;
        }
    }

    for (; column_max_index >= 0; column_max_index--) {
        long* ts =
            (long*)(agg_info.ptr + column_max_index * sizeof(long) + array_size * sizeof(long));
        if (*ts == 0) {
            continue;
        } else {
            break;
        }
    }

    for (int row = 0; row <= row_max_index; row++) {
        long* min_ts = (long*)(agg_info.ptr + row * sizeof(long));
        if (*min_ts <= 0) {
            // printf("retention_info_finalize, row:%d\n", row);
            continue;
        }
        local_set.insert(make_value(row, -1));

        if (same) {
            local_set.insert(make_value(row, 0));
        }

        for (int column = row; column <= column_max_index; column++) {
            long* second_ts =
                (long*)(agg_info.ptr + column * sizeof(long) + array_size * sizeof(long));
            if (*second_ts > *min_ts) {
                local_set.insert(make_value(row, column - row));
            }
        }
    }

    StringVal rst;
    for (set<short>::iterator iter = local_set.begin(); iter != local_set.end(); iter++) {
        short value = *iter;
        string encoded_str = encode((unsigned char*)&value, sizeof(short));
        rst.append(context, (uint8_t*)(encoded_str.c_str()), encoded_str.length());
    }

    return rst;
}
}

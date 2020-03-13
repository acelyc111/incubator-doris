#include "udf_ga/retention_info_v2.h"

namespace doris_udf {
void add_start_event(long start_time, const string& unit, long event_time, StringVal* agg_info) {
    int idx = get_delta_periods(start_time, event_time, unit);
    if (idx >= 0 && idx < start_event_size) {
        auto start_event = reinterpret_cast<uint64_t*>(agg_info->ptr) + 1;
        (*start_event) |= (uint64_mask << (start_event_size - 1U - idx));
    }
}

void add_end_event(long start_time, const string& unit, long event_time, StringVal* agg_info) {
    int deltaPeriods = get_delta_periods(start_time, event_time, unit);
    if (deltaPeriods >= 0 && deltaPeriods < end_event_size * end_event_parts) {
        int part = deltaPeriods / end_event_size;
        int idx = deltaPeriods % end_event_size;
        auto end_event = reinterpret_cast<uint64_t*>(agg_info->ptr) + 2 + part;
        (*end_event) |= (uint64_mask << (end_event_size - 1U - idx));
    }
}

void retention_info_init_v2(FunctionContext* context, StringVal* agg_info) {
    agg_info->is_null = false;
    auto agg_info_size = sizeof(RetentionInfoAgg);
    agg_info->ptr = context->allocate(agg_info_size);
    agg_info->len = agg_info_size;
    memset(agg_info->ptr, 0, agg_info_size);
}

void retention_info_update_v2(FunctionContext* context, BigIntVal& start_time, StringVal& unit,
                              TinyIntVal& retention_range, BigIntVal& event_time, IntVal& type,
                              StringVal* agg_info) {
    if (0 == type.val || agg_info == nullptr) return;
    auto range = reinterpret_cast<short*>(agg_info->ptr);
    (*range) = retention_range.val;
    string unit_str((const char*)unit.ptr, unit.len);
    switch (type.val) {
        // 输入事件为开始事件
        case 1:
            add_start_event(start_time.val, unit_str, event_time.val, agg_info);
            break;
        // 输入事件为结束事件
        case 2:
            add_end_event(start_time.val, unit_str, event_time.val, agg_info);
            break;
        // 开始事件与结束事件为相同事件
        case 3:
            add_start_event(start_time.val, unit_str, event_time.val, agg_info);
            add_end_event(start_time.val, unit_str, event_time.val, agg_info);
            break;
        default:
            break;
    }
}

void retention_info_merge_v2(FunctionContext* context, const StringVal& src_agg_info,
                             StringVal* dst_agg_info) {
    if (src_agg_info.len == 0 || src_agg_info.ptr == nullptr) {
        return;
    }

    auto src_retention_range = reinterpret_cast<short*>(src_agg_info.ptr);
    auto dst_retention_range = reinterpret_cast<short*>(dst_agg_info->ptr);
    (*dst_retention_range) = (*src_retention_range);

    // 更新开始事件与结束事件发生日期
    auto src_start_event = reinterpret_cast<uint64_t*>(src_agg_info.ptr) + 1;
    auto dst_start_event = reinterpret_cast<uint64_t*>(dst_agg_info->ptr) + 1;
    (*dst_start_event) |= (*src_start_event);
    for (int i = 0; i < end_event_parts; ++i) {
        auto src_end_event = reinterpret_cast<uint64_t*>(src_agg_info.ptr) + 2 + i;
        auto dst_end_event = reinterpret_cast<uint64_t*>(dst_agg_info->ptr) + 2 + i;
        (*dst_end_event) |= (*src_end_event);
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
    return rst;
}

StringVal retention_info_finalize_v2(FunctionContext* context, const StringVal& dst_agg_info) {
    auto retention_range = reinterpret_cast<short*>(dst_agg_info.ptr);
    auto start_event = reinterpret_cast<uint64_t*>(dst_agg_info.ptr) + 1;
    auto end_event = reinterpret_cast<uint64_t*>(dst_agg_info.ptr) + 2;
    // warning!!!
    set<short> local_set;

    int row_max_idx = (*start_event == 0) ? -1 : get_last_not_zero_idx_of_uint64(*start_event);

    for (int row = 0; row <= row_max_idx; ++row) {
        if (!get_idx_mask_of_uint64(*start_event, row)) {
            continue;
        }

        local_set.insert(make_value(row, -1));

        for (int col = row; col <= row + *retention_range; ++col) {
            int part = col / end_event_size;
            int idx = col % end_event_size;
            if (get_idx_mask_of_uint64(*(end_event + part), idx)) {
                local_set.insert(make_value(row, col - row));
            }
        }
    }

    StringVal rst;
    for (short value : local_set) {
        string encoded_str = encode((unsigned char*)&value, sizeof(short));
        rst.append(context, (uint8_t*)(encoded_str.c_str()), encoded_str.length());
    }
    return rst;
}
}  // namespace doris_udf

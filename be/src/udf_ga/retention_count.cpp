#include "udf_ga/retention_count.h"

namespace doris_udf {
void retention_count_init(FunctionContext* context, StringVal* val) {
    val->ptr = NULL;
    val->is_null = false;
    val->len = 0;
}

void parse_from_count_info(const StringVal* val, set<short>* local) {
    uint8_t* index = val->ptr;
    uint8_t* end_index = val->ptr + val->len;
    while (index < end_index) {
        short v = decode((char*)index, sizeof(short) * 2);
        index += sizeof(short) * 2;
        local->insert(v);
    }
}

void parse_from_count(const StringVal* val, RetentionCountAgg* agg) {
    string content((const char*)(val->ptr), val->len);
    vector<string> all_str = split(content, ";");
    for (int i = 0; i < all_str.size(); i++) {
        vector<string> details = split(all_str[i], ":");
        int row_column = 0, count = 0;
        sscanf(all_str[i].c_str(), "%d:%d", &row_column, &count);

        unordered_map<short, long>::iterator detail_map_iter = agg->_result_map.find(row_column);
        if (detail_map_iter == agg->_result_map.end()) {
            agg->_result_map.insert(make_pair(row_column, count));
        } else {
            detail_map_iter->second = detail_map_iter->second + count;
        }
    }
}

void parse_from_count_v1(const StringVal* val, RetentionCountAgg* agg) {
    // time_t parse_start = time(NULL);
    int rst[51][51] = {0};
    int row = 0, column = 0;
    char* start = (char*)val->ptr;
    char* end = (char*)val->ptr + val->len;
    char* tmp_end = start;
    char tmp_char = '0';
    int row_column = 0;
    while (start < end) {
        while (*tmp_end != ';' && *tmp_end != ':') tmp_end++;
        tmp_char = *tmp_end;
        *tmp_end = '\0';
        row_column = atoi(start);
        parse_retention_row_column(row_column, &row, &column);

        if (tmp_char == ':') {
            start = tmp_end + 1;
            while (*tmp_end != ';') tmp_end++;
            *tmp_end = '\0';
            rst[row + 1][column + 1] += atoi(start);
        } else {
            rst[row + 1][column + 1] += 1;
        }
        start = tmp_end + 1;
    }

    // time_t parse_arr= time(NULL);

    for (int r = 0; r < 51; r++) {
        for (int c = 0; c < 51; c++) {
            if (rst[r][c] <= 0) continue;
            short row_column = make_value_r(r - 1, c - 1);
            unordered_map<short, long>::iterator detail_map_iter =
                agg->_result_map.find(row_column);
            if (detail_map_iter == agg->_result_map.end()) {
                agg->_result_map.insert(make_pair(row_column, rst[r][c]));
            } else {
                detail_map_iter->second = detail_map_iter->second + rst[r][c];
            }
        }
    }
}

void convert_agg_to_string(FunctionContext* context, RetentionCountAgg* src, StringVal* val) {
    char buffer[128] = "";

    if (val->ptr != NULL) {
        context->free(val->ptr);
        val->ptr = NULL;
        val->len = 0;
    }

    for (unordered_map<short, long>::iterator iter = src->_result_map.begin();
         iter != src->_result_map.end(); iter++) {
        int row_column = iter->first;
        int count = iter->second;
        sprintf(buffer, "%d:%d;", row_column, count);
        val->append(context, (uint8_t*)buffer, strlen(buffer));
    }
}

inline short make_value_r(int row, int column) {
    short rst = 0;
    if (column < 0) {
        rst = (short)((-1) * (row * 100 + (-1) * column));
    } else {
        rst = (short)(row * 100 + column);
    }
    return rst;
}

void retention_count_update(FunctionContext* context, StringVal& src_info, StringVal* dst_count) {
    RetentionCountAgg agg_src;
    set<short> src_row_column;
    parse_from_count_info(&src_info, &src_row_column);

    if (dst_count->len > 10 * 1024 * 1024) {
        // 10M
        RetentionCountAgg agg_dst;
        parse_from_count_v1(dst_count, &agg_dst);
        convert_agg_to_string(context, &agg_dst, dst_count);
    }
    char buffer[32];
    for (set<short>::iterator iter = src_row_column.begin(); iter != src_row_column.end(); iter++) {
        sprintf(buffer, "%d;", *iter);
        dst_count->append(context, (const uint8_t*)buffer, strlen(buffer));
    }
}

void retention_count_merge(FunctionContext* context, const StringVal& src_count,
                           StringVal* dst_count) {
    dst_count->append(context, (uint8_t*)src_count.ptr, src_count.len);
}

inline void parse_retention_row_column(int v, int* row, int* column) {
    if (v > 0) {
        *column = v % 100;
        *row = v / 100;
    } else {
        *column = (-1) * ((-1 * v) % 100);
        *row = ((-1) * v / 100);
    }
}

void agg_output(FunctionContext* context, RetentionCountAgg* src, StringVal* val) {
    char buffer[128] = "";

    if (val->ptr != NULL) {
        context->free(val->ptr);
        val->ptr = NULL;
        val->len = 0;
    }

    for (unordered_map<short, long>::iterator iter = src->_result_map.begin();
         iter != src->_result_map.end(); iter++) {
        int v = iter->first;
        int row = 0, column = 0;
        parse_retention_row_column(v, &row, &column);
        int count = iter->second;
        sprintf(buffer, "%d:%d:%d;", row, column, count);
        val->append(context, (uint8_t*)buffer, strlen(buffer));
    }
}

StringVal retention_count_finalize(FunctionContext* context, const StringVal& agg_count) {
    RetentionCountAgg agg;
    parse_from_count_v1(&agg_count, &agg);
    StringVal rst;
    agg_output(context, &agg, &rst);
    return rst;
}
}

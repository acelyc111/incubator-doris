#include "udf_ga/funnel_count.h"

namespace doris_udf {

void funnel_count_init(FunctionContext* context, StringVal* val) {
    val->is_null = false;
    val->ptr = NULL;
    val->len = 0;
}

// parse from "funnelm/8=ZQA="" to FunnelCountAgg
void parse_from_info(const StringVal* val, set<short>* local) {
    uint8_t* index = val->ptr + tag_size;
    uint8_t* end_index = val->ptr + val->len;
    // cout << __FILE__  << __LINE__ << ":" << val->len << endl;
    while (index < end_index) {
        short v = decode((char*)index, sizeof(short) * 2);
        index += sizeof(short) * 2;
        local->insert(v);
    }
}

void parse_from_count(const StringVal* val, FunnelCountAgg* agg) {
    string content((const char*)(val->ptr), val->len);
    vector<string> all_str = split(content, ";");
    for (int i = 0; i < all_str.size(); i++) {
        vector<string> details = split(all_str[i], ":");
        int row_column = atoi(details[0].c_str());
        int count = 1;
        if (details.size() == 2) {
            count = atoi(details[1].c_str());
        }

        unordered_map<int, long>::iterator detail_map_iter = agg->_result_map.find(row_column);
        if (detail_map_iter == agg->_result_map.end()) {
            agg->_result_map.insert(make_pair(row_column, count));
        } else {
            detail_map_iter->second = detail_map_iter->second + count;
        }
    }
}

void parse_from_count_v1(const StringVal* val, FunnelCountAgg* agg) {
    // time_t parse_start = time(NULL);
    string content((const char*)(val->ptr), val->len);
    vector<string> all_str = split(content, ";");
    // time_t parse_split= time(NULL);
    int rst[51][51] = {0};
    int row = 0, column = 0;
    for (int i = 0; i < all_str.size(); i++) {
        vector<string> details = split(all_str[i], ":");
        int row_column = atoi(details[0].c_str());
        parse_row_column(row_column, &row, &column);
        int count = 1;
        if (details.size() == 2) {
            count = atoi(details[1].c_str());
        }
        rst[row + 1][column + 1] += count;
    }
    // time_t parse_arr= time(NULL);

    for (int r = 0; r < 51; r++) {
        for (int c = 0; c < 51; c++) {
            if (rst[r][c] <= 0) continue;
            short row_column = make_value_f(r - 1, c - 1);
            // LOG(INFO) << "r:" << r << ", c:" << c << ", row_column:" << row_column;
            unordered_map<int, long>::iterator detail_map_iter = agg->_result_map.find(row_column);
            if (detail_map_iter == agg->_result_map.end()) {
                agg->_result_map.insert(make_pair(row_column, rst[r][c]));
            } else {
                detail_map_iter->second = detail_map_iter->second + rst[r][c];
            }
        }
    }
    // time_t parse_final = time(NULL);
    // LOG(INFO) << "content length" << content.length() << "parse time cost
    // split:" << parse_split - parse_start << ", make arr" << parse_arr -
    // parse_split << ", final:" << parse_final - parse_arr;
}

void parse_from_count_v2(const StringVal* val, FunnelCountAgg* agg) {
    if (val->len <= 0 || val->ptr == NULL) return;
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
        parse_row_column(row_column, &row, &column);

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
            short row_column = make_value_f(r - 1, c - 1);
            // LOG(INFO) << "r:" << r << ", c:" << c << ", row_column:" << row_column;
            unordered_map<int, long>::iterator detail_map_iter = agg->_result_map.find(row_column);
            if (detail_map_iter == agg->_result_map.end()) {
                agg->_result_map.insert(make_pair(row_column, rst[r][c]));
            } else {
                detail_map_iter->second = detail_map_iter->second + rst[r][c];
            }
        }
    }
}

void convert_agg_to_string(FunctionContext* context, FunnelCountAgg* src, StringVal* val) {
    char buffer[128] = "";

    // truncate val content
    if (val->ptr != NULL) {
        context->free(val->ptr);
        val->ptr = NULL;
        val->len = 0;
        // val->capacity = 0;
    }

    for (unordered_map<int, long>::iterator iter = src->_result_map.begin();
         iter != src->_result_map.end(); iter++) {
        int row_column = iter->first;
        int count = iter->second;
        sprintf(buffer, "%d:%d;", row_column, count);
        val->append(context, (uint8_t*)buffer, strlen(buffer));
    }
}

// src format:funnelm/8=ZQA=
// dst format:1:2:100; 3:4:301;
void funnel_count_update(FunctionContext* context, const StringVal& src, StringVal* val) {
    if (src.len <= tag_size) {
        return;
    }

    FunnelCountAgg agg_src;
    set<short> src_row_column;
    parse_from_info(&src, &src_row_column);

    if (val->len > 1024 * 1024) {
        FunnelCountAgg agg_dst;
        parse_from_count_v1(val, &agg_dst);
        convert_agg_to_string(context, &agg_dst, val);
    }
    char buffer[32];
    for (set<short>::iterator iter = src_row_column.begin(); iter != src_row_column.end(); iter++) {
        sprintf(buffer, "%d;", *iter);
        val->append(context, (const uint8_t*)buffer, strlen(buffer));
    }
}

void funnel_count_merge(FunctionContext* context, const StringVal& src, StringVal* dst) {
    // FunnelCountAgg agg_src, agg_dst;
    dst->append(context, (uint8_t*)src.ptr, src.len);
}

inline void parse_row_column(int v, int* row, int* column) {
    if (v > 0) {
        *column = v % 100;
        *row = v / 100;
    } else {
        *column = (-1 * v) % 100;
        *row = (-1) * ((-1) * v / 100);
    }
}

void agg_output(FunctionContext* context, FunnelCountAgg* src, StringVal* val) {
    char buffer[128] = "";

    // truncate val content
    if (val->ptr != NULL) {
        context->free(val->ptr);
        val->ptr = NULL;
        val->len = 0;
    }

    for (unordered_map<int, long>::iterator iter = src->_result_map.begin();
         iter != src->_result_map.end(); iter++) {
        int v = iter->first;
        int row = 0, column = 0;
        parse_row_column(v, &row, &column);
        int count = iter->second;
        sprintf(buffer, "%d:%d:%d;", row, column, count);
        val->append(context, (uint8_t*)buffer, strlen(buffer));
    }
}

StringVal funnel_count_finalize(FunctionContext* context, const StringVal& val) {
    // time_t finalize_start = time(NULL);
    FunnelCountAgg agg;
    parse_from_count_v2(&val, &agg);
    // time_t finalize_parse= time(NULL);
    StringVal rst;
    agg_output(context, &agg, &rst);
    return rst;
}
}

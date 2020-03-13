#pragma once

#ifndef DORIS_BE_UDF_UDA_ENCODE
#define DORIS_BE_UDF_UDA_ENCODE

#include <cstring>
#include <iostream>
#include <string>
#include <vector>

namespace doris_udf {
using namespace std;
const time_t UTC16 = 16 * 60 * 60 * 1000;
const time_t UTC8 = 8 * 60 * 60 * 1000;
const time_t DAY_TIME_MS = 24 * 60 * 60 * 1000;
const time_t WEEK_TIME_MS = 24 * 60 * 60 * 7 * 1000;
const char decode_table[] = {
    -2, -2, -2, -2, -2, -2, -2, -2, -2, -1, -1, -2, -2, -1, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
    -2, -2, -2, -2, -2, -2, -2, -2, -1, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 62, -2, -2, -2, 63,
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -2, -2, -2, -2, -2, -2, -2, 0,  1,  2,  3,  4,  5,  6,
    7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -2, -2, -2, -2, -2,
    -2, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
    49, 50, 51, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
    -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
    -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
    -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
    -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
    -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2};

const string _base64_table = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
;
static const char base64_pad = '=';

vector<string> split(const string& str, const string& delim);
string encode(const unsigned char* str, int bytes);

long decode(const char* data, size_t length);

long get_start_of_day(long ts);
long get_start_of_week(long ts);
int get_delta_periods(long start_time, long event_time, string unit);
/*
void main() {
    short data = 5;
    string s = encode((const unsigned char*)(&data), sizeof(short));
    short rst = decode(s.c_str(), s.length());
    cout <<"from origin data:" << data << " to:" << rst << endl;
} */
}

#endif

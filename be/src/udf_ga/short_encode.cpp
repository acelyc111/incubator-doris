#include <ctime>

#include "udf_ga/short_encode.h"

namespace doris_udf {
using namespace std;

vector<string> split(const string& str, const string& delim) {
    vector<string> res;
    if ("" == str) return res;
    char* strs = new char[str.length() + 1];
    strcpy(strs, str.c_str());

    char* d = new char[delim.length() + 1];
    strcpy(d, delim.c_str());

    char* p = strtok(strs, d);
    while (p) {
        string s = p;
        res.push_back(s);
        p = strtok(NULL, d);
    }

    return res;
}

string encode(const unsigned char* str, int bytes) {
    std::string _encode_result;
    const unsigned char* current;
    current = str;
    int origin_bytes = 2 * bytes;
    while (bytes > 2) {
        _encode_result += _base64_table[current[0] >> 2];
        _encode_result += _base64_table[((current[0] & 0x03) << 4) + (current[1] >> 4)];
        _encode_result += _base64_table[((current[1] & 0x0f) << 2) + (current[2] >> 6)];
        _encode_result += _base64_table[current[2] & 0x3f];

        current += 3;
        bytes -= 3;
    }
    if (bytes > 0) {
        _encode_result += _base64_table[current[0] >> 2];
        if (bytes % 3 == 1) {
            _encode_result += _base64_table[(current[0] & 0x03) << 4];
            _encode_result += "==";
        } else if (bytes % 3 == 2) {
            _encode_result += _base64_table[((current[0] & 0x03) << 4) + (current[1] >> 4)];
            _encode_result += _base64_table[(current[1] & 0x0f) << 2];
            _encode_result += "=";
        }
    }

    if (_encode_result.length() < origin_bytes) {
        int cur = _encode_result.length();
        while (cur < origin_bytes) {
            _encode_result += "=";
            cur++;
        }
    }

    return _encode_result;
}

long decode(const char* data, size_t length) {
    const char* current = data;
    char decoded_data[17] = "";
    int ch = 0, i = 0, j = 0, k = 0;
    int origin_len = length;
    while ((ch = *current++) != '\0' && length-- > 0) {
        if (ch == base64_pad) {
            if (*current != '=' && (i % 4) == 1) {
                return -1;
            }
            continue;
        }

        ch = decode_table[ch];
        if (ch == -1) {
            continue;
        } else if (ch == -2) {
            return -1;
        }

        switch (i % 4) {
            case 0:
                decoded_data[j] = ch << 2;
                break;
            case 1:
                decoded_data[j++] |= ch >> 4;
                decoded_data[j] = (ch & 0x0f) << 4;
                break;
            case 2:
                decoded_data[j++] |= ch >> 2;
                decoded_data[j] = (ch & 0x03) << 6;
                break;
            case 3:
                decoded_data[j++] |= ch;
                break;
            default:
                break;
        }

        i++;
    }

    k = j;
    /* mop things up if we ended on a boundary */
    if (ch == base64_pad) {
        switch (i % 4) {
            case 1:
                return 0;
            case 2:
                k++;
            case 3:
                decoded_data[k] = 0;
            default:
                break;
        }
    }

    decoded_data[j] = '\0';
    if (origin_len == 4) {
        return *((short*)decoded_data);
    } else if (origin_len == 8) {
        return *((int*)decoded_data);
    } else if (origin_len == 16) {
        return *((long*)decoded_data);
    } else {
        return -1;
    }
}

long get_start_of_day(long ts) {
    long mod = (ts - UTC16) % DAY_TIME_MS;
    return ts - mod;
}

long get_start_of_week(long ts) {
    // 1900-01-01为星期四
    long mod = (ts + UTC8 - 4 * 24 * 60 * 60 * 1000) % WEEK_TIME_MS;
    return ts - mod;
}

int get_delta_periods(long start_time, long event_time, string unit) {
    if (unit == "day") {
        return (int)((get_start_of_day(event_time) - get_start_of_day(start_time)) / DAY_TIME_MS);
    } else if (unit == "week") {
        return (int)((get_start_of_week(event_time) - get_start_of_week(start_time))
                     / WEEK_TIME_MS);
    } else if (unit == "month") {
        start_time /= 1000;
        event_time /= 1000;
        std::tm start_tm = *localtime(&start_time);
        std::tm event_tm = *localtime(&event_time);
        return event_tm.tm_mon - start_tm.tm_mon;
    }
    return -1;
}
/*
void main() {
    short data = 5;
    string s = encode((const unsigned char*)(&data), sizeof(short));
    short rst = decode(s.c_str(), s.length());
    cout <<"from origin data:" << data << " to:" << rst << endl;
} */
}

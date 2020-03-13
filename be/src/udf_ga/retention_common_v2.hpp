//
// Created by wangcong on 2019/10/30.
//

#ifndef DORIS_RETENTION_COMMON_V2_HPP
#define DORIS_RETENTION_COMMON_V2_HPP

typedef unsigned __int128 uint128_t;

const int start_event_size = 64;
const int end_event_size = 64;
const int end_event_parts = 2;
uint128_t uint128_mask = 1;
uint64_t uint64_mask = 0x1ULL;

// idx从左往右从0开始
inline bool get_idx_mask_of_uint64(uint64_t data, uint32_t idx) {
    return (data >> (64 - 1 - idx)) & uint64_mask;
}

inline bool get_idx_mask_of_uint128(const uint128_t& data, uint32_t idx) {
    return static_cast<bool>((data >> (128 - 1 - idx)) & uint128_mask);
}

int get_last_not_zero_idx_of_uint64(uint64_t data) {
    for (int idx = 64 - 1; idx >= 0; --idx) {
        if (get_idx_mask_of_uint64(data, idx)) {
            return idx;
        }
    }
    return -1;
}

int get_last_not_zero_idx_if_uint128(const uint128_t& data) {
    for (int idx = 128 - 1; idx >= 0; --idx) {
        if (get_idx_mask_of_uint128(data, idx)) {
            return idx;
        }
    }
    return -1;
}

void string_split(const std::string& str, std::vector<std::string>& tokens,
                  const std::string& delimiters = " ") {
    std::string::size_type first_pos = str.find_first_not_of(delimiters, 0);
    std::string::size_type second_pos = str.find_first_of(delimiters, first_pos);
    while (std::string::npos != second_pos || std::string::npos != first_pos) {
        tokens.emplace_back(str.substr(first_pos, second_pos - first_pos));
        first_pos = str.find_first_not_of(delimiters, second_pos);
        second_pos = str.find_first_of(delimiters, first_pos);
    }
}

void split_uint128(uint128_t src, uint64_t& dest1, uint64_t& dest2) {
    constexpr const uint128_t bottom_mask = (uint128_t{1} << 64U) - 1;
    constexpr const uint128_t top_mask = ~bottom_mask;
    dest1 = src & bottom_mask;
    dest2 = (src & top_mask) >> 64U;
}

void merge_uint128(uint64_t src1, uint64_t src2, uint128_t& dest) {
    dest = (uint128_t{src2} << 64U) | src1;
}

std::string encode_uint128(uint128_t src) {
    uint64_t high_level;
    uint64_t low_level;
    split_uint128(src, high_level, low_level);
    return std::to_string(high_level) + " " + std::to_string(low_level);
}

uint128_t decode_uint128(const std::string& src) {
    std::vector<std::string> tokens;
    string_split(src, tokens);
    uint64_t high_level = stoul(tokens[0]);
    uint64_t low_level = stoul(tokens[1]);
    uint128_t result;
    merge_uint128(high_level, low_level, result);
    return result;
}

// Base 128 Varints
int encode_varints(char* buf, uint64_t x) {
    int n = 0;
    while (x > 127) {
        buf[n++] = (char)(0x80U | (x & 0x7FU));
        x >>= 7U;
    }
    buf[n++] = (char)x;
    return n;
}

uint64_t decode_varints(const char* buf) {
    uint8_t shift = 0;
    int n = 0;
    uint64_t x = 0;
    for (; shift < 64; shift += 7) {
        auto c = (uint64_t)buf[n++];
        x |= (c & 0x7FU) << shift;
        if ((c & 0x80U) == 0) {
            break;
        }
    }
    return x;
}

#endif  // DORIS_RETENTION_COMMON_V2_HPP

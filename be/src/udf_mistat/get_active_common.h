//
// Created by wangcong on 2019/9/23.
//

#ifndef DORIS_GET_ACTIVE_COMMON_H
#define DORIS_GET_ACTIVE_COMMON_H

#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include "udf/udf.h"

namespace doris_udf {

inline std::string StringValToStdString(const StringVal& input) {
    return std::string(reinterpret_cast<char*>(input.ptr), input.len);
}

inline long GetTimeStamp(const std::string& src, const std::string& separator) {
    long result = 0;
    auto pos = src.find(separator);
    if (pos == std::string::npos) {
        return result;
    }

    try {
        result = std::stol(src.substr(0, pos));
    } catch (const std::invalid_argument& e) {
    }
    return result;
}

enum ACTIVE_INFO_STRATEGY { FIRST = 0, LAST = 1 };

struct ActiveInfo {
    std::string value;
    std::string separator = "\t";
    ACTIVE_INFO_STRATEGY strategy;

    ActiveInfo() : strategy(ACTIVE_INFO_STRATEGY::FIRST) {}

    explicit ActiveInfo(ACTIVE_INFO_STRATEGY activeInfoStrategy) : strategy(activeInfoStrategy) {}

    bool needUpdate(long otherTs) {
        if (strategy == ACTIVE_INFO_STRATEGY::FIRST) {
            return otherTs < GetTimeStamp(this->value, this->separator);
        } else if (strategy == ACTIVE_INFO_STRATEGY::LAST) {
            return otherTs > GetTimeStamp(this->value, this->separator);
        } else {
            // never reach here
            return false;
        }
    }

    void update(const BigIntVal& ts, const StringVal& origin) {
        this->value = std::to_string(ts.val) + this->separator + StringValToStdString(origin);
    }

    void update(const std::string& other) { this->value = other; }

    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar& value;
        ar& separator;
        ar& strategy;
    }
};

void GetActiveCommonUpdate(FunctionContext* context, const StringVal& separator,
                           const BigIntVal& ts, const StringVal& activeInfo, StringVal* origin);

StringVal GetActiveCommonSerialize(FunctionContext* context, const StringVal& activeInfo);
void GetActiveCommonMerge(FunctionContext* context, const StringVal& src, StringVal* activeInfo);
StringVal GetActiveCommonFinalize(FunctionContext* context, StringVal* activeInfo);
}

#endif  // DORIS_GET_ACTIVE_COMMON_H

//
// Created by wangcong on 2019/8/28.
//

#ifndef DORIS_GET_TIME_UTIL_H
#define DORIS_GET_TIME_UTIL_H

#include <string>
#include <vector>

namespace doris_udf {
const std::string connector(";"); /* NOLINT */
const std::string separator("_"); /* NOLINT */

template <typename T>
void UNUSED(T&&) {}

void processInvalidRange(std::vector<std::pair<long, long>>& inputRanges);
}

#endif  // DORIS_GET_TIME_UTIL_H

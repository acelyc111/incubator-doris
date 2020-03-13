//
// Created by mi on 19-7-24.
//

#ifndef DORIS_GET_PER_PAGE_TIME_H
#define DORIS_GET_PER_PAGE_TIME_H

#include "udf/udf.h"
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <string>
#include <vector>

namespace doris_udf {
template <typename T>
void UNUSED(T&&) {}

const std::string separator("_"); /* NOLINT */

void GetPerPageTimeInit(FunctionContext* context, StringVal* pageTimeInfo);

void GetPerPageTimeUpdate(FunctionContext* context, const BigIntVal& startTime,
                          const BigIntVal& endTime, StringVal* pageTimeInfo);

void GetPerPageTimeMerge(FunctionContext* context, const StringVal& src, StringVal* pageTimeInfo);

StringVal GetPerPageTimeFinalize(FunctionContext* context, const StringVal& totalTime);
}

#endif  // DORIS_GET_PER_PAGE_TIME_H

//
// Created by mi on 19-7-24.
//

#ifndef DORIS_GET_SESSION_TIME_H
#define DORIS_GET_SESSION_TIME_H

#include <iostream>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>

#include "udf/udf.h"
#include "udf_mistat/get_time_util.h"

namespace doris_udf {

unsigned long getSessionTimes(const std::string& inputString);

void GetSessionTimeInit(FunctionContext* context, StringVal* sessionInfo);

void GetSessionTimeUpdate(FunctionContext* context, const BigIntVal& startTime,
                          const BigIntVal& endTime, StringVal* sessionInfo);

void GetSessionTimeMerge(FunctionContext* context, const StringVal& src, StringVal* sessionInfo);

StringVal GetSessionTimeFinalize(FunctionContext* context, const StringVal& sessionInfo);
}

#endif  // DORIS_GET_SESSION_TIME_H

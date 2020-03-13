//
// Created by wangcong on 2019/8/28.
//

#ifndef DORIS_GET_PAGE_TIME_H
#define DORIS_GET_PAGE_TIME_H

#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/algorithm/string.hpp>

#include "common/logging.h"
#include "udf/udf.h"
#include "udf_mistat/get_time_util.h"

namespace doris_udf {
void GetPageTimeInit(FunctionContext* context, StringVal* dst);
void GetPageTimeUpdate(FunctionContext* context, const StringVal& pg, const BigIntVal& st,
                       const BigIntVal& et, StringVal* dst);
void GetPageTimeMerge(FunctionContext* context, const StringVal& src, StringVal* dst);
StringVal GetPageTimeFinalize(FunctionContext* context, StringVal* dst);
}

#endif  // DORIS_GET_PAGE_TIME_H

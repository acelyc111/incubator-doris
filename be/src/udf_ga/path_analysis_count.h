//
// Created by wangcong on 2019/9/18.
//

#ifndef DORIS_PATH_ANALYSIS_COUNT_H
#define DORIS_PATH_ANALYSIS_COUNT_H

#include <boost/algorithm/string.hpp>

#include "udf/udf.h"
#include "udf_ga/path_analysis_common.hpp"

namespace doris_udf {
void PathAnalysisCountInit(FunctionContext* context, StringVal* pathInfoAgg);
void PathAnalysisCountUpdate(FunctionContext* context, const StringVal& src,
                             StringVal* pathInfoAgg);
void PathAnalysisCountMerge(FunctionContext* context, const StringVal& src, StringVal* pathInfoAgg);
StringVal PathAnalysisCountFinalize(FunctionContext* context, const StringVal& pathInfoAgg);
}

#endif  // DORIS_PATH_ANALYSIS_COUNT_H

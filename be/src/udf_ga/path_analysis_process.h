//
// Created by wangcong on 2019/9/18.
//

#ifndef DORIS_PATH_ANALYSIS_PROCESS_H
#define DORIS_PATH_ANALYSIS_PROCESS_H

#include "udf/udf.h"
#include "udf_ga/path_analysis_common.hpp"

namespace doris_udf {

void PathAnalysisProcessInit(FunctionContext* context, StringVal* pathInfoAgg);
void PathAnalysisProcessUpdate(FunctionContext* context,
                               const StringVal& eventName,        //　传入的事件名
                               const BigIntVal& ts,               // 传入的时间戳
                               const IntVal& sessionInterval,     // 要切分的session时长
                               const StringVal& targetEventName,  // 起始/终止事件
                               const TinyIntVal& isReverse,       // 是否逆序
                               const IntVal& maxLevel,            // 最长路径层数
                               StringVal* pathInfoAgg);
StringVal PathAnalysisProcessSerialize(FunctionContext* context, const StringVal& pathInfoAgg);
void PathAnalysisProcessMerge(FunctionContext* context, const StringVal& src,
                              StringVal* pathInfoAgg);
StringVal PathAnalysisProcessFinalize(FunctionContext* context, StringVal* pathInfoAgg);

}  // namespace doris_udf

#endif  // DORIS_PATH_ANALYSIS_PROCESS_H

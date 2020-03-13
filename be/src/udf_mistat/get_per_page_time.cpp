//
// Created by mi on 19-7-24.
//

#include "get_per_page_time.h"

void doris_udf::GetPerPageTimeInit(FunctionContext* context, StringVal* pageTimeInfo) {
    UNUSED(context);
    pageTimeInfo->is_null = false;
    pageTimeInfo->ptr = nullptr;
    pageTimeInfo->len = 0;
}

void doris_udf::GetPerPageTimeUpdate(FunctionContext* context, const BigIntVal& startTime,
                                     const BigIntVal& endTime, StringVal* pageTimeInfo) {
    UNUSED(context);
    // 过滤开始时间大于结束时间以及时间超过24小时的脏数据
    if (startTime.val >= endTime.val || (endTime.val - startTime.val) >= 86400 * 1000L) {
        return;
    }
    std::string timeRange;
    if (pageTimeInfo->ptr == nullptr) {
        timeRange = std::to_string(startTime.val) + separator + std::to_string(endTime.val);
    } else {
        timeRange =
            separator + std::to_string(startTime.val) + separator + std::to_string(endTime.val);
    }
    pageTimeInfo->append(context, reinterpret_cast<const uint8_t*>(timeRange.c_str()),
                         timeRange.size());
}

void doris_udf::GetPerPageTimeMerge(FunctionContext* context, const StringVal& src,
                                    StringVal* pageTimeInfo) {
    UNUSED(context);
    if (src.ptr == nullptr) {
        return;
    }
    if (pageTimeInfo->ptr != nullptr) {
        pageTimeInfo->append(context, reinterpret_cast<const uint8_t*>(separator.c_str()),
                             separator.size());
    }
    pageTimeInfo->append(context, src.ptr, src.len);
}

StringVal doris_udf::GetPerPageTimeFinalize(FunctionContext* context,
                                            const StringVal& pageTimeInfo) {
    UNUSED(context);
    StringVal result;

    if (pageTimeInfo.ptr == nullptr) {
        std::string value("0 0");
        result.resize(context, value.size());
        memcpy(result.ptr, reinterpret_cast<const uint8_t*>(value.c_str()), value.size());
    } else {
        std::vector<std::string> splitResult;
        std::string input_string(reinterpret_cast<char*>(pageTimeInfo.ptr), pageTimeInfo.len);
        boost::split(splitResult, input_string, boost::is_any_of(separator));
        std::vector<std::pair<long, long>> resultRanges;
        for (auto iter = splitResult.begin(); iter != splitResult.end();) {
            long startTime = std::stol(*iter);
            iter++;
            long endTime = std::stol(*iter);
            iter++;
            resultRanges.emplace_back(std::make_pair(startTime, endTime));
        }
        std::sort(resultRanges.begin(), resultRanges.end());

        auto currentIter = resultRanges.begin();
        auto prevIter = currentIter;
        long totalTime = ((*prevIter).second - (*prevIter).first);
        long sessionTimes = 1;
        currentIter++;
        for (; currentIter != resultRanges.end(); ++currentIter) {
            if ((*prevIter).second <= (*currentIter).first) {
                totalTime += ((*currentIter).second - (*currentIter).first);
                // 超过30s则切分session
                if (((*currentIter).first - (*prevIter).second) > 30000) {
                    sessionTimes++;
                }
                prevIter = currentIter;
            } else if ((*prevIter).second >= (*currentIter).second) {
                // 包含情况处理
                totalTime -= ((*prevIter).second - (*prevIter).first);
                totalTime += ((*currentIter).second - (*currentIter).first);
                prevIter = currentIter;
            } else {
                // 重叠情况处理
                totalTime -= ((*prevIter).second - (*prevIter).first);
                prevIter = ++currentIter;
                if (prevIter == resultRanges.end()) {
                    break;
                }
                totalTime += ((*prevIter).second - (*prevIter).first);
            }
        }
        std::string timeValue = std::to_string(sessionTimes) + "_" + std::to_string(totalTime);
        result.resize(context, timeValue.size());
        memcpy(result.ptr, reinterpret_cast<const uint8_t*>(timeValue.c_str()), timeValue.size());
    }

    return result;
}

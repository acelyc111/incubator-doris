//
// Created by mi on 19-7-24.
//

#include "get_session_time.h"

unsigned long doris_udf::getSessionTimes(const std::string& inputString) {
    if (inputString.empty()) {
        return 0;
    }

    std::vector<std::string> splitResult;
    boost::split(splitResult, inputString, boost::is_any_of(separator));
    std::vector<std::pair<long, long>> resultRanges;
    for (auto iter = splitResult.begin(); iter != splitResult.end();) {
        long startTime = std::stol(*iter);
        iter++;
        long endTime = std::stol(*iter);
        iter++;
        resultRanges.emplace_back(std::make_pair(startTime, endTime));
    }
    std::sort(resultRanges.begin(), resultRanges.end());

    processInvalidRange(resultRanges);
    if (resultRanges.size() < 2) {
        return resultRanges.size();
    } else {
        // session切分
        unsigned long sessionTimes = 1;
        for (auto iter = resultRanges.begin() + 1; iter != resultRanges.end(); ++iter) {
            auto prevIter = iter - 1;
            // 超过30s切分一??session
            if (((*iter).first - (*prevIter).second) >= 30000) {
                sessionTimes++;
            }
        }
        return sessionTimes;
    }
}

void doris_udf::GetSessionTimeInit(FunctionContext* context, StringVal* sessionInfo) {
    UNUSED(context);
    sessionInfo->is_null = false;
    sessionInfo->ptr = nullptr;
    sessionInfo->len = 0;
}

void doris_udf::GetSessionTimeUpdate(FunctionContext* context, const BigIntVal& startTime,
                                     const BigIntVal& endTime, StringVal* sessionInfo) {
    UNUSED(context);
    // 过滤开始时间大于结束时间以及时间超过24小时的脏数据
    if (startTime.is_null || startTime.val == 0 || endTime.is_null || endTime.val == 0
        || startTime.val >= endTime.val || (endTime.val - startTime.val) >= 86400 * 1000L) {
        return;
    }
    std::string timeRange =
        separator + std::to_string(startTime.val) + separator + std::to_string(endTime.val);
    sessionInfo->append(context, reinterpret_cast<const uint8_t*>(timeRange.c_str()),
                        timeRange.size());
}

void doris_udf::GetSessionTimeMerge(FunctionContext* context, const StringVal& src,
                                    StringVal* sessionInfo) {
    UNUSED(context);
    if (src.ptr == nullptr) {
        return;
    }
    sessionInfo->append(context, src.ptr, src.len);
}

StringVal doris_udf::GetSessionTimeFinalize(FunctionContext* context,
                                            const StringVal& sessionInfo) {
    UNUSED(context);
    if (sessionInfo.ptr == nullptr || sessionInfo.len == 0) {
        return StringVal::null();
    }
    std::string inputSessionInfo(reinterpret_cast<char*>(sessionInfo.ptr), sessionInfo.len);
    if (inputSessionInfo.front() == separator.front()) {
        try {
            inputSessionInfo.erase(0, 1);
        } catch (std::out_of_range& e) {
#ifdef NDEBUG
            std::cout << "Out of Range: " << e.what() << std::endl;
#endif
        }
    }
    std::string sessionTimes = std::to_string(getSessionTimes(inputSessionInfo));

    StringVal result;
    result.append(context, reinterpret_cast<const uint8_t*>(sessionTimes.c_str()),
                  sessionTimes.size());
    return result;
}

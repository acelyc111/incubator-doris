//
// Created by wangcong on 2019/8/28.
//

#include "get_time_util.h"

void doris_udf::processInvalidRange(std::vector<std::pair<long, long>>& inputRanges) {
    if (inputRanges.size() < 2) {
        return;
    }

    auto currentIter = inputRanges.begin();
    auto prevIter = currentIter;
    currentIter++;
    for (; currentIter != inputRanges.end(); ++currentIter) {
        if ((*prevIter).second <= (*currentIter).first) {
            prevIter = currentIter;
        } else if ((*prevIter).second >= (*currentIter).second) {
            // 包含情况处理
            prevIter = inputRanges.erase(prevIter);
            currentIter = prevIter;
        } else {
            // 重叠情况处理
            currentIter = inputRanges.erase(prevIter);
            prevIter = inputRanges.erase(currentIter);
            if (prevIter == inputRanges.end()) {
                break;
            }
        }
    }
}

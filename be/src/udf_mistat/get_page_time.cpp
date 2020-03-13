//
// Created by wangcong on 2019/8/28.
//

#include "get_page_time.h"

void doris_udf::GetPageTimeInit(FunctionContext* context, StringVal* dst) {
    dst->is_null = false;
    dst->ptr = nullptr;
    dst->len = 0;
}

void doris_udf::GetPageTimeUpdate(FunctionContext* context, const StringVal& pg,
                                  const BigIntVal& bt, const BigIntVal& et, StringVal* dst) {
    // 过滤输入为空,开始时间大于结束时间以及时间超过24小时的脏数据
    if (pg.is_null || bt.is_null || et.is_null || bt.val >= et.val
        || (et.val - bt.val) >= 60 * 60 * 24 * 1000L) {
        return;
    }
    std::stringstream ss;
    ss << connector << std::string(reinterpret_cast<const char*>(pg.ptr), pg.len) << separator
       << bt.val << separator << et.val;
    dst->append(context, reinterpret_cast<const uint8_t*>(ss.str().c_str()), ss.str().size());
}

void doris_udf::GetPageTimeMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
    if (src.len == 0) {
        return;
    }
    dst->append(context, src.ptr, src.len);
}

StringVal doris_udf::GetPageTimeFinalize(FunctionContext* context, StringVal* dst) {
    std::string dst_str(reinterpret_cast<char*>(dst->ptr), dst->len);
    if (dst->is_null || dst_str.empty()) {
        return StringVal::null();
    }
    if (dst_str.front() == connector.front()) {
        try {
            dst_str.erase(0, 1);
        } catch (std::out_of_range& e) {
        }
    }

    std::vector<std::string> pages;
    boost::split(pages, dst_str, boost::is_any_of(connector));

    std::unordered_map<std::string, std::vector<std::pair<long, long>>> info_by_page;

    std::vector<std::string> tmpPage;
    const int pageInfoNums = 3;
    for (auto page : pages) {
        boost::split(tmpPage, page, boost::is_any_of(separator));
        if (tmpPage.size() != pageInfoNums) {
            continue;
        }
        try {
            info_by_page[tmpPage[0]].emplace_back(
                std::make_pair(std::stol(tmpPage[1]), std::stol(tmpPage[2])));
        } catch (std::invalid_argument& t) {
        }
    }

    std::stringstream ss;
    for (auto info : info_by_page) {
        std::sort(info.second.begin(), info.second.end());
        processInvalidRange(info.second);
        int pageTimes = 0;
        long pageSessions = 0;
        for (auto timeRange : info.second) {
            pageTimes++;
            pageSessions += (timeRange.second - timeRange.first);
        }
        if (pageTimes) {
            ss << info.first << separator << pageTimes << separator << pageSessions << connector;
        }
    }
    std::string processedResult = ss.str();
    if (processedResult.empty()) {
        return StringVal::null();
    }
    try {
        processedResult.erase(processedResult.size() - 1, 1);
    } catch (std::out_of_range& e) {
    }

    StringVal result;
    result.append(context, reinterpret_cast<const uint8_t*>(processedResult.c_str()),
                  processedResult.size());
    return result;
}

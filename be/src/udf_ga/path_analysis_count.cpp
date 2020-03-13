//
// Created by wangcong on 2019/9/18.
//

#include "path_analysis_count.h"

void doris_udf::PathAnalysisCountInit(FunctionContext* context, StringVal* pathInfoAgg) {
    pathInfoAgg->is_null = false;
    pathInfoAgg->ptr = nullptr;
    pathInfoAgg->len = 0;
}

void doris_udf::PathAnalysisCountUpdate(FunctionContext* context, const StringVal& src,
                                        StringVal* pathInfoAgg) {
    if (src.is_null || src.len == 0) {
        return;
    }

    std::string appendVal;
    appendVal += separator;
    appendVal += StringValToStdString(src);
    pathInfoAgg->append(context, reinterpret_cast<const uint8_t*>(appendVal.c_str()),
                        appendVal.size());
}

void doris_udf::PathAnalysisCountMerge(FunctionContext* context, const StringVal& src,
                                       StringVal* pathInfoAgg) {
    PathAnalysisCountUpdate(context, src, pathInfoAgg);
}

StringVal doris_udf::PathAnalysisCountFinalize(FunctionContext* context,
                                               const StringVal& pathInfoAgg) {
    if (pathInfoAgg.is_null) {
        return StringVal::null();
    }

    std::vector<std::string> paths;
    std::string input = StringValToStdString(pathInfoAgg);
    boost::split(paths, input, std::bind1st(std::equal_to<char>(), separator));

    std::vector<std::pair<std::string, long>> pathInfoPairs;
    std::pair<std::string, long> tmp;
    for (const auto& path : paths) {
        std::string::size_type splitIdx = path.find(timesConnector);
        if (splitIdx == std::__cxx11::basic_string<char>::npos) {
            continue;
        }
        try {
            tmp.first = path.substr(0, splitIdx);
            tmp.second = std::stol(path.substr(splitIdx + 1, path.size() - 1));
            if (tmp.second < 1) {
                continue;
            }
        } catch (const std::invalid_argument& e) {
            continue;
        }
        pathInfoPairs.emplace_back(tmp);
    }

    std::unordered_map<std::string, long> results;
    for (const auto& pathInfoPair : pathInfoPairs) {
        results[pathInfoPair.first] += pathInfoPair.second;
    }

    std::string output;
    for (const auto& result : results) {
        output += result.first;
        output += timesConnector;
        output += std::to_string(result.second);
        output += separator;
    }
    if (output.back() == separator) {
        output.pop_back();
    }

    StringVal pathInfo;
    pathInfo.append(context, reinterpret_cast<const uint8_t*>(output.c_str()), output.size());
    return pathInfo;
}

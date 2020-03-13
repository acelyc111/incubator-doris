//
// Created by mi on 19-7-18.
//

#include "udf_mistat/string_split.h"

StringVal doris_udf::StringSplit(FunctionContext* context, const StringVal& sep,
                                 const StringVal& inputs, const IntVal& idx) {
    if (sep.is_null || inputs.is_null || idx.is_null) {
        return StringVal::null();
    }
    std::vector<std::string> results;
    std::string input_string(reinterpret_cast<char*>(inputs.ptr), inputs.len);
    std::string separator(reinterpret_cast<char*>(sep.ptr), sep.len);
    boost::split(results, input_string, boost::is_any_of(separator));
    if (idx.val >= results.size() || results[idx.val].empty()) {
        return StringVal::null();
    }
    auto result = results[idx.val];
    StringVal returnVal(context, result.size());
    memcpy(returnVal.ptr, result.c_str(), result.size());
    return returnVal;
}

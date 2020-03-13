//
// Created by wangcong on 2019/9/23.
//

#include "get_active_common.h"

void doris_udf::GetActiveCommonUpdate(FunctionContext* context, const StringVal& separator,
                                      const BigIntVal& ts, const StringVal& origin,
                                      StringVal* activeInfo) {
    auto activeInfoAgg = reinterpret_cast<ActiveInfo*>(activeInfo->ptr);
    if (activeInfoAgg->value.empty() || activeInfoAgg->needUpdate(ts.val)) {
        activeInfoAgg->separator = StringValToStdString(separator);
        activeInfoAgg->update(ts, origin);
    }
}

StringVal doris_udf::GetActiveCommonSerialize(FunctionContext* context,
                                              const StringVal& activeInfo) {
    auto activeInfoAgg = reinterpret_cast<ActiveInfo*>(activeInfo.ptr);
    std::unique_ptr<ActiveInfo> activeInfoAggPtr(activeInfoAgg);

    std::ostringstream oss;
    boost::archive::text_oarchive oa(oss);
    oa << activeInfoAggPtr.get();

    StringVal result;
    result.append(context, reinterpret_cast<const uint8_t*>(oss.str().c_str()), oss.str().size());
    return result;
}

void doris_udf::GetActiveCommonMerge(FunctionContext* context, const StringVal& src,
                                     StringVal* activeInfo) {
    std::string srcActiveInfoStr(reinterpret_cast<char*>(src.ptr), src.len);
    std::istringstream iss(srcActiveInfoStr);
    boost::archive::text_iarchive ia(iss);
    ActiveInfo* srcActiveInfoAgg = nullptr;
    ia >> srcActiveInfoAgg;

    auto dstActiveInfoAgg = reinterpret_cast<ActiveInfo*>(activeInfo->ptr);
    if (srcActiveInfoAgg->value.empty()) {
        return;
    }
    if (dstActiveInfoAgg->value.empty()
        || dstActiveInfoAgg->needUpdate(
               GetTimeStamp(srcActiveInfoAgg->value, srcActiveInfoAgg->separator))) {
        dstActiveInfoAgg->update(srcActiveInfoAgg->value);
    }
}

StringVal doris_udf::GetActiveCommonFinalize(FunctionContext* context, StringVal* activeInfo) {
    auto activeInfoResult = reinterpret_cast<ActiveInfo*>(activeInfo->ptr)->value;
    StringVal result;
    result.append(context, reinterpret_cast<const uint8_t*>(activeInfoResult.c_str()),
                  activeInfoResult.size());
    return result;
}

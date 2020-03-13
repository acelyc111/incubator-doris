//
// Created by wangcong on 2019/9/18.
//

#include "path_analysis_process.h"

void doris_udf::PathAnalysisProcessInit(FunctionContext* context, StringVal* pathInfoAgg) {
    pathInfoAgg->is_null = false;
    pathInfoAgg->ptr = reinterpret_cast<uint8_t*>(new EventInfoAgg());
    pathInfoAgg->len = sizeof(EventInfoAgg);
}

void doris_udf::PathAnalysisProcessUpdate(FunctionContext* context, const StringVal& eventName,
                                          const BigIntVal& ts, const IntVal& sessionInterval,
                                          const StringVal& targetEventName,
                                          const TinyIntVal& isReverse, const IntVal& maxLevel,
                                          StringVal* pathInfoAgg) {
    if (eventName.is_null || eventName.ptr == nullptr || ts.is_null || sessionInterval.is_null
        || targetEventName.is_null || targetEventName.ptr == nullptr || isReverse.is_null
        || maxLevel.is_null) {
        return;
    }

    auto eventInfoAgg = reinterpret_cast<EventInfoAgg*>(pathInfoAgg->ptr);
    if (!eventInfoAgg->initialized()) {
        eventInfoAgg->initialize(sessionInterval.val, StringValToStdString(targetEventName),
                                 isReverse.val == 0, maxLevel.val);
    }
    eventInfoAgg->addEvent(ts.val, StringValToStdString(eventName));
}

StringVal doris_udf::PathAnalysisProcessSerialize(FunctionContext* context,
                                                  const StringVal& pathInfoAgg) {
    auto eventInfoAgg = reinterpret_cast<EventInfoAgg*>(pathInfoAgg.ptr);
    std::unique_ptr<EventInfoAgg> eventInfoAggPtr(eventInfoAgg);

    std::ostringstream oss;
    boost::archive::text_oarchive oa(oss);
    oa << eventInfoAggPtr.get();

    StringVal result;
    result.append(context, reinterpret_cast<const uint8_t*>(oss.str().c_str()), oss.str().length());
    return result;
}

// pathInfoAgg是在内存中的对象, src是网络传输的序列化对象
void doris_udf::PathAnalysisProcessMerge(FunctionContext* context, const StringVal& src,
                                         StringVal* pathInfoAgg) {
    if (src.is_null || src.len == 0) {
        return;
    }

    std::string srcInfoAggStr(reinterpret_cast<char*>(src.ptr), src.len);
    std::istringstream iss(srcInfoAggStr);
    boost::archive::text_iarchive ia(iss);
    EventInfoAgg* srcInfoAgg = nullptr;
    ia >> srcInfoAgg;

    auto dstEventInfoAgg = reinterpret_cast<EventInfoAgg*>(pathInfoAgg->ptr);
    dstEventInfoAgg->merge(*srcInfoAgg);
}

StringVal doris_udf::PathAnalysisProcessFinalize(FunctionContext* context, StringVal* pathInfoAgg) {
    auto resultPathInfoAgg = reinterpret_cast<EventInfoAgg*>(pathInfoAgg->ptr);
    std::string output = resultPathInfoAgg->output();
    delete resultPathInfoAgg;
    StringVal result;
    result.append(context, reinterpret_cast<const uint8_t*>(output.c_str()), output.size());
    return result;
}

//
// Created by wangcong on 2019/9/23.
//

#include "get_first_active_info.h"

void doris_udf::GetFirstActiveInfoInit(FunctionContext* context, StringVal* activeInfo) {
    activeInfo->is_null = false;
    activeInfo->ptr = reinterpret_cast<uint8_t*>(new ActiveInfo(ACTIVE_INFO_STRATEGY::FIRST));
    activeInfo->len = sizeof(ActiveInfo);
}

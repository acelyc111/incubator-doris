//
// Created by wangcong on 2019/9/23.
//

#include "get_last_active_info.h"

void doris_udf::GetLastActiveInfoInit(FunctionContext* context, StringVal* activeInfo) {
    activeInfo->is_null = false;
    activeInfo->ptr = reinterpret_cast<uint8_t*>(new ActiveInfo(ACTIVE_INFO_STRATEGY::LAST));
    activeInfo->len = sizeof(ActiveInfo);
}

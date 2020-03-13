//
// Created by wangcong on 2019/9/23.
//

#ifndef DORIS_GET_LAST_ACTIVE_INFO_H
#define DORIS_GET_LAST_ACTIVE_INFO_H

#include "udf_mistat/get_active_common.h"

namespace doris_udf {

void GetLastActiveInfoInit(FunctionContext* context, StringVal* activeInfo);
}

#endif  // DORIS_GET_LAST_ACTIVE_INFO_H

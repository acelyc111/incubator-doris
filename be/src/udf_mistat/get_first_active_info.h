//
// Created by wangcong on 2019/9/23.
//

#ifndef DORIS_GET_FIRST_ACTIVE_INFO_H
#define DORIS_GET_FIRST_ACTIVE_INFO_H

#include "udf_mistat/get_active_common.h"

namespace doris_udf {

void GetFirstActiveInfoInit(FunctionContext* context, StringVal* activeInfo);
}

#endif  // DORIS_GET_FIRST_ACTIVE_INFO_H

//
// Created by wangcong on 2019/9/3.
//

#ifndef DORIS_STRING_HASH_H
#define DORIS_STRING_HASH_H

#include <iostream>

#include "udf/udf.h"

namespace doris_udf {
LargeIntVal stringHash(FunctionContext* context, const StringVal& val);
};

#endif  // DORIS_STRING_HASH_H

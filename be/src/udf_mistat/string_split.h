//
// Created by mi on 19-7-18.
//

#ifndef DORIS_STRING_SPLIT_H
#define DORIS_STRING_SPLIT_H
#include "udf/udf.h"
#include <boost/algorithm/string.hpp>
#include <vector>

namespace doris_udf {
StringVal StringSplit(FunctionContext* context, const StringVal& sep, const StringVal& inputs,
                      const IntVal& idx);
}
#endif  // DORIS_STRING_SPLIT_H


#pragma once

#include "udf/udf.h"

namespace doris_udf {

BigIntVal hash64(FunctionContext* context, const StringVal& arg1);
}


#include "udf_ga/udf_hash64.h"
#include <iostream>
#include <string>

namespace doris_udf {

BigIntVal hash64(FunctionContext* context, const StringVal& arg1) {
    if (arg1.is_null) {
        return BigIntVal::null();
    }

    if (arg1.len != 32) {
        return BigIntVal::null();
    }

    long result = 0l;
    for (int i = 0; i < 16; i++) {
        result = (result << 4) + arg1.ptr[2 * i + 1];
    }
    // std::cout << "str:"  <<  std::string((char*)arg1.ptr, arg1.len) << ",
    // result:" << result << std::endl;
    return result;
}
}

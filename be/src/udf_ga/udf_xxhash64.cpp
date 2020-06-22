#include "udf_ga/udf_xxhash64.h"

namespace doris_udf {

static const uint64_t DEFAULT_SEED = 4870177450012600283ULL;

BigIntVal xxhash64(FunctionContext* context, const StringVal& arg) {
    if (arg.is_null) {
	return 0L;
    }
    return XXHash64::hash(arg.ptr, arg.len, DEFAULT_SEED);
}
}

#include <unistd.h>

#include "MurmurHash3.h"

#include "utils.h"

namespace mdb {

uint32_t stringhash32(const void* data, int len) {
    uint32_t hash_value;
    static int seed = getpid();
    MurmurHash3_x86_32(data, len, seed, &hash_value);
    return hash_value;
}

} // namespace mdb

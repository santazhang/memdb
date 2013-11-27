#include "memdb/utils.h"
#include "base/all.h"

using namespace base;
using namespace mdb;

TEST(utils, stringhash32) {
    EXPECT_EQ(stringhash32("hello"), stringhash32("hello"));
    EXPECT_NEQ(stringhash32("hello"), stringhash32("world"));
    Log::debug("stringhash32('hello') = %u", stringhash32("hello"));
}

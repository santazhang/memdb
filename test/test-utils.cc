#include <iostream>
#include <vector>
#include <list>
#include <set>
#include <map>

#include "memdb/utils.h"
#include "base/all.h"

using namespace base;
using namespace mdb;
using namespace std;

TEST(utils, stringhash32) {
    EXPECT_EQ(stringhash32("hello"), stringhash32("hello"));
    EXPECT_NEQ(stringhash32("hello"), stringhash32("world"));
    Log::debug("stringhash32('hello') = %u", stringhash32("hello"));
}

TEST(utils, stringhash64) {
    EXPECT_EQ(stringhash64("hello"), stringhash64("hello"));
    EXPECT_NEQ(stringhash64("hello"), stringhash64("world"));
    Log::debug("stringhash64('hello') = %llu", stringhash64("hello"));
}

TEST(utils, inthash32) {
    EXPECT_EQ(inthash32(1987, 1001), inthash32(1987, 1001));
    EXPECT_NEQ(inthash32(1987, 1989), inthash32(1987, 1001));
    Log::debug("inthash32(1987, 1001) = %u", inthash32(1987, 1001));
}

TEST(utils, inthash64) {
    EXPECT_EQ(inthash64(1987, 1001), inthash64(1987, 1001));
    EXPECT_NEQ(inthash64(1987, 1989), inthash64(1987, 1001));
    Log::debug("inthash64(1987, 1001) = %llu", inthash64(1987, 1001));
}

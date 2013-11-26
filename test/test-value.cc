#include <map>
#include <string>

#include "pkv/schema.h"
#include "base/all.h"

using namespace base;
using namespace pkv;
using namespace std;

TEST(value, types) {
    Value v;
    EXPECT_EQ(v.get_kind(), Value::UNKNOWN);
    v.set_i32(1987);
    EXPECT_EQ(v.get_kind(), Value::I32);
    EXPECT_EQ(v.get_i32(), 1987);

    EXPECT_LT(Value(43), Value(48));

    EXPECT_LT(Value(43000000LL), Value(48000000LL));
    EXPECT_EQ(Value(43000000LL).get_kind(), Value::I64);
    EXPECT_EQ(Value(43000000LL).get_i64(), 43000000LL);

    EXPECT_EQ(Value("hi").get_str(), "hi");
    EXPECT_EQ(Value(), Value());

}

TEST(value, insert_into_map) {
    map<string, Value> row;
    insert_into_map(row, string("id"), Value(2));
    row["name"] = Value("alice");
    // check for overwriting, use valgrind to detect memory leak
    row["name"] = Value("bob");
}

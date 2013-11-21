#include "pkv/schema.h"
#include "base/all.h"

using namespace base;
using namespace pkv;

TEST(schema, create) {
    Schema schema;
    schema.add_column("id", Value::I32);
    EXPECT_EQ(schema.get_column_id("id"), 0);
}

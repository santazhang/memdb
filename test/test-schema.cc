#include "memdb/schema.h"
#include "base/all.h"

using namespace base;
using namespace mdb;

TEST(schema, create) {
    verify(sizeof(f64) == 8);

    Schema* schema = new Schema;
    EXPECT_EQ(schema->get_column_info("no_such_column"), (void *) nullptr);

    schema->add_column("id", Value::I32);    // 0~3
    EXPECT_EQ(schema->get_column_id("id"), 0);
    EXPECT_EQ(schema->get_column_info(schema->get_column_id("id"))->type, Value::I32);
    EXPECT_EQ(schema->get_column_info("id")->type, Value::I32);
    EXPECT_EQ(schema->get_column_info("id")->fixed_size_offst, 0);

    schema->add_column("id_2", Value::I64);  // 4~11
    EXPECT_EQ(schema->get_column_info("id_2")->fixed_size_offst, 4);
    EXPECT_EQ(schema->get_column_info("id_2")->type, Value::I64);
    EXPECT_EQ(schema->get_column_info(schema->get_column_id("id_2"))->type, Value::I64);
    EXPECT_EQ(schema->get_column_id("id_2"), 1);

    schema->add_column("double_col", Value::F64);  // 12~11
    EXPECT_EQ(schema->get_column_info("double_col")->fixed_size_offst, 12);
    EXPECT_EQ(schema->get_column_info("double_col")->type, Value::F64);
    EXPECT_EQ(schema->get_column_info(schema->get_column_id("double_col"))->type, Value::F64);
    EXPECT_EQ(schema->get_column_id("double_col"), 2);

    schema->add_column("str_1", Value::STR);  // 12~11
    EXPECT_EQ(schema->get_column_info("str_1")->var_size_idx, 0);
    EXPECT_EQ(schema->get_column_info("str_1")->type, Value::STR);
    EXPECT_EQ(schema->get_column_info(schema->get_column_id("str_1"))->type, Value::STR);
    EXPECT_EQ(schema->get_column_id("str_1"), 3);

    schema->add_column("str_2", Value::STR);  // 12~11
    EXPECT_EQ(schema->get_column_info("str_2")->var_size_idx, 1);
    EXPECT_EQ(schema->get_column_info("str_2")->type, Value::STR);
    EXPECT_EQ(schema->get_column_info(schema->get_column_id("str_2"))->type, Value::STR);
    EXPECT_EQ(schema->get_column_id("str_2"), 4);

    schema->add_column("str_3", Value::STR);  // 12~11
    EXPECT_EQ(schema->get_column_info("str_3")->var_size_idx, 2);
    EXPECT_EQ(schema->get_column_info("str_3")->type, Value::STR);
    EXPECT_EQ(schema->get_column_info(schema->get_column_id("str_3"))->type, Value::STR);
    EXPECT_EQ(schema->get_column_id("str_3"), 5);

    schema->release();
}

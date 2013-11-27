#include "schema.h"

using namespace mdb;
using namespace std;

namespace mdb {

int Schema::add_column(const char* name, Value::kind type, bool indexed /* =? */) {
    int next_column_id = col_name_to_id_.size();
    if (col_name_to_id_.find(name) != col_name_to_id_.end()) {
        return -1;
    }

    column_info col_info;
    col_info.indexed = indexed;
    col_info.type = type;

    if (col_info.type == Value::STR) {
        // var size
        col_info.var_size_idx = var_size_cols_;
        var_size_cols_++;
    } else {
        // fixed size
        col_info.fixed_size_offst = fixed_part_size_;
        switch (type) {
        case Value::I32:
            fixed_part_size_ += sizeof(i32);
            break;
        case Value::I64:
            fixed_part_size_ += sizeof(i64);
            break;
        case Value::F64:
            fixed_part_size_ += sizeof(double);
            break;
        default:
            Log::fatal("value type %d not recognized", (int) type);
            verify(0);
            break;
        }
    }

    insert_into_map(col_name_to_id_, string(name), next_column_id);
    col_info_.push_back(col_info);
    return next_column_id;
}

} // namespace mdb

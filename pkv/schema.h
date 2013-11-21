#pragma once

#include <unordered_map>
#include <string>
#include <vector>

#include "value.h"
#include "utils.h"

namespace pkv {

class Row;

class Schema: public NoCopy {
    friend Row;

    struct column_info {
        int id;
        Value::kind type;

        // if fixed size (i32, i64, double)
        int fixed_size_offst;

        // if not fixed size (str)
        // need to lookup a index table on row
        int var_size_idx;
    };

    std::unordered_map<std::string, int> col_name_to_id_;
    std::vector<column_info> col_info_;

    // number of variable size cols (lookup table on row data)
    int var_size_cols_;
    int fixed_part_size_;

public:

    Schema(): var_size_cols_(0), fixed_part_size_(0) {}

    int add_column(const char* name, Value::kind type);
    int get_column_id(const std::string& name) const {
        auto it = col_name_to_id_.find(name);
        if (it != end(col_name_to_id_)) {
            assert(col_info_[it->second].id == it->second);
            return it->second;
        }
        return -1;
    }
};

} // namespace pkv

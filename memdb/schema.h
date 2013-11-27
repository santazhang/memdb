#pragma once

#include <unordered_map>
#include <string>
#include <vector>

#include "value.h"
#include "utils.h"

namespace mdb {

class Row;

class Schema: public NoCopy {
    friend class Row;

public:

    struct column_info {
        int id; // XXX is it really useful?
        Value::kind type;

        union {
            // if fixed size (i32, i64, double)
            int fixed_size_offst;

            // if not fixed size (str)
            // need to lookup a index table on row
            int var_size_idx;
        };
    };

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

    const column_info* get_column_info(const std::string& name) const {
        int col_id = get_column_id(name);
        if (col_id < 0) {
            return nullptr;
        } else {
            verify(col_id == col_info_[col_id].id);
            return &col_info_[col_id];
        }
    }
    const column_info* get_column_info(int column_id) const {
        verify(column_id >= 0 && column_id < col_info_.size());
        return &col_info_[column_id];
    }

private:

    std::unordered_map<std::string, int> col_name_to_id_;
    std::vector<column_info> col_info_;

    // number of variable size cols (lookup table on row data)
    int var_size_cols_;
    int fixed_part_size_;

};

} // namespace mdb

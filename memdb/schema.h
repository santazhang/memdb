#pragma once

#include <unordered_map>
#include <string>
#include <vector>

#include "value.h"
#include "utils.h"

namespace mdb {

class Row;

class Schema {
    friend class Row;

public:

    struct column_info {
        column_info(): id(-1), key(false), type(Value::UNKNOWN), fixed_size_offst(-1) {}

        int id;
        std::string name;
        bool key;
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

    int add_column(const char* name, Value::kind type, bool key = false);
    int add_key_column(const char* name, Value::kind type) {
        return add_column(name, type, true);
    }

    int get_column_id(const std::string& name) const {
        auto it = col_name_to_id_.find(name);
        if (it != std::end(col_name_to_id_)) {
            assert(col_info_[it->second].id == it->second);
            return it->second;
        }
        return -1;
    }
    const std::vector<int>& key_columns_id() const {
        return key_cols_id_;
    }

    const column_info* get_column_info(const std::string& name) const {
        int col_id = get_column_id(name);
        if (col_id < 0) {
            return nullptr;
        } else {
            return &col_info_[col_id];
        }
    }
    const column_info* get_column_info(int column_id) const {
        return &col_info_[column_id];
    }

    typedef std::vector<column_info>::iterator iterator;
    iterator begin() {
        return std::begin(col_info_);
    }
    iterator end() {
        return std::end(col_info_);
    }

private:

    std::unordered_map<std::string, int> col_name_to_id_;
    std::vector<column_info> col_info_;
    std::vector<int> key_cols_id_;

    // number of variable size cols (lookup table on row data)
    int var_size_cols_;
    int fixed_part_size_;
};

} // namespace mdb

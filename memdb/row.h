#pragma once

#include <map>
#include <unordered_map>
#include <vector>

#include "utils.h"

namespace mdb {

// forward declartion
class Schema;

class Row: public NoCopy {
    // version
    i64 ver_;

    // fixed size part
    char* fixed_part_;

    // var size part
    char* var_part_;

    // index table for var size part
    int* var_idx_;

    Schema* schema_;

    // private ctor, factory model
    Row(): ver_(0), fixed_part_(nullptr), var_part_(nullptr), var_idx_(nullptr), schema_(nullptr) {}
    ~Row();

public:
    i64 get_ver() const {
        return ver_;
    }
    Value get_column(int column_id) const;
    Value get_column(const std::string& col_name) const;

    static Row* create(Schema* schema, const std::map<std::string, Value>& values);
    static Row* create(Schema* schema, const std::unordered_map<std::string, Value>& values);
    static Row* create(Schema* schema, const std::vector<Value>& values);

    // helper function for all the create()
    static Row* create(Schema* schema, const std::vector<const Value*>& values);
};

} // namespace mdb

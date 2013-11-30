#pragma once

#include <map>
#include <unordered_map>
#include <vector>

#include "utils.h"
#include "schema.h"

namespace mdb {

// forward declartion
class Schema;

class Row: public RefCounted {
    // fixed size part
    char* fixed_part_;

    // var size part
    char* var_part_;

    // index table for var size part
    int* var_idx_;

    Schema* schema_;

    // private ctor, factory model
    Row(): fixed_part_(nullptr), var_part_(nullptr), var_idx_(nullptr), schema_(nullptr) {}

protected:
    // protected dtor as requried by RefCounted
    ~Row();

public:
    Value get_column(int column_id) const;
    Value get_column(const std::string& col_name) const {
        return get_column(schema_->get_column_id(col_name));
    }
    Value get_primary() const {
        return get_column(schema_->primary_column_id());
    }

    blob get_blob(int column_id) const;
    blob get_blob(const std::string& col_name) const {
        return get_blob(schema_->get_column_id(col_name));
    }
    blob get_primary_blob() const {
        return get_blob(schema_->primary_column_id());
    }

    static Row* create(Schema* schema, const std::map<std::string, Value>& values);
    static Row* create(Schema* schema, const std::unordered_map<std::string, Value>& values);
    static Row* create(Schema* schema, const std::vector<Value>& values);

    // helper function for all the create()
    static Row* create(Schema* schema, const std::vector<const Value*>& values);
};

} // namespace mdb

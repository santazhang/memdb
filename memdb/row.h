#pragma once

#include <map>
#include <unordered_map>
#include <vector>

#include "utils.h"
#include "schema.h"

namespace mdb {

// forward declartion
class Schema;
class Table;

class Row: public NoCopy {
    // fixed size part
    char* fixed_part_;

    enum {
        DENSE,
        SPARSE,
    };

    int kind_;

    union {
        // for DENSE rows
        struct {
            // var size part
            char* dense_var_part_;

            // index table for var size part
            int* dense_var_idx_;
        };

        // for SPARSE rows
        std::string* sparse_var_;
    };

    Schema* schema_;
    Table* tbl_;

    // private ctor, factory model
    Row(): fixed_part_(nullptr), kind_(DENSE),
           dense_var_part_(nullptr), dense_var_idx_(nullptr),
           schema_(nullptr), tbl_(nullptr) {}

public:
    ~Row();

    const Schema* schema() const {
        return schema_;
    }

    void make_sparse();
    void set_table(Table* tbl) {
        verify(tbl_ == nullptr);
        tbl_ = tbl;
    }

    Value get_column(int column_id) const;
    Value get_column(const std::string& col_name) const {
        return get_column(schema_->get_column_id(col_name));
    }
    MultiBlob get_key() const;

    blob get_blob(int column_id) const;
    blob get_blob(const std::string& col_name) const {
        return get_blob(schema_->get_column_id(col_name));
    }

    static Row* create(Schema* schema, const std::map<std::string, Value>& values);
    static Row* create(Schema* schema, const std::unordered_map<std::string, Value>& values);
    static Row* create(Schema* schema, const std::vector<Value>& values);

    // helper function for all the create()
    static Row* create(Schema* schema, const std::vector<const Value*>& values);
};

} // namespace mdb

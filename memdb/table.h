#pragma once

#include <string>
#include <list>
#include <unordered_map>

#include "value.h"
#include "row.h"
#include "schema.h"
#include "utils.h"
#include "blob.h"

namespace mdb {

class Table: public RefCounted {
    struct secondary_index {
        friend class Table;

    private:
        // hidden ctor and detor, only friend class Table can call them.
        secondary_index(Schema::column_info* column_info): column(column_info) {}
        ~secondary_index() {}

        // return nullptr if not found
        std::list<Row*> query(const Value& index_value) const;
        void remove(const Value& index_value);

        // safe to hold it, since Table is holding a reference to the schema, and
        // index is deleted before Table dies
        Schema::column_info* column;

        std::unordered_map<blob, blob, blob::hash, blob::equal> index;
    };

public:
    Table(Schema* schema);

    void insert(Row* row);

    // based on primary index
    Row* query(const Value& primary_value) const {
        auto it = rows_.find(primary_value.get_blob());
        if (it != end(rows_)) {
            return it->second;
        }
        return nullptr;
    }
    void remove(const Value& primary_value) {
        rows_.erase(primary_value.get_blob());
    }
    void update(const Value& primary_value, Row* new_row);

protected:
    // protected dtor as requried by RefCounted
    ~Table();

private:

    Schema* schema_;

    // indexed by primary values
    std::unordered_map<blob, Row*, blob::hash, blob::equal> rows_;

    std::map<std::string, secondary_index*> secondary_indices_;
};

} // namespace mdb

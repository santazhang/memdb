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
public:
    Table(Schema* schema);

    void insert(Row* row);

    // based on key index
    Row* query(const Value& key) const {
        auto it = rows_.find(key.get_blob());
        if (it != end(rows_)) {
            return it->second;
        }
        return nullptr;
    }
    void remove(const Value& key) {
        auto it = rows_.find(key.get_blob());
        if (it != end(rows_)) {
            it->second->release();
            rows_.erase(it);
        }
    }

protected:
    // protected dtor as requried by RefCounted
    ~Table();

private:
    Schema* schema_;

    // indexed by key values
    std::unordered_map<blob, Row*, blob::hash> rows_;
};

} // namespace mdb

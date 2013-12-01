#include "utils.h"
#include "table.h"

namespace mdb {

Table::Table(Schema* schema) {
    schema_ = (Schema *) schema->ref_copy();
    // for (auto& it: *schema_) {
    //     Log::debug("column: %d, %s%s", it.id, it.name.c_str(), it.key ? ", key" : "");
    // }
}

Table::~Table() {
    schema_->release();
    for (auto& it: rows_) {
        it.second->release();
    }
}

void Table::insert(Row* row) {
    blob key_blob = row->get_key_blob();
    auto it = rows_.find(key_blob);
    if (it != end(rows_)) {
        Log::warn("duplicated key index value in memtable, will overwrite");
        it->second->release();
        rows_.erase(it);
    }
    insert_into_map(rows_, key_blob, (Row *) row->ref_copy());
}

} // namespace mdb

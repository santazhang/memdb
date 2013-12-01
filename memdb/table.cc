#include "utils.h"
#include "table.h"

namespace mdb {

Table::Table(Schema* schema) {
    schema_ = (Schema *) schema->ref_copy();
    for (auto& it: *schema_) {
        Log::debug("column: %d, %s%s", it.id, it.name.c_str(), it.primary ? ", primary" : "");
    }
}

Table::~Table() {
    schema_->release();
    for (auto& it: rows_) {
        it.second->release();
    }
}

void Table::insert(Row* row) {
    blob primary_blob = row->get_primary_blob();
    auto it = rows_.find(primary_blob);
    if (it != end(rows_)) {
        Log::warn("duplicated primary index value in memtable, will overwrite");
        it->second->release();
        rows_.erase(it);
    }
    insert_into_map(rows_, primary_blob, (Row *) row->ref_copy());
}

} // namespace mdb

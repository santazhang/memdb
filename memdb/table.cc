#include "utils.h"
#include "table.h"

namespace mdb {

Table::Table(Schema* schema) {
    schema_ = (Schema *) schema->ref_copy();
    for (auto& it: *schema_) {
        Log::debug("column: %d, %s%s%s", it.id, it.name.c_str(),
            it.primary ? ", primary" : "", !it.primary && it.indexed ? ", indexed" : "");

        if (!it.primary && it.indexed) {
            // secondary indices only
            secondary_index* idx = new secondary_index(&it);
            insert_into_map(secondary_indices_, it.name, idx);
        }
    }
}

Table::~Table() {
    schema_->release();
    for (auto& it: secondary_indices_) {
        delete it.second;
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

    // insert into secondary index
    for (auto& it: secondary_indices_) {
        blob index_blob = row->get_blob(it.second->column->id);
        insert_into_map(it.second->index, index_blob, primary_blob);
    }
}

} // namespace mdb

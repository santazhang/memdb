#include "utils.h"
#include "table.h"

namespace mdb {

UnsortedTable::~UnsortedTable() {
    for (auto& it: rows_) {
        delete it.second;
    }
    delete schema_;
}

void UnsortedTable::clear() {
    for (auto& it: rows_) {
        delete it.second;
    }
    rows_.clear();
}

void UnsortedTable::remove(const MultiBlob& key) {
    auto query_range = rows_.equal_range(key);
    iterator it = query_range.first;
    while (it != query_range.second) {
        it = remove(it);
    }
}

void UnsortedTable::remove(Row* row, bool do_free /* =? */) {
    auto query_range = rows_.equal_range(row->get_key());
    iterator it = query_range.first;
    while (it != query_range.second) {
        if (it->second == row) {
            it->second->set_table(nullptr);
            remove(it, do_free);
            break;
        }
    }
}

UnsortedTable::iterator UnsortedTable::remove(iterator it, bool do_free /* =? */) {
    if (it != rows_.end()) {
        if (do_free) {
            delete it->second;
        }
        return rows_.erase(it);
    } else {
        return rows_.end();
    }
}

} // namespace mdb

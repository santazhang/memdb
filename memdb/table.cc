#include "utils.h"
#include "table.h"

namespace mdb {

UnsortedTable::~UnsortedTable() {
    for (auto& it: rows_) {
        delete it.second;
    }
    delete schema_;
}

UnsortedTable::iterator UnsortedTable::remove(iterator it) {
    if (it != rows_.end()) {
        delete it->second;
        return rows_.erase(it);
    } else {
        return rows_.end();
    }
}

} // namespace mdb

#include "utils.h"
#include "table.h"

namespace mdb {

Table::~Table() {
    for (auto& it: rows_) {
        it.second->release();
    }
    delete schema_;
}

Table::iterator Table::remove(iterator it) {
    if (it != rows_.end()) {
        it->second->release();
        return rows_.erase(it);
    } else {
        return rows_.end();
    }
}

} // namespace mdb

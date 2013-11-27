#include "table.h"

namespace mdb {

Table::Table(Schema* schema) {
    schema_ = (Schema *) schema->ref_copy();
    // TODO
}

Table::~Table() {
    schema_->release();
    for (auto& it: indices_) {
        delete it.second;
    }
}

void Table::insert_row(Row* row) {
    // TODO
}

} // namespace mdb

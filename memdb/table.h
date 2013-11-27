#pragma once

#include <string>

#include "value.h"
#include "row.h"
#include "schema.h"
#include "utils.h"

namespace mdb {

class Table: public RefCounted {
public:

    class Index: public NoCopy {
        // TODO
    };

    Table(Schema* schema);

    void insert_row(Row*);

protected:
    // protected dtor as requried by RefCounted
    ~Table();

private:
    Schema* schema_;
    std::map<std::string, Index*> indices_;

};

} // namespace mdb

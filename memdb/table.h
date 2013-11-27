#pragma once

#include "utils.h"

namespace mdb {

class Table: public RefCounted {
protected:
    // protected dtor as requried by RefCounted
    ~Table();
};

} // namespace mdb

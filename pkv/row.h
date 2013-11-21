#pragma once

namespace pkv {

// forward declartion
class Schema;

class Row: public NoCopy {
    // version
    i32 ver_;

    // fixed size part
    char* fixed_part_;

    // var size part
    char* var_part_;

    // index table for var size part
    int* var_idx_;

    Schema* schema_;
};

} // namespace pkv

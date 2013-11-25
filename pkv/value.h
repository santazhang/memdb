#pragma once

#include <ostream>
#include <string>

#include "utils.h"

namespace pkv {

class Value {
    friend std::ostream& operator<< (std::ostream& o, const Value& v);

public:

    typedef enum {
        UNKNOWN,
        I32,
        I64,
        F64,
        STR
    } kind;

    Value(): k_(UNKNOWN) {}
    explicit Value(i32 v): k_(I32), i32_(v) {}
    explicit Value(i64 v): k_(I64), i64_(v) {}
    explicit Value(f64 v): k_(F64), f64_(v) {}
    explicit Value(const std::string& v): k_(STR), p_str_(new std::string(v)) {}

    Value(const Value& o) {
        k_ = o.k_;
        i32_ = o.i32_;
        i64_ = o.i64_;
        f64_ = o.f64_;
        if (k_ == STR) {
            p_str_ = new std::string(*o.p_str_);
        }
    }

    ~Value() {
        if (k_ == STR) {
            delete p_str_;
        }
    }

    const Value& operator= (const Value& o) {
        if (this != &o) {
            if (k_ == STR) {
                delete p_str_;
            }
            if (o.k_ == STR) {
                p_str_ = new std::string(*o.p_str_);
            }
            k_ = o.k_;
            i32_ = o.i32_;
            i64_ = o.i64_;
            f64_ = o.f64_;
        }
        return *this;
    }

    // -1: this < o, 0: this == o, 1: this > o
    // UNKNOWN == UNKNOWN
    // both side should have same kind
    int compare(const Value& o) const;

    bool operator ==(const Value& o) const {
        return compare(o) == 0;
    }
    bool operator !=(const Value& o) const {
        return compare(o) != 0;
    }
    bool operator <(const Value& o) const {
        return compare(o) == -1;
    }
    bool operator >(const Value& o) const {
        return compare(o) == 1;
    }
    bool operator <=(const Value& o) const {
        return compare(o) != 1;
    }
    bool operator >=(const Value& o) const {
        return compare(o) != -1;
    }

    kind get_kind() const {
        return k_;
    }

    i32 get_i32() const {
        verify(k_ == I32);
        return i32_;
    }

    i64 get_i64() const {
        verify(k_ == I64);
        return i64_;
    }

    f64 get_f64() const {
        verify(k_ == F64);
        return f64_;
    }

    const std::string& get_str() const {
        verify(k_ == STR);
        return *p_str_;
    }

    void set_i32(i32 v) {
        if (k_ == UNKNOWN) {
            k_ = I32;
        }
        verify(k_ == I32);
        i32_ = v;
    }

    void set_i64(i64 v) {
        if (k_ == UNKNOWN) {
            k_ = I64;
        }
        verify(k_ == I64);
        i64_ = v;
    }

    void set_f64(f64 v) {
        if (k_ == UNKNOWN) {
            k_ = F64;
        }
        verify(k_ == F64);
        f64_ = v;
    }

    void set_str(const std::string& str) {
        if (k_ == UNKNOWN) {
            k_ = STR;
            p_str_ = new std::string(str);
        } else {
            verify(k_ == STR);
            *p_str_ = str;
        }
    }

private:
    kind k_;

    union {
        i32 i32_;
        i64 i64_;
        f64 f64_;
        std::string* p_str_;
    };
};

std::ostream& operator<< (std::ostream& o, const Value& v);

} // namespace pkv

#include "value.h"

namespace pkv {

int Value::compare(const Value& o) const {
    verify(k_ == o.k_);

    switch (k_) {
    case UNKNOWN:
        return 0;

    case I32:
        if (i32_ < o.i32_) {
            return -1;
        } else if (i32_ == o.i32_) {
            return 0;
        } else {
            return 1;
        }
        break;

    case I64:
        if (i64_ < o.i64_) {
            return -1;
        } else if (i64_ == o.i64_) {
            return 0;
        } else {
            return 1;
        }
        break;

    case F64:
        if (f64_ < o.f64_) {
            return -1;
        } else if (f64_ == o.f64_) {
            return 0;
        } else {
            return 1;
        }
        break;

    case STR:
        if (*p_str_ < *o.p_str_) {
            return -1;
        } else if (*p_str_ == *o.p_str_) {
            return 0;
        } else {
            return 1;
        }
        break;

    default:
        Log::fatal("unexpected value type %d", k_);
        verify(0);
        break;
    }

    return 0;
}


void Value::write_binary(char* buf) const {
    switch (k_) {
    case Value::I32:
        memcpy(buf, &i32_, sizeof(i32));
        break;
    case Value::I64:
        memcpy(buf, &i64_, sizeof(i64));
        break;
    case Value::F64:
        memcpy(buf, &f64_, sizeof(f64));
        break;
    case Value::STR:
        memcpy(buf, &((*p_str_)[0]), p_str_->size());
        break;
    default:
        Log::fatal("cannot write_binary() on value type %d", k_);
        verify(0);
        break;
    }
}

std::ostream& operator<< (std::ostream& o, const Value& v) {
    switch (v.k_) {
    case Value::UNKNOWN:
        o << "UNKNOWN";
        break;
    case Value::I32:
        o << "I32:" << v.i32_;
        break;
    case Value::I64:
        o << "I64:" << v.i64_;
        break;
    case Value::F64:
        o << "F64:" << v.f64_;
        break;
    case Value::STR:
        o << "STR:" << *v.p_str_;
        break;
    default:
        Log::fatal("unexpected value type %d", v.k_);
        verify(0);
        break;
    }
    return o;
}

} // namespace pkv

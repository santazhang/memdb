#pragma once

#include "base/all.h"
#include "memdb/row.h"

template <class T>
void report_qps(const char* action, T n_ops, double duration) {
    base::Log::info("%s: %d ops, took %.2lf sec, qps=%s",
        action, n_ops, duration, base::format_decimal(T(n_ops / duration)).c_str());
}

template <class EnumeratorOfRows>
bool rows_are_sorted(EnumeratorOfRows rows, mdb::symbol_t order = mdb::symbol_t::ORD_ASC) {
    verify(order == mdb::symbol_t::ORD_ASC || order == mdb::symbol_t::ORD_DESC);
    if (!rows.has_next()) {
        return true;
    }
    const mdb::Row* last = rows.next();
    while (rows.has_next()) {
        const mdb::Row* new_one = rows.next();
        if (order == mdb::symbol_t::ORD_DESC) {
            if (*last < *new_one) {
                return false;
            }
        } else {
            if (*last > *new_one) {
                return false;
            }
        }
    }
    return true;
}

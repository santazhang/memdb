#pragma once

#include "base/all.h"

template <class T>
void report_qps(const char* action, T n_ops, double duration) {
    base::Log::info("%s: %d ops, took %.2lf sec, qps=%s",
        action, n_ops, duration, base::format_decimal(T(n_ops / duration)).c_str());
}

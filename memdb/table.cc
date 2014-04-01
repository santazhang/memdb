#include "utils.h"
#include "table.h"

namespace mdb {

int SortedMultiKey::compare(const SortedMultiKey& o) const {
    verify(schema_ == o.schema_);
    const std::vector<int>& key_cols = schema_->key_columns_id();
    for (size_t i = 0; i < key_cols.size(); i++) {
        const Schema::column_info* info = schema_->get_column_info(key_cols[i]);
        verify(info->indexed);
        switch (info->type) {
        case Value::I32:
            {
                i32 mine = *(i32 *) mb_[i].data;
                i32 other = *(i32 *) o.mb_[i].data;
                assert(mb_[i].len == (int) sizeof(i32));
                assert(o.mb_[i].len == (int) sizeof(i32));
                if (mine < other) {
                    return -1;
                } else if (mine > other) {
                    return 1;
                }
            }
            break;
        case Value::I64:
            {
                i64 mine = *(i64 *) mb_[i].data;
                i64 other = *(i64 *) o.mb_[i].data;
                assert(mb_[i].len == (int) sizeof(i64));
                assert(o.mb_[i].len == (int) sizeof(i64));
                if (mine < other) {
                    return -1;
                } else if (mine > other) {
                    return 1;
                }
            }
            break;
        case Value::DOUBLE:
            {
                double mine = *(double *) mb_[i].data;
                double other = *(double *) o.mb_[i].data;
                assert(mb_[i].len == (int) sizeof(double));
                assert(o.mb_[i].len == (int) sizeof(double));
                if (mine < other) {
                    return -1;
                } else if (mine > other) {
                    return 1;
                }
            }
            break;
        case Value::STR:
            {
                int min_size = std::min(mb_[i].len, o.mb_[i].len);
                int cmp = memcmp(mb_[i].data, o.mb_[i].data, min_size);
                if (cmp < 0) {
                    return -1;
                } else if (cmp > 0) {
                    return 1;
                }
                // now check who's longer
                if (mb_[i].len < o.mb_[i].len) {
                    return -1;
                } else if (mb_[i].len > o.mb_[i].len) {
                    return 1;
                }
            }
            break;
        default:
            Log::fatal("unexpected column type %d", info->type);
            verify(0);
        }
    }
    return 0;
}


SortedTable::~SortedTable() {
    for (auto& it: rows_) {
        it.second->release();
    }
}

void SortedTable::clear() {
    for (auto& it: rows_) {
        it.second->release();
    }
    rows_.clear();
}

void SortedTable::remove(const SortedMultiKey& smk) {
    auto query_range = rows_.equal_range(smk);
    iterator it = query_range.first;
    while (it != query_range.second) {
        it = remove_iter(it);
    }
}

void SortedTable::remove(Row* row, bool do_free /* =? */) {
    SortedMultiKey smk = SortedMultiKey(row->get_key(), schema_);
    auto query_range = rows_.equal_range(smk);
    iterator it = query_range.first;
    while (it != query_range.second) {
        if (it->second == row) {
            it->second->set_table(nullptr);
            it = remove_iter(it, do_free);
            break;
        } else {
            ++it;
        }
    }
}

void SortedTable::remove(Cursor cur) {
    iterator it = cur.begin();
    while (it != cur.end()) {
        it = this->remove_iter(it);
    }
}

SortedTable::iterator SortedTable::remove_iter(iterator it, bool do_free /* =? */) {
    if (it != rows_.end()) {
        if (do_free) {
            it->second->release();
        }
        return rows_.erase(it);
    } else {
        return rows_.end();
    }
}

UnsortedTable::~UnsortedTable() {
    for (auto& it: rows_) {
        it.second->release();
    }
}

void UnsortedTable::clear() {
    for (auto& it: rows_) {
        it.second->release();
    }
    rows_.clear();
}

void UnsortedTable::remove(const MultiBlob& key) {
    auto query_range = rows_.equal_range(key);
    iterator it = query_range.first;
    while (it != query_range.second) {
        it = remove(it);
    }
}

void UnsortedTable::remove(Row* row, bool do_free /* =? */) {
    auto query_range = rows_.equal_range(row->get_key());
    iterator it = query_range.first;
    while (it != query_range.second) {
        if (it->second == row) {
            it->second->set_table(nullptr);
            it = remove(it, do_free);
            break;
        } else {
            ++it;
        }
    }
}

UnsortedTable::iterator UnsortedTable::remove(iterator it, bool do_free /* =? */) {
    if (it != rows_.end()) {
        if (do_free) {
            it->second->release();
        }
        return rows_.erase(it);
    } else {
        return rows_.end();
    }
}


IndexedTable::~IndexedTable() {
    // TODO
}


} // namespace mdb

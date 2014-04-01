#pragma once

#include <map>
#include <unordered_map>
#include <vector>
#include <string>

#include "utils.h"
#include "schema.h"
#include "locking.h"

namespace mdb {

// forward declartion
class Schema;
class Table;

class Row: public RefCounted {
    // fixed size part
    char* fixed_part_;

    enum {
        DENSE,
        SPARSE
    };

    int kind_;

    union {
        // for DENSE rows
        struct {
            // var size part
            char* dense_var_part_;

            // index table for var size part (marks the stop of a var segment)
            int* dense_var_idx_;
        };

        // for SPARSE rows
        std::string* sparse_var_;
    };

    Table* tbl_;

protected:

    void update_fixed(const Schema::column_info* col, void* ptr, int len);

    bool rdonly_;
    const Schema* schema_;

    // hidden ctor, factory model
    Row(): fixed_part_(nullptr), kind_(DENSE),
           dense_var_part_(nullptr), dense_var_idx_(nullptr),
           tbl_(nullptr), rdonly_(false), schema_(nullptr) {}

    // RefCounted should have protected dtor
    virtual ~Row();

    void copy_into(Row* row) const;

    // generic row creation
    static Row* create(Row* raw_row, const Schema* schema, const std::vector<const Value*>& values);

    // helper function for row creation
    static void fill_values_ptr(const Schema* schema, std::vector<const Value*>& values_ptr,
                                const Value& value, size_t fill_counter) {
        values_ptr[fill_counter] = &value;
    }

    // helper function for row creation
    static void fill_values_ptr(const Schema* schema, std::vector<const Value*>& values_ptr,
                                const std::pair<const std::string, Value>& pair, size_t fill_counter) {
        int col_id = schema->get_column_id(pair.first);
        verify(col_id >= 0);
        values_ptr[col_id] = &pair.second;
    }

public:

    virtual symbol_t rtti() const {
        return symbol_t::ROW_BASIC;
    }

    const Schema* schema() const {
        return schema_;
    }
    bool readonly() const {
        return rdonly_;
    }
    void make_readonly() {
        rdonly_ = true;
    }
    void make_sparse();
    void set_table(Table* tbl) {
        if (tbl != nullptr) {
            verify(tbl_ == nullptr);
        }
        tbl_ = tbl;
    }
    const Table* get_table() const {
        return tbl_;
    }

    Value get_column(int column_id) const;
    Value get_column(const std::string& col_name) const {
        return get_column(schema_->get_column_id(col_name));
    }
    virtual MultiBlob get_key() const;

    blob get_blob(int column_id) const;
    blob get_blob(const std::string& col_name) const {
        return get_blob(schema_->get_column_id(col_name));
    }

    void update(int column_id, i32 v) {
        const Schema::column_info* info = schema_->get_column_info(column_id);
        verify(info->type == Value::I32);
        update_fixed(info, &v, sizeof(v));
    }
    void update(int column_id, i64 v) {
        const Schema::column_info* info = schema_->get_column_info(column_id);
        verify(info->type == Value::I64);
        update_fixed(info, &v, sizeof(v));
    }
    void update(int column_id, double v) {
        const Schema::column_info* info = schema_->get_column_info(column_id);
        verify(info->type == Value::DOUBLE);
        update_fixed(info, &v, sizeof(v));
    }
    void update(int column_id, const std::string& str);
    void update(int column_id, const Value& v);

    void update(const std::string& col_name, i32 v) {
        this->update(schema_->get_column_id(col_name), v);
    }
    void update(const std::string& col_name, i64 v) {
        this->update(schema_->get_column_id(col_name), v);
    }
    void update(const std::string& col_name, double v) {
        this->update(schema_->get_column_id(col_name), v);
    }
    void update(const std::string& col_name, const std::string& v) {
        this->update(schema_->get_column_id(col_name), v);
    }
    void update(const std::string& col_name, const Value& v) {
        this->update(schema_->get_column_id(col_name), v);
    }

    // compare based on keys
    // must have same schema!
    int compare(const Row& another) const;

    bool operator ==(const Row& o) const {
        return compare(o) == 0;
    }
    bool operator !=(const Row& o) const {
        return compare(o) != 0;
    }
    bool operator <(const Row& o) const {
        return compare(o) == -1;
    }
    bool operator >(const Row& o) const {
        return compare(o) == 1;
    }
    bool operator <=(const Row& o) const {
        return compare(o) != 1;
    }
    bool operator >=(const Row& o) const {
        return compare(o) != -1;
    }

    virtual Row* copy() const {
        Row* row = new Row();
        copy_into(row);
        return row;
    }

    template <class Container>
    static Row* create(const Schema* schema, const Container& values) {
        verify(values.size() == schema->columns_count());
        std::vector<const Value*> values_ptr(values.size(), nullptr);
        size_t fill_counter = 0;
        for (auto it = values.begin(); it != values.end(); ++it) {
            fill_values_ptr(schema, values_ptr, *it, fill_counter);
            fill_counter++;
        }
        return Row::create(new Row(), schema, values_ptr);
    }
};


class CoarseLockedRow: public Row {
    RWLock lock_;

protected:

    // protected dtor as required by RefCounted
    ~CoarseLockedRow() {}

    void copy_into(CoarseLockedRow* row) const {
        this->Row::copy_into((Row *) row);
        row->lock_ = lock_;
    }

public:

    virtual symbol_t rtti() const {
        return symbol_t::ROW_COARSE;
    }

    bool rlock_row_by(lock_owner_t o) {
        return lock_.rlock_by(o);
    }
    bool wlock_row_by(lock_owner_t o) {
        return lock_.wlock_by(o);
    }
    bool unlock_row_by(lock_owner_t o) {
        return lock_.unlock_by(o);
    }

    virtual Row* copy() const {
        CoarseLockedRow* row = new CoarseLockedRow();
        copy_into(row);
        return row;
    }

    template <class Container>
    static CoarseLockedRow* create(const Schema* schema, const Container& values) {
        verify(values.size() == schema->columns_count());
        std::vector<const Value*> values_ptr(values.size(), nullptr);
        size_t fill_counter = 0;
        for (auto it = values.begin(); it != values.end(); ++it) {
            fill_values_ptr(schema, values_ptr, *it, fill_counter);
            fill_counter++;
        }
        return (CoarseLockedRow * ) Row::create(new CoarseLockedRow(), schema, values_ptr);
    }
};


class FineLockedRow: public Row {
    RWLock* lock_;
    void init_lock(int n_columns) {
        lock_ = new RWLock[n_columns];
    }

protected:

    // protected dtor as required by RefCounted
    ~FineLockedRow() {
        delete[] lock_;
    }

    void copy_into(FineLockedRow* row) const {
        this->Row::copy_into((Row *) row);
        int n_columns = schema_->columns_count();
        row->init_lock(n_columns);
        for (int i = 0; i < n_columns; i++) {
            row->lock_[i] = lock_[i];
        }
    }

public:

    virtual symbol_t rtti() const {
        return symbol_t::ROW_FINE;
    }

    bool rlock_column_by(column_id_t column_id, lock_owner_t o) {
        return lock_[column_id].rlock_by(o);
    }
    bool rlock_column_by(const std::string& col_name, lock_owner_t o) {
        column_id_t column_id = schema_->get_column_id(col_name);
        return lock_[column_id].rlock_by(o);
    }
    bool wlock_column_by(column_id_t column_id, lock_owner_t o) {
        return lock_[column_id].wlock_by(o);
    }
    bool wlock_column_by(const std::string& col_name, lock_owner_t o) {
        int column_id = schema_->get_column_id(col_name);
        return lock_[column_id].wlock_by(o);
    }
    bool unlock_column_by(column_id_t column_id, lock_owner_t o) {
        return lock_[column_id].unlock_by(o);
    }
    bool unlock_column_by(const std::string& col_name, lock_owner_t o) {
        column_id_t column_id = schema_->get_column_id(col_name);
        return lock_[column_id].unlock_by(o);
    }

    virtual Row* copy() const {
        FineLockedRow* row = new FineLockedRow();
        copy_into(row);
        return row;
    }

    template <class Container>
    static FineLockedRow* create(const Schema* schema, const Container& values) {
        verify(values.size() == schema->columns_count());
        std::vector<const Value*> values_ptr(values.size(), nullptr);
        size_t fill_counter = 0;
        for (auto it = values.begin(); it != values.end(); ++it) {
            fill_values_ptr(schema, values_ptr, *it, fill_counter);
            fill_counter++;
        }
        FineLockedRow* raw_row = new FineLockedRow();
        raw_row->init_lock(schema->columns_count());
        return (FineLockedRow * ) Row::create(raw_row, schema, values_ptr);
    }
};


// inherit from CoarseLockedRow since we need locking on commit phase, when doing 2 phase commit
class VersionedRow: public CoarseLockedRow {
    version_t* ver_;
    void init_ver(int n_columns) {
        ver_ = new version_t[n_columns];
        memset(ver_, 0, sizeof(version_t) * n_columns);
    }

protected:

    // protected dtor as required by RefCounted
    ~VersionedRow() {
        delete[] ver_;
    }

    void copy_into(VersionedRow* row) const {
        this->CoarseLockedRow::copy_into((CoarseLockedRow *)row);
        int n_columns = schema_->columns_count();
        row->init_ver(n_columns);
        memcpy(row->ver_, this->ver_, n_columns * sizeof(version_t));
    }

public:

    virtual symbol_t rtti() const {
        return symbol_t::ROW_VERSIONED;
    }

    version_t get_column_ver(column_id_t column_id) const {
        return ver_[column_id];
    }

    void incr_column_ver(column_id_t column_id) const {
        ver_[column_id]++;
    }

    virtual Row* copy() const {
        VersionedRow* row = new VersionedRow();
        copy_into(row);
        return row;
    }

    template <class Container>
    static VersionedRow* create(const Schema* schema, const Container& values) {
        verify(values.size() == schema->columns_count());
        std::vector<const Value*> values_ptr(values.size(), nullptr);
        size_t fill_counter = 0;
        for (auto it = values.begin(); it != values.end(); ++it) {
            fill_values_ptr(schema, values_ptr, *it, fill_counter);
            fill_counter++;
        }
        VersionedRow* raw_row = new VersionedRow();
        raw_row->init_ver(schema->columns_count());
        return (VersionedRow * ) Row::create(raw_row, schema, values_ptr);
    }
};

} // namespace mdb

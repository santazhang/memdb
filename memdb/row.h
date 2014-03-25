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

class Row: public NoCopy {
    // fixed size part
    char* fixed_part_;

    enum {
        DENSE,
        SPARSE,
    };

    int kind_;

    union {
        // for DENSE rows
        struct {
            // var size part
            char* dense_var_part_;

            // index table for var size part
            int* dense_var_idx_;
        };

        // for SPARSE rows
        std::string* sparse_var_;
    };

    Table* tbl_;

    void update_fixed(const Schema::column_info* col, void* ptr, int len);

protected:

    bool rdonly_;
    const Schema* schema_;

    // hidden ctor, factory model
    Row(): fixed_part_(nullptr), kind_(DENSE),
           dense_var_part_(nullptr), dense_var_idx_(nullptr),
           tbl_(nullptr), rdonly_(false), schema_(nullptr) {}

    // helper function for all the create()
    static Row* create(Row* raw_row, Schema* schema, const std::vector<const Value*>& values);

public:
    virtual ~Row();

    virtual symbol_t rtti() const {
        return ROW_BASIC;
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
    void update(int column_id, const std::string& v);
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

    static Row* create(Schema* schema, const std::map<std::string, Value>& values);
    static Row* create(Schema* schema, const std::unordered_map<std::string, Value>& values);

    template <class Container>
    static Row* create(Schema* schema, const Container& values) {
        verify(values.size() == schema->columns_count());
        std::vector<const Value*> values_ptr;
        values_ptr.reserve(values.size());
        for (auto& it: values) {
            values_ptr.push_back(&it);
        }
        return Row::create(new Row(), schema, values_ptr);
    }
};


class CoarseLockedRow: public Row {
    RWLock lock;
public:

    virtual symbol_t rtti() const {
        return ROW_COARSE;
    }

    bool rlock_row_by(lock_owner_t o) {
        verify(!rdonly_);
        return lock.rlock_by(o);
    }
    bool wlock_row_by(lock_owner_t o) {
        verify(!rdonly_);
        return lock.wlock_by(o);
    }
    bool unlock_row_by(lock_owner_t o) {
        verify(!rdonly_);
        return lock.unlock_by(o);
    }

    static CoarseLockedRow* create(Schema* schema, const std::map<std::string, Value>& values);
    static CoarseLockedRow* create(Schema* schema, const std::unordered_map<std::string, Value>& values);

    template <class Container>
    static CoarseLockedRow* create(Schema* schema, const Container& values) {
        verify(values.size() == schema->columns_count());
        std::vector<const Value*> values_ptr;
        values_ptr.reserve(values.size());
        for (auto& it: values) {
            values_ptr.push_back(&it);
        }
        return (CoarseLockedRow * ) Row::create(new CoarseLockedRow(), schema, values_ptr);
    }
};


class FineLockedRow: public Row {
    RWLock* lock;
    void init_lock(int n_locks) {
        lock = new RWLock[n_locks];
    }
public:
    ~FineLockedRow() {
        delete[] lock;
    }

    virtual symbol_t rtti() const {
        return ROW_FINE;
    }

    bool rlock_column_by(int column_id, lock_owner_t o) {
        verify(!rdonly_);
        return lock[column_id].rlock_by(o);
    }
    bool rlock_column_by(const std::string& col_name, lock_owner_t o) {
        verify(!rdonly_);
        int column_id = schema_->get_column_id(col_name);
        return lock[column_id].rlock_by(o);
    }
    bool wlock_column_by(int column_id, lock_owner_t o) {
        verify(!rdonly_);
        return lock[column_id].wlock_by(o);
    }
    bool wlock_column_by(const std::string& col_name, lock_owner_t o) {
        verify(!rdonly_);
        int column_id = schema_->get_column_id(col_name);
        return lock[column_id].wlock_by(o);
    }
    bool unlock_column_by(int column_id, lock_owner_t o) {
        verify(!rdonly_);
        return lock[column_id].unlock_by(o);
    }
    bool unlock_column_by(const std::string& col_name, lock_owner_t o) {
        verify(!rdonly_);
        int column_id = schema_->get_column_id(col_name);
        return lock[column_id].unlock_by(o);
    }

    static FineLockedRow* create(Schema* schema, const std::map<std::string, Value>& values);
    static FineLockedRow* create(Schema* schema, const std::unordered_map<std::string, Value>& values);

    template <class Container>
    static FineLockedRow* create(Schema* schema, const Container& values) {
        verify(values.size() == schema->columns_count());
        std::vector<const Value*> values_ptr;
        values_ptr.reserve(values.size());
        for (auto& it: values) {
            values_ptr.push_back(&it);
        }
        FineLockedRow* raw_row = new FineLockedRow();
        raw_row->init_lock(schema->columns_count());
        return (FineLockedRow * ) Row::create(raw_row, schema, values_ptr);
    }
};

} // namespace mdb

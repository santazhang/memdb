#include "value.h"
#include "row.h"
#include "schema.h"
#include "table.h"

namespace mdb {

Row::~Row() {
    delete[] fixed_part_;
    if (schema_->var_size_cols_ > 0) {
        if (kind_ == DENSE) {
            delete[] dense_var_part_;
            delete[] dense_var_idx_;
        } else {
            verify(kind_ == SPARSE);
            delete[] sparse_var_;
        }
    }
}

void Row::make_sparse() {
    if (kind_ == SPARSE) {
        // already sparse data
        return;
    }

    kind_ = SPARSE;

    if (schema_->var_size_cols_ == 0) {
        // special case, no memory copying required
        return;
    }

    // save those 2 values to prevent overwriting (union type!)
    char* var_data = dense_var_part_;
    int* var_idx = dense_var_idx_;

    assert(schema_->var_size_cols_ > 0);
    sparse_var_ = new std::string[schema_->var_size_cols_];
    sparse_var_[0] = std::string(var_data, var_idx[0]);
    for (int i = 1; i < schema_->var_size_cols_; i++) {
        int var_start = var_idx[i - 1];
        int var_len = var_idx[i] - var_idx[i - 1];
        sparse_var_[i] = std::string(&var_data[var_start], var_len);
    }

    delete[] var_data;
    delete[] var_idx;
}

Value Row::get_column(int column_id) const {
    Value v;
    const Schema::column_info* info = schema_->get_column_info(column_id);
    verify(info != nullptr);
    switch (info->type) {
    case Value::I32:
        v = Value(*((i32*) &fixed_part_[info->fixed_size_offst]));
        break;
    case Value::I64:
        v = Value(*((i64*) &fixed_part_[info->fixed_size_offst]));
        break;
    case Value::DOUBLE:
        v = Value(*((double*) &fixed_part_[info->fixed_size_offst]));
        break;
    case Value::STR:
        if (kind_ == DENSE) {
            int var_start = 0;
            int var_len = 0;
            if (info->var_size_idx == 0) {
                var_start = 0;
                var_len = dense_var_idx_[0];
            } else {
                var_start = dense_var_idx_[info->var_size_idx - 1];
                var_len = dense_var_idx_[info->var_size_idx] - dense_var_idx_[info->var_size_idx - 1];
            }
            v = Value(std::string(&dense_var_part_[var_start], var_len));
        } else {
            verify(kind_ == SPARSE);
            v = Value(sparse_var_[info->var_size_idx]);
        }
        break;
    default:
        Log::fatal("unexpected value type %d", info->type);
        verify(0);
        break;
    }
    return v;
}

MultiBlob Row::get_key() const {
    const std::vector<int>& key_cols = schema_->key_columns_id();
    MultiBlob mb(key_cols.size());
    for (int i = 0; i < mb.count(); i++) {
        mb[i] = this->get_blob(key_cols[i]);
    }
    return mb;
}

blob Row::get_blob(int column_id) const {
    blob b;
    const Schema::column_info* info = schema_->get_column_info(column_id);
    verify(info != nullptr);
    switch (info->type) {
    case Value::I32:
        b.data = &fixed_part_[info->fixed_size_offst];
        b.len = sizeof(i32);
        break;
    case Value::I64:
        b.data = &fixed_part_[info->fixed_size_offst];
        b.len = sizeof(i64);
        break;
    case Value::DOUBLE:
        b.data = &fixed_part_[info->fixed_size_offst];
        b.len = sizeof(double);
        break;
    case Value::STR:
        if (kind_ == DENSE) {
            int var_start = 0;
            int var_len = 0;
            if (info->var_size_idx == 0) {
                var_start = 0;
                var_len = dense_var_idx_[0];
            } else {
                var_start = dense_var_idx_[info->var_size_idx - 1];
                var_len = dense_var_idx_[info->var_size_idx] - dense_var_idx_[info->var_size_idx - 1];
            }
            b.data = &dense_var_part_[var_start];
            b.len = var_len;
        } else {
            verify(kind_ == SPARSE);
            b.data = &(sparse_var_[info->var_size_idx][0]);
            b.len = sparse_var_[info->var_size_idx].size();
        }
        break;
    default:
        Log::fatal("unexpected value type %d", info->type);
        verify(0);
        break;
    }
    return b;
}

void Row::update_fixed(const Schema::column_info* col, void* ptr, int len) {
    // check if really updating (new data!), and if necessary to remove/insert into table
    bool re_insert = false;
    if (memcmp(&fixed_part_[col->fixed_size_offst], ptr, len) == 0) {
        // not really updating
        return;
    }

    if (col->key) {
        re_insert = true;
    }

    if (re_insert && tbl_ != nullptr) {
        tbl_->remove(this, false);
    }

    memcpy(&fixed_part_[col->fixed_size_offst], ptr, len);

    if (re_insert && tbl_ != nullptr) {
        tbl_->insert(this);
    }
}

void Row::update(int column_id, const std::string& v) {
    const Schema::column_info* col = schema_->get_column_info(column_id);
    verify(col->type == Value::STR);

    // check if really updating (new data!), and if necessary to remove/insert into table
    bool re_insert = false;

    if (kind_ == SPARSE) {
        if (this->sparse_var_[col->var_size_idx] == v) {
            return;
        }
    } else {
        verify(kind_ == DENSE);
        int var_start = 0;
        int var_len = 0;
        if (col->var_size_idx == 0) {
            var_start = 0;
            var_len = dense_var_idx_[0];
        } else {
            var_start = dense_var_idx_[col->var_size_idx - 1];
            var_len = dense_var_idx_[col->var_size_idx] - dense_var_idx_[col->var_size_idx - 1];
        }
        if (memcmp(&dense_var_part_[var_start], &v[0], v.size()) == 0) {
            return;
        }
    }

    if (col->key) {
        re_insert = true;
    }

    if (re_insert && tbl_ != nullptr) {
        tbl_->remove(this, false);
    }

    this->make_sparse();
    this->sparse_var_[col->var_size_idx] = v;

    if (re_insert && tbl_ != nullptr) {
        tbl_->insert(this);
    }
}

void Row::update(int column_id, const Value& v) {
    switch (v.get_kind()) {
    case Value::I32:
        this->update(column_id, v.get_i32());
        break;
    case Value::I64:
        this->update(column_id, v.get_i64());
        break;
    case Value::DOUBLE:
        this->update(column_id, v.get_double());
        break;
    case Value::STR:
        this->update(column_id, v.get_str());
        break;
    default:
        Log::fatal("unexpected value type %d", v.get_kind());
        verify(0);
        break;
    }
}

Row* Row::create(Schema* schema, const std::map<std::string, Value>& values) {
    verify(values.size() == schema->columns_count());
    std::vector<const Value*> values_ptr(values.size(), nullptr);
    for (auto& it: values) {
        int col_id = schema->get_column_id(it.first);
        verify(col_id >= 0);
        values_ptr[col_id] = &it.second;
    }
    return Row::create(schema, values_ptr);
}

Row* Row::create(Schema* schema, const std::unordered_map<std::string, Value>& values) {
    verify(values.size() == schema->columns_count());
    std::vector<const Value*> values_ptr(values.size(), nullptr);
    for (auto& it: values) {
        int col_id = schema->get_column_id(it.first);
        verify(col_id >= 0);
        values_ptr[col_id] = &it.second;
    }
    return Row::create(schema, values_ptr);
}

Row* Row::create(Schema* schema, const std::vector<Value>& values) {
    verify(values.size() == schema->columns_count());
    std::vector<const Value*> values_ptr;
    values_ptr.reserve(values.size());
    for (auto& it: values) {
        values_ptr.push_back(&it);
    }
    return Row::create(schema, values_ptr);
}

Row* Row::create(Schema* schema, const std::vector<const Value*>& values) {
    Row* row = new Row;
    row->schema_ = schema;
    row->fixed_part_ = new char[schema->fixed_part_size_];
    if (schema->var_size_cols_ > 0) {
        row->dense_var_idx_ = new int[schema->var_size_cols_];
    }

    // 1st pass, write fixed part, and calculate var part size
    int var_part_size = 0;
    int fixed_pos = 0;
    for (auto& it: values) {
        switch (it->get_kind()) {
        case Value::I32:
            it->write_binary(&row->fixed_part_[fixed_pos]);
            fixed_pos += sizeof(i32);
            break;
        case Value::I64:
            it->write_binary(&row->fixed_part_[fixed_pos]);
            fixed_pos += sizeof(i64);
            break;
        case Value::DOUBLE:
            it->write_binary(&row->fixed_part_[fixed_pos]);
            fixed_pos += sizeof(double);
            break;
        case Value::STR:
            var_part_size += it->get_str().size();
            break;
        default:
            Log::fatal("unexpected value type %d", it->get_kind());
            verify(0);
            break;
        }
    }
    verify(fixed_pos == schema->fixed_part_size_);

    if (schema->var_size_cols_ > 0) {
        // 2nd pass, write var part
        int var_counter = 0;
        int var_pos = 0;
        row->dense_var_part_ = new char[var_part_size];
        for (auto& it: values) {
            if (it->get_kind() == Value::STR) {
                row->dense_var_idx_[var_counter] = it->get_str().size();
                it->write_binary(&row->dense_var_part_[var_pos]);
                var_counter++;
                var_pos += it->get_str().size();
            }
        }
        verify(var_part_size == var_pos);
        verify(var_counter == schema->var_size_cols_);
    }
    return row;
}


} // namespace mdb

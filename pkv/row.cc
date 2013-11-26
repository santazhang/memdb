#include "value.h"
#include "row.h"
#include "schema.h"

namespace pkv {

Row::~Row() {
    delete[] fixed_part_;
    if (schema_->var_size_cols_ > 0) {
        delete[] var_part_;
        delete[] var_idx_;
    }
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
    case Value::F64:
        v = Value(*((f64*) &fixed_part_[info->fixed_size_offst]));
        break;
    case Value::STR:
        {
            int var_start = 0;
            int var_len = 0;
            if (info->var_size_idx == 0) {
                var_start = 0;
                var_len = var_idx_[0];
            } else {
                var_start = var_idx_[info->var_size_idx - 1];
                var_len = var_idx_[info->var_size_idx] - var_idx_[info->var_size_idx - 1];
            }
            v = Value(std::string(&var_part_[var_start], var_len));
        }
        break;
    default:
        Log::fatal("unexpected value type %d", info->type);
        verify(0);
        break;
    }
    return v;
}

Value Row::get_column(const std::string& col_name) const {
    return get_column(schema_->get_column_id(col_name));
}

Row* Row::create(Schema* schema, const std::map<std::string, Value>& values) {
    Row* row = new Row;
    // TODO
    return row;
}


Row* Row::create(Schema* schema, const std::unordered_map<std::string, Value>& values) {
    Row* row = new Row;
    // TODO
    return row;
}


Row* Row::create(Schema* schema, const std::vector<Value>& values) {
    Row* row = new Row;
    row->schema_ = schema;
    row->fixed_part_ = new char[schema->fixed_part_size_];
    if (schema->var_size_cols_ > 0) {
        row->var_idx_ = new int[schema->var_size_cols_];
    }

    // 1st pass, write fixed part, and calculate var part size
    int var_part_size = 0;
    int fixed_pos = 0;
    for (auto& it: values) {
        switch (it.get_kind()) {
        case Value::I32:
            it.write_binary(&row->fixed_part_[fixed_pos]);
            fixed_pos += sizeof(i32);
            break;
        case Value::I64:
            it.write_binary(&row->fixed_part_[fixed_pos]);
            fixed_pos += sizeof(i64);
            break;
        case Value::F64:
            it.write_binary(&row->fixed_part_[fixed_pos]);
            fixed_pos += sizeof(f64);
            break;
        case Value::STR:
            var_part_size += it.get_str().size();
            break;
        default:
            Log::fatal("unexpected value type %d", it.get_kind());
            verify(0);
            break;
        }
    }

    if (schema->var_size_cols_ > 0) {
        // 2nd pass, write var part
        int var_counter = 0;
        int var_pos = 0;
        row->var_part_ = new char[var_part_size];
        for (auto& it: values) {
            if (it.get_kind() == Value::STR) {
                row->var_idx_[var_counter] = it.get_str().size();
                it.write_binary(&row->var_part_[var_pos]);
                var_counter++;
                var_pos += it.get_str().size();
            }
        }
        verify(var_part_size == var_pos);
    }

    return row;
}


} // namespace pkv

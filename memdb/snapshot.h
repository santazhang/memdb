#pragma once

#include <inttypes.h>
#include <map>

#include "utils.h"

namespace mdb {

typedef int64_t version_t;

template <class Key, class Value>
struct ref_sortedmap: public RefCounted {
    std::multimap<Key, Value> data;

    // protected dtor as required by RefCounted
protected:
    ~ref_sortedmap() {}
};

template <class Value>
struct versioned_value {
    version_t created_at, deleted_at;

    // const value, not modifiable
    const Value val;

    versioned_value(version_t created, const Value& v): created_at(created), deleted_at(-1), val(v) {}
    bool valid_at(version_t v) const {
        return created_at <= v && (deleted_at == -1 || v < deleted_at);
    }
};

template <class Key, class Value>
class snapshot_sortedmap {

    // empty class, used to mark a ctor as snapshotting
    class SnapshotMarker {};

    class Writer {
        // TODO
    };


    ref_sortedmap<Key, versioned_value<Value>>* data_;

    version_t ver_;

    // doubly linked list of snapshots
    mutable const snapshot_sortedmap* prev_;
    mutable const snapshot_sortedmap* next_;

    Writer* writer_;

    void make_me_snapshot_of(const snapshot_sortedmap& src) {
        verify(ver_ < 0);
        ver_ = src.ver_;
        verify(writer_ == nullptr);
        writer_ = nullptr;
        verify(data_ == nullptr);
        data_ = (ref_sortedmap<Key, versioned_value<Value>> *) src.data_->ref_copy();

        // set the doubly linked list
        if (src.prev_ == nullptr) {
            verify(src.next_ == nullptr);
            src.prev_ = this;
            src.next_ = this;
            this->prev_ = &src;
            this->next_ = &src;
        } else {
            verify(src.next_ != nullptr);
            // insert me after src
            this->prev_ = &src;
            this->next_ = src.next_;
            src.next_->prev_ = this;
            src.next_ = this;
        }
    }

    void destory_me() {
        // unlink me from the doubly linked list
        if (this->prev_ != nullptr) {
            verify(this->next_ != nullptr);
            if (this->prev_ == this->next_) {
                // special case for last element
                this->prev_->next_ = nullptr;
                this->next_->prev_ = nullptr;
            } else {
                this->prev_->next_ = this->next_;
                this->next_->prev_ = this->prev_;
            }
            this->prev_ = nullptr;
            this->next_ = nullptr;
        }

        if (writer_ != nullptr) {
            delete writer_;
            writer_ = nullptr;
        }
        data_->release();
        data_ = nullptr;
        ver_ = -1;
    }

    // creating a snapshot
    snapshot_sortedmap(const snapshot_sortedmap& src, const SnapshotMarker&)
            : data_(nullptr), ver_(-1), prev_(nullptr), next_(nullptr), writer_(nullptr) {
        make_me_snapshot_of(src);
    }

public:

    // creating a new snapshot_sortedmap
    snapshot_sortedmap(): ver_(0), prev_(nullptr), next_(nullptr) {
        writer_ = new Writer;
        data_ = new ref_sortedmap<Key, versioned_value<Value>>;
    }

    snapshot_sortedmap(const snapshot_sortedmap& src)
            : data_(nullptr), ver_(-1), prev_(nullptr), next_(nullptr), writer_(nullptr) {
        if (src.readonly()) {
            // src is a snapshot, make me a snapshot, too
            make_me_snapshot_of(src);
        } else {
            // TODO copy data (only current version)
        }
    }

    ~snapshot_sortedmap() {
        destory_me();
    }

    version_t version() const {
        return ver_;
    }

    bool valid() const {
        return ver_ >= 0;
    }

    bool readonly() const {
        return writer_ == nullptr;
    }

    const snapshot_sortedmap& operator= (const snapshot_sortedmap& src) {
        if (&src != this) {
            destory_me();
            if (src.readonly()) {
                make_me_snapshot_of(src);
            } else {
                // TODO copy data
            }
        }
        return *this;
    }

    snapshot_sortedmap snapshot() const {
        SnapshotMarker marker;
        return snapshot_sortedmap(*this, marker);
    }

    class SnapshotEnumerator: public Enumerator<const snapshot_sortedmap*> {
        const snapshot_sortedmap* o_;
        const snapshot_sortedmap* p_;
        bool first_;
        int count_;
    public:
        SnapshotEnumerator(const snapshot_sortedmap* ss): o_(ss), p_(ss), first_(true), count_(-1) {}
        bool has_next() {
            if (first_) {
                return true;
            }
            return p_ != nullptr && p_ != o_;
        }
        const snapshot_sortedmap* next() {
            const snapshot_sortedmap* ret = p_;
            p_ = p_->next_;
            first_ = false;
            return ret;
        }
        int count() {
            if (count_ > 0) {
                return count_;
            }
            count_ = 0;
            const snapshot_sortedmap* o = o_;
            const snapshot_sortedmap* p = o_->next_;
            if (p == nullptr) {
                count_ = 1;
            } else {
                for (;;) {
                    count_++;
                    if (p == o) {
                        break;
                    }
                    p = p->next_;
                }
            }
            return count_;
        }
    };

    SnapshotEnumerator all_snapshots() const {
        return SnapshotEnumerator(this);
    }

    // void insert(const Key& key, const Value& value) {
    //     verify(writer_ != nullptr);
    //     ver_++;
    //     versioned_value<Value> vv(ver_, value);
    //     insert_to_map(*data_, key, vv);
    // }

};

} // namespace mdb

#pragma once

#include <inttypes.h>
#include <map>
#include <set>

#include "utils.h"

namespace mdb { namespace snapshot {

typedef int64_t version_t;

template <class Value>
struct versioned_value {
    version_t created_at, removed_at;

    // const value, not modifiable
    const Value val;

    versioned_value(version_t created, const Value& v): created_at(created), removed_at(-1), val(v) {}
    bool valid_at(version_t v) const {
        return created_at <= v && (removed_at == -1 || v < removed_at);
    }
    // all future query will have version > v
    bool invalid_after(version_t v) const {
        return removed_at >= 0 && removed_at <= v + 1;
    }
    void remove(version_t v) {
        verify(removed_at == -1);
        removed_at = v;
        verify(created_at < removed_at);
    }
};

template <class Key, class Value, class Iterator, class Snapshot>
class snapshot_range: public Enumerator<std::pair<const Key*, const Value*>> {
    Snapshot snapshot_;
    Iterator begin_, end_, next_;
    bool cached_;
    std::pair<const Key*, const Value*> cached_next_;
    int count_;

    bool prefetch_next() {
        verify(cached_ == false);
        while (cached_ == false && next_ != end_) {
            if (next_->second.valid_at(snapshot_.version())) {
                cached_next_.first = &(next_->first);
                cached_next_.second = &(next_->second.val);
                cached_ = true;
            }
            ++next_;
        }
        return cached_;
    }

public:

    snapshot_range(const Snapshot& snapshot, Iterator it_begin, Iterator it_end)
        : snapshot_(snapshot), begin_(it_begin), end_(it_end), next_(it_begin), cached_(false), count_(-1) {}

    bool has_next() {
        if (cached_) {
            return true;
        } else {
            return prefetch_next();
        }
    }

    std::pair<const Key*, const Value*> next() {
        if (!cached_) {
            verify(prefetch_next());
        }
        cached_ = false;
        return cached_next_;
    }

    int count() {
        if (count_ >= 0) {
            return count_;
        }
        count_ = 0;
        for (auto it = begin_; it != end_; ++it) {
            if (it->second.valid_at(snapshot_.version())) {
                count_++;
            }
        }
        return count_;
    }

};

template <class Key, class Value, class Container, class Snapshot>
struct snapshotset: public RefCounted {
    Container data;
    Snapshot* writer;
    std::set<Snapshot*> reader;

    snapshotset(): writer(nullptr) {}

    // protected dtor as required by RefCounted
protected:
    ~snapshotset() {}
};

// empty class, used to mark a ctor as snapshotting
struct snapshot_marker {};

} // namespace mdb; namespace snapshot


using snapshot::version_t;
using snapshot::versioned_value;

template <class Key, class Value>
class snapshot_sortedmap {

public:

    typedef snapshot::snapshot_range<
        Key,
        Value,
        typename std::multimap<Key, versioned_value<Value>>::const_iterator,
        snapshot_sortedmap> kv_range;

    typedef snapshot::snapshotset<
        Key,
        Value,
        typename std::multimap<Key, versioned_value<Value>>,
        snapshot_sortedmap> snapshotset;

private:

    class Writer {
        snapshot_sortedmap* ss_;
    public:
        Writer(snapshot_sortedmap* ss): ss_(ss) {}
        void insert(const Key& key, const Value& value) {
            // advance to next version
            ss_->ver_++;
            versioned_value<Value> vv(ss_->ver_, value);
            insert_into_map(ss_->sss_->data, key, vv);
        }

        template <class Iterator>
        void insert(Iterator begin, Iterator end) {
            ss_->ver_++;
            while (begin != end) {
                versioned_value<Value> vv(ss_->ver_, begin->second);
                insert_into_map(ss_->sss_->data, begin->first, vv);
                ++begin;
            }
        }

        void insert(kv_range range) {
            ss_->ver_++;
            while (range) {
                std::pair<const Key*, const Value*> kv_pair = range.next();
                versioned_value<Value> vv(ss_->ver_, *(kv_pair.second));
                insert_into_map(ss_->sss_->data, *(kv_pair.first), vv);
            }
        }

        void remove_key(const Key& key) {
            ss_->ver_++;
            if (ss_->has_snapshot()) {
                for (auto it = ss_->sss_->data.lower_bound(key); it != ss_->sss_->data.upper_bound(key); ++it) {
                    it->second.remove(ss_->ver_);
                }
            } else {
                auto it = ss_->sss_->data.lower_bound(key);
                while (it != ss_->sss_->data.upper_bound(key)) {
                    it = ss_->sss_->data.erase(it);
                }
            }
        }
    };

    snapshotset* sss_;

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
        verify(sss_ == nullptr);
        sss_ = (snapshotset *) src.sss_->ref_copy();

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
        collect_my_garbage();

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
        sss_->release();
        sss_ = nullptr;
        ver_ = -1;
    }

    // creating a snapshot
    snapshot_sortedmap(const snapshot_sortedmap& src, const snapshot::snapshot_marker&)
            : sss_(nullptr), ver_(-1), prev_(nullptr), next_(nullptr), writer_(nullptr) {
        make_me_snapshot_of(src);
    }

public:

    // creating a new snapshot_sortedmap
    snapshot_sortedmap(): ver_(0), prev_(nullptr), next_(nullptr) {
        sss_ = new snapshotset;
        writer_ = new Writer(this);
    }

    snapshot_sortedmap(const snapshot_sortedmap& src)
            : sss_(nullptr), ver_(-1), prev_(nullptr), next_(nullptr), writer_(nullptr) {
        if (src.readonly()) {
            // src is a snapshot, make me a snapshot, too
            make_me_snapshot_of(src);
        } else {
            sss_ = new snapshotset;
            writer_ = new Writer(this);
            writer_->insert(src.all());
        }
    }

    template <class Iterator>
    snapshot_sortedmap(Iterator it_begin, Iterator it_end): ver_(0), prev_(nullptr), next_(nullptr) {
        sss_ = new snapshotset;
        writer_ = new Writer(this);
        writer_->insert(it_begin, it_end);
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
                verify(prev_ == nullptr);
                verify(next_ == nullptr);
                verify(ver_ == -1);
                verify(sss_ == nullptr);
                verify(writer_ == nullptr);
                ver_ = 0;
                sss_ = new snapshotset;
                writer_ = new Writer(this);
                writer_->insert(src.all());
            }
        }
        return *this;
    }

    bool has_snapshot() const {
        if (prev_ == nullptr) {
            verify(next_ == nullptr);
            return false;
        }
        return true;
    }

    snapshot_sortedmap snapshot() const {
        return snapshot_sortedmap(*this, snapshot::snapshot_marker());
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

    void insert(const Key& key, const Value& value) {
        verify(!readonly());
        writer_->insert(key, value);
    }

    template <class Iterator>
    void insert(Iterator begin, Iterator end) {
        writer_->insert(begin, end);
    }

    void remove_key(const Key& key) {
        verify(!readonly());
        writer_->remove_key(key);
    }

    kv_range all() const {
        return kv_range(this->snapshot(), this->sss_->data.begin(), this->sss_->data.end());
    }

    kv_range query(const Key& key) const {
        return kv_range(this->snapshot(), this->sss_->data.lower_bound(key), this->sss_->data.upper_bound(key));
    }

    kv_range query_lt(const Key& key) const {
        return kv_range(this->snapshot(), this->sss_->data.begin(), this->sss_->data.lower_bound(key));
    }

    kv_range query_gt(const Key& key) const {
        return kv_range(this->snapshot(), this->sss_->data.upper_bound(key), this->sss_->data.end());
    }

    size_t total_data_count() const {
        return this->sss_->data.size();
    }

private:

    void collect_my_garbage() {
        if (this->ver_ < 0) {
            return;
        }

        // ENHANCE faster garbage collection (on data only visible to this snapshot)
        auto snapshots = all_snapshots();
        while (snapshots) {
            const snapshot_sortedmap* ss = snapshots.next();
            if (ss != this && ss->ver_ <= this->ver_) {
                return;
            }
        }

        auto it = sss_->data.begin();
        while (it != sss_->data.end()) {
            // all future query will have version > this->ver_
            if (it->second.invalid_after(this->ver_)) {
                it = sss_->data.erase(it);
            } else {
                ++it;
            }
        }
    }

};

} // namespace mdb

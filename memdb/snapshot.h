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

// note: this includes both writer and snapshots
template <class Key, class Value, class Container, class Snapshot>
struct snapshotset: public RefCounted {
    Container data;
    Snapshot* writer;
    std::set<Snapshot*> snapshots;

    snapshotset(Snapshot* w): writer(w) {}

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

    bool rdonly_;
    version_t ver_;
    snapshotset* sss_;

    void make_me_snapshot_of(const snapshot_sortedmap& src) {
        verify(ver_ < 0);
        ver_ = src.ver_;
        verify(rdonly_ == true);
        verify(sss_ == nullptr);
        sss_ = (snapshotset *) src.sss_->ref_copy();
        sss_->snapshots.insert(this);
    }

    void destory_me() {
        collect_my_garbage();

        if (sss_->writer == this) {
            sss_->writer = nullptr;
        } else {
            sss_->snapshots.erase(this);
        }

        sss_->release();
        sss_ = nullptr;
        rdonly_ = true;
        ver_ = -1;
    }

    // creating a snapshot
    snapshot_sortedmap(const snapshot_sortedmap& src, const snapshot::snapshot_marker&)
            : rdonly_(true), ver_(-1), sss_(nullptr) {
        make_me_snapshot_of(src);
    }

public:

    // creating a new snapshot_sortedmap
    snapshot_sortedmap(): rdonly_(false), ver_(0) {
        sss_ = new snapshotset(this);
    }

    snapshot_sortedmap(const snapshot_sortedmap& src)
            : rdonly_(false), ver_(-1), sss_(nullptr) {
        if (src.readonly()) {
            // src is a snapshot, make me a snapshot, too
            rdonly_ = true;
            ver_ = -1;
            sss_ = nullptr;
            make_me_snapshot_of(src);
        } else {
            rdonly_ = false;
            ver_ = 0;
            sss_ = new snapshotset(this);
            insert(src.all());
        }
    }

    template <class Iterator>
    snapshot_sortedmap(Iterator it_begin, Iterator it_end): rdonly_(false), ver_(0) {
        sss_ = new snapshotset(this);
        insert(it_begin, it_end);
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
        if (rdonly_) {
            verify(sss_->writer != this);
        }
        return rdonly_;
    }

    const snapshot_sortedmap& operator= (const snapshot_sortedmap& src) {
        if (&src != this) {
            destory_me();
            if (src.readonly()) {
                make_me_snapshot_of(src);
            } else {
                verify(ver_ == -1);
                verify(sss_ == nullptr);
                verify(rdonly_ == true);
                rdonly_ = false;
                ver_ = 0;
                sss_ = new snapshotset(this);
                insert(src.all());
            }
        }
        return *this;
    }

    // snapshot: readonly
    bool has_snapshot() const {
        return !sss_->snapshots.empty();
    }

    snapshot_sortedmap snapshot() const {
        return snapshot_sortedmap(*this, snapshot::snapshot_marker());
    }

    std::set<snapshot_sortedmap*> all_snapshots() const {
        return this->sss_->snapshots;
    }

    void insert(const Key& key, const Value& value) {
        verify(!readonly());
        ver_++;
        versioned_value<Value> vv(ver_, value);
        insert_into_map(sss_->data, key, vv);
    }

    template <class Iterator>
    void insert(Iterator begin, Iterator end) {
        verify(!readonly());
        ver_++;
        while (begin != end) {
            versioned_value<Value> vv(ver_, begin->second);
            insert_into_map(sss_->data, begin->first, vv);
            ++begin;
        }
    }

    void insert(kv_range range) {
        verify(!readonly());
        ver_++;
        while (range) {
            std::pair<const Key*, const Value*> kv_pair = range.next();
            versioned_value<Value> vv(ver_, *(kv_pair.second));
            insert_into_map(sss_->data, *(kv_pair.first), vv);
        }
    }

    void remove_key(const Key& key) {
        verify(!readonly());
        ver_++;
        if (has_snapshot()) {
            for (auto it = sss_->data.lower_bound(key); it != sss_->data.upper_bound(key); ++it) {
                it->second.remove(ver_);
            }
        } else {
            auto it = sss_->data.lower_bound(key);
            while (it != sss_->data.upper_bound(key)) {
                it = sss_->data.erase(it);
            }
        }
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
        for (auto& it : all_snapshots()) {
            if (it != this && it->ver_ <= this->ver_) {
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

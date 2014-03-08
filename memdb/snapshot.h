#pragma once

#include <inttypes.h>
#include <map>
#include <set>

#include "utils.h"

namespace mdb {

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
    bool invalid_at_and_before(version_t v) const {
        return v < created_at;
    }
    bool invalid_at_and_after(version_t v) const {
        return removed_at >= 0 && removed_at <= v;
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
    std::pair<const Key&, const Value&> cached_next_2;
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
        : snapshot_(snapshot), begin_(it_begin), end_(it_end), next_(it_begin),
          cached_(false), cached_next_2(it_begin->first, it_begin->second.val), count_(-1) {}

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
    std::multimap<version_t, std::pair<Key, Key>> removed_key_ranges;
    Snapshot* writer;
    std::set<Snapshot*> snapshots;

    snapshotset(Snapshot* w): writer(w) {}

    // protected dtor as required by RefCounted
protected:
    ~snapshotset() {}
};

// empty class, used to mark a ctor as snapshotting
struct snapshot_marker {};


template <class Key, class Value>
class snapshot_sortedmap {

public:

    typedef snapshot_range<
        Key,
        Value,
        typename std::multimap<Key, versioned_value<Value>>::const_iterator,
        snapshot_sortedmap> kv_range;

    typedef snapshotset<
        Key,
        Value,
        typename std::multimap<Key, versioned_value<Value>>,
        snapshot_sortedmap> snapshotset;

    typedef typename std::pair<const Key&, const Value&> value_type;

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
    snapshot_sortedmap(const snapshot_sortedmap& src, const snapshot_marker&)
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
        return snapshot_sortedmap(*this, snapshot_marker());
    }

    const std::set<snapshot_sortedmap*>& all_snapshots() const {
        return this->sss_->snapshots;
    }

    void insert(const Key& key, const Value& value) {
        verify(!readonly());
        ver_++;
        versioned_value<Value> vv(ver_, value);
        insert_into_map(sss_->data, key, vv);
    }

    void insert(const value_type kv_pair) {
        verify(!readonly());
        ver_++;
        versioned_value<Value> vv(ver_, kv_pair.second);
        insert_into_map(sss_->data, kv_pair.first, vv);
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
                verify(key == it->first);
                it->second.remove(ver_);
            }
            insert_into_map(sss_->removed_key_ranges, ver_, std::make_pair(key, key));
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

        // ENHANCE: when removing a snapshot S, GC keys only visible to this snapshot
        // if S is writer, let S' be the snapshot with highest version, any key invalid_at_and_after(S') should be collected
        // if S is reader, let S1 < S < S2, keys created_at > S1, deleted_at <= S2 should be collected

        // handle the special case of writer being destroyed
        if (!this->rdonly_ && !all_snapshots().empty()) {
            version_t max_ver = -1;
            for (auto it: all_snapshots()) {
                if (max_ver < it->version()) {
                    max_ver = it->version();
                }
            }
            auto it = sss_->data.begin();
            while (it != sss_->data.end()) {
                // all future query will have version > next_smallest_ver
                if (it->second.invalid_at_and_before(max_ver)) {
                    it = sss_->data.erase(it);
                } else {
                    ++it;
                }
            }
            return;
        }

        for (auto& it : all_snapshots()) {
            if (it != this && it->version() <= this->version()) {
                return;
            }
        }

        version_t next_smallest_ver = -1;
        for (auto& it : all_snapshots()) {
            if (it == this) {
                continue;
            }
            next_smallest_ver = it->version();
            break;
        }
        if (next_smallest_ver == -1) {
            next_smallest_ver = ver_ + 1;
        }

        // GC based on tracking removed keys
        auto it_key_range = sss_->removed_key_ranges.begin();
        while (it_key_range != sss_->removed_key_ranges.upper_bound(next_smallest_ver)) {

            const Key& low = it_key_range->second.first;
            const Key& high = it_key_range->second.second;
            verify(low <= high);

            auto it = sss_->data.lower_bound(low);
            while (it != sss_->data.upper_bound(high)) {
                // all future query will have version > next_smallest_ver
                if (it->second.invalid_at_and_after(next_smallest_ver)) {
                    it = sss_->data.erase(it);
                } else {
                    ++it;
                }
            }
            it_key_range = sss_->removed_key_ranges.erase(it_key_range);
        }
        return;
    }

};

template <class Key, class Value>
using snapshotmap = snapshot_sortedmap<Key, Value>;

} // namespace mdb

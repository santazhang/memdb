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
        assert(removed_at == -1);
        removed_at = v;
        assert(created_at < removed_at);
    }
};

template <class Key, class Value, class Iterator, class Snapshot>
class snapshot_range: public Enumerator<std::pair<const Key&, const Value&>> {
    Snapshot snapshot_;
    Iterator begin_, end_, next_;
    bool cached_;
    std::pair<const Key*, const Value*> cached_next_;
    int count_;

    bool prefetch_next() {
        assert(cached_ == false);
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

    std::pair<const Key&, const Value&> next() {
        if (!cached_) {
            assert(prefetch_next());
        }
        cached_ = false;
        return std::pair<const Key&, const Value&>(*cached_next_.first, *cached_next_.second);
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

// A group of snapshots. Each snapshot in the group points to it, so they can share data.
// There could be at most one writer in the group. Members are ordered in a doubly linked list:
// S1 <= S2 <= S3 <= ... <= Sw (increasing version, writer at tail if exists)
template <class Key, class Value, class Container, class Snapshot>
struct snapshot_group: public RefCounted {
    Container data;
    std::multimap<version_t, std::pair<Key, Key>> removed_key_ranges;

    // the writer of the group, nullptr means nobody can write to the group
    Snapshot* writer;

    snapshot_group(Snapshot* w): writer(w) {}

    // protected dtor as required by RefCounted
protected:
    ~snapshot_group() {}
};

template <class Key, class Value>
class snapshot_sortedmap {
public:

    typedef snapshot_range<
        Key,
        Value,
        typename std::multimap<Key, versioned_value<Value>>::const_iterator,
        snapshot_sortedmap> range_type;

    typedef snapshot_group<
        Key,
        Value,
        typename std::multimap<Key, versioned_value<Value>>,
        snapshot_sortedmap> snapshot_group;

    typedef typename std::pair<const Key&, const Value&> value_type;

    // creating a new snapshot_sortedmap
    snapshot_sortedmap(): ver_(0), prev_(this), next_(this) {
        ssg_ = new snapshot_group(this);
    }

    snapshot_sortedmap(const snapshot_sortedmap& src): ver_(-1), ssg_(nullptr), prev_(nullptr), next_(nullptr) {
        if (src.readonly()) {
            // src is a snapshot, make me a snapshot, too
            make_me_snapshot_of(src);
        } else {
            if (!src.has_writable_snapshot() && (src.ver_ > src.next_->ver_)) {
                // tiny optimization:
                // if 1) src does not have writable snapshot in its group,
                // and 2) src is the snapshot with largest version in its group,
                // let this become the new writer in src's group
                ver_ = src.ver_;
                ssg_ = (snapshot_group *) src.ssg_->ref_copy();
                // note that we are adding this to the right side of src in the group list
                prev_ = &src;
                next_ = src.next_;
                src.next_ = this;
                next_->prev_ = this;
                assert(debug_group_sanity_check());
            } else {
                // otherwise: just copy everything!
                ver_ = 0;
                ssg_ = new snapshot_group(this);
                prev_ = this;
                next_ = this;
                insert(src.all());
            }
        }
    }

    template <class Iterator>
    snapshot_sortedmap(Iterator it_begin, Iterator it_end): ver_(0), prev_(this), next_(this) {
        ssg_ = new snapshot_group(this);
        insert(it_begin, it_end);
    }

    ~snapshot_sortedmap() {
        destory_me();
    }

    version_t version() const {
        return ver_;
    }

    bool readonly() const {
        assert(this != nullptr);
        return ssg_->writer != this;
    }

    bool writable() const {
        assert(this != nullptr);
        return ssg_->writer == this;
    }

    const snapshot_sortedmap& operator= (const snapshot_sortedmap& src) {
        assert(src.ver_ != -1);
        assert(src.ssg_ != nullptr);
        assert(src.prev_ != nullptr);
        assert(src.next_ != nullptr);
        if (&src != this) {
            destory_me();
            if (src.readonly()) {
                make_me_snapshot_of(src);
            } else {
                assert(ver_ == -1);
                assert(ssg_ == nullptr);
                assert(prev_ == nullptr);
                assert(next_ == nullptr);
                ver_ = 0;
                ssg_ = new snapshot_group(this);
                prev_ = this;
                next_ = this;
                insert(src.all());
            }
        }
        assert(ver_ != -1);
        assert(ssg_ != nullptr);
        assert(prev_ != nullptr);
        assert(next_ != nullptr);
        return *this;
    }

    // snapshot: readonly
    bool has_readonly_snapshot() const {
        if (this->readonly()) {
            return true;
        } else {
            // I'm the writer!
            assert(this->ssg_->writer == this);
            return this->prev_->readonly();
        }
    }

    bool has_writable_snapshot() const {
        return ssg_->writer != nullptr;
    }

    size_t snapshot_count() const {
        size_t count = 0;
        const snapshot_sortedmap* p = this;
        const snapshot_sortedmap* q = this;
        do {
            count++;
            q = q->next_;
        } while(q != p);
        return count;
    }

    snapshot_sortedmap snapshot() const {
        assert(ver_ != -1);
        assert(ssg_ != nullptr);
        assert(prev_ != nullptr);
        assert(next_ != nullptr);
        return snapshot_sortedmap(*this, snapshot_marker());
    }

    void insert(const Key& key, const Value& value) {
        verify(writable());
        ver_++;
        versioned_value<Value> vv(ver_, value);
        insert_into_map(ssg_->data, key, vv);
    }

    void insert(const value_type& kv_pair) {
        verify(writable());
        ver_++;
        versioned_value<Value> vv(ver_, kv_pair.second);
        insert_into_map(ssg_->data, kv_pair.first, vv);
    }

    template <class Iterator>
    void insert(Iterator begin, Iterator end) {
        verify(writable());
        ver_++;
        while (begin != end) {
            versioned_value<Value> vv(ver_, begin->second);
            insert_into_map(ssg_->data, begin->first, vv);
            ++begin;
        }
    }

    void insert(range_type range) {
        verify(writable());
        ver_++;
        while (range) {
            value_type kv_pair = range.next();
            versioned_value<Value> vv(ver_, kv_pair.second);
            insert_into_map(ssg_->data, kv_pair.first, vv);
        }
    }

    void erase(const Key& key) {
        verify(writable());
        ver_++;
        if (has_readonly_snapshot()) {
            for (auto it = ssg_->data.lower_bound(key); it != ssg_->data.upper_bound(key); ++it) {
                assert(key == it->first);
                it->second.remove(ver_);
            }
            insert_into_map(ssg_->removed_key_ranges, ver_, std::make_pair(key, key));
        } else {
            auto it = ssg_->data.lower_bound(key);
            while (it != ssg_->data.upper_bound(key)) {
                it = ssg_->data.erase(it);
            }
        }
    }

    range_type all() const {
        return range_type(this->snapshot(), this->ssg_->data.begin(), this->ssg_->data.end());
    }

    range_type query(const Key& key) const {
        return range_type(this->snapshot(), this->ssg_->data.lower_bound(key), this->ssg_->data.upper_bound(key));
    }

    range_type query_lt(const Key& key) const {
        return range_type(this->snapshot(), this->ssg_->data.begin(), this->ssg_->data.lower_bound(key));
    }

    range_type query_gt(const Key& key) const {
        return range_type(this->snapshot(), this->ssg_->data.upper_bound(key), this->ssg_->data.end());
    }

    size_t debug_storage_size() const {
        return this->ssg_->data.size();
    }

private:

    // empty struct, used to mark a ctor as snapshotting
    struct snapshot_marker {};

    version_t ver_;
    snapshot_group* ssg_;

    // doubly linked list of all snapshots in a group
    mutable const snapshot_sortedmap* prev_;
    mutable const snapshot_sortedmap* next_;


    // creating a snapshot
    snapshot_sortedmap(const snapshot_sortedmap& src, const snapshot_marker&)
            : ver_(-1), ssg_(nullptr), prev_(nullptr), next_(nullptr) {
        make_me_snapshot_of(src);
    }


    void make_me_snapshot_of(const snapshot_sortedmap& src) {
        assert(ver_ < 0);
        ver_ = src.ver_;
        assert(ssg_ == nullptr);
        ssg_ = (snapshot_group *) src.ssg_->ref_copy();

        // join snapshot group doubly linked list
        assert(prev_ == nullptr);
        assert(next_ == nullptr);
        assert(src.prev_ != nullptr);
        assert(src.next_ != nullptr);
        // insert to left of src, because src might be writable, and increase its version
        this->prev_ = src.prev_;
        this->next_ = &src;
        src.prev_->next_ = this;
        src.prev_ = this;
        assert(debug_group_sanity_check());
    }

    void destory_me() {
        assert(debug_group_sanity_check());
        collect_my_garbage();

        if (ssg_->writer == this) {
            // remove writer in snapshot group
            ssg_->writer = nullptr;
        }

        if (prev_ != this) {
            // not the last item, update doubly linked list
            prev_->next_ = this->next_;
            next_->prev_ = this->prev_;
        }

        this->prev_ = nullptr;
        this->next_ = nullptr;
        ssg_->release();
        ssg_ = nullptr;
        ver_ = -1;
    }

    bool debug_group_sanity_check() {
        if (ssg_->writer != nullptr) {
            const snapshot_sortedmap* w = ssg_->writer;
            // check writer is in group
            bool writer_is_in_group = false;
            const snapshot_sortedmap* p = this;
            const snapshot_sortedmap* q = this;
            do {
                if (q == w) {
                    writer_is_in_group = true;
                    break;
                }
                q = q->next_;
            } while(q != p);
            verify(writer_is_in_group);
            // check writer has highest version
            verify(w->ver_ >= w->prev_->ver_);
            verify(w->ver_ >= w->next_->ver_);
        }
        // check ordering
        int order_flip_count = 0;
        const snapshot_sortedmap* p = this;
        const snapshot_sortedmap* q = this;
        do {
            if (q->ver_ > q->next_->ver_) {
                order_flip_count++;
            }
            q = q->next_;
        } while(q != p);
        verify(order_flip_count < 2);
        return true;
    }

    void gc_snapshot_group_front() {
        assert(prev_->ver_ >= next_->ver_);
        version_t gc_before;
        version_t gc_after;
        // TODO
    }

    void gc_snapshot_group_middle() {
        // TODO
    }

    void gc_snapshot_group_back() {
        assert(prev_->ver_ >= next_->ver_);
        // TODO
    }

    void collect_my_garbage() {
        if (prev_ == this) {
            assert(prev_ == next_ && this != nullptr);
            // last instance, do nothing, let dtor take over
        } else if (prev_->ver_ > this->ver_ && this->ver_ < next_->ver_) {
            gc_snapshot_group_front();
        } else if (prev_->ver_ < this->ver_ && this->ver_ < next_->ver_) {
            gc_snapshot_group_middle();
        } else if (prev_->ver_ < this->ver_ && this->ver_ > next_->ver_) {
            gc_snapshot_group_back();
        }
    }

};


} // namespace mdb

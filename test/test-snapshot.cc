#include <string>

#include "base/all.h"
#include "memdb/snapshot.h"

using namespace base;
using namespace mdb;
using namespace std;

TEST(snapshot, snapshot_on_empty_table) {
    snapshot_sortedmap<int, string> data;
    EXPECT_EQ(data.all_snapshots().count(), 1);
    {
        snapshot_sortedmap<int, string> snapshot = data.snapshot();
        EXPECT_EQ(data.all_snapshots().count(), 2);
        EXPECT_EQ(snapshot.all_snapshots().count(), 2);
        data.snapshot();
        data.snapshot();
        snapshot = snapshot.snapshot();
        EXPECT_EQ(snapshot.all_snapshots().count(), 2);
        EXPECT_EQ(data.version(), 0);
        EXPECT_EQ(snapshot.version(), 0);
        EXPECT_TRUE(data.valid());
        EXPECT_TRUE(snapshot.valid());
        EXPECT_FALSE(data.readonly());
        data = snapshot.snapshot();
        EXPECT_TRUE(data.readonly());
        EXPECT_TRUE(snapshot.readonly());
        EXPECT_EQ(data.all_snapshots().count(), 2);
    }
    EXPECT_EQ(data.all_snapshots().count(), 1);
}

static void print_range(snapshot_sortedmap<int, string>::kv_range range) {
    while (range) {
        pair<const int*, const string*> kv_pair = range.next();
        Log::debug("%d => %s", *kv_pair.first, kv_pair.second->c_str());
    }
}

TEST(snapshot, versioned_query) {
    snapshot_sortedmap<int, string> data;
    data.insert(1, "hello");
    auto range1 = data.all();
    data.insert(2, "world");
    auto range2 = data.all();
    data.remove_key(1);
    auto range3 = data.all();
    Log::debug("v1: %d elements", range1.count());
    print_range(range1);
    Log::debug("v2: %d elements", range2.count());
    print_range(range2);
    Log::debug("v3: %d elements", range3.count());
    print_range(range3);
    EXPECT_EQ(range1.count(), 1);
    EXPECT_EQ(range2.count(), 2);
    EXPECT_EQ(range3.count(), 1);
}

TEST(snapshot, insert_many) {
    map<int, string> m;
    m[1] = "hello";
    m[2] = "world";
    m[3] = "snapshot";
    m[4] = "is";
    m[5] = "awesome";
    snapshot_sortedmap<int, string> data;
    data.insert(m.begin(), m.end());
    print_range(data.all());
    Log::debug("version: %d", data.version());
    EXPECT_EQ(data.all().count(), 5);
}

TEST(snapshot, copy_ctor) {
    map<int, string> m;
    m[1] = "hello";
    m[2] = "world";
    m[3] = "snapshot";
    m[4] = "is";
    m[5] = "awesome";
    snapshot_sortedmap<int, string> data;
    data.insert(m.begin(), m.end());
    print_range(data.all());
    Log::debug("version: %d", data.version());
    snapshot_sortedmap<int, string> data2(data);
    print_range(data2.all());
    Log::debug("version: %d", data2.version());
    EXPECT_EQ(data.all().count(), 5);
    EXPECT_EQ(data2.all().count(), 5);
}

TEST(snapshot, assignment) {
    map<int, string> m;
    m[1] = "hello";
    m[2] = "world";
    m[3] = "snapshot";
    m[4] = "is";
    m[5] = "awesome";
    snapshot_sortedmap<int, string> data(m.begin(), m.end());
    {
        snapshot_sortedmap<int, string> data2;
        data2 = data;
        EXPECT_FALSE(data2.readonly());
        print_range(data2.all());
    }

    {
        snapshot_sortedmap<int, string> data2 = data.snapshot();
        EXPECT_TRUE(data2.readonly());
        data2 = data2.snapshot();
        EXPECT_TRUE(data2.readonly());
        print_range(data2.all());
        data2 = data;
        EXPECT_FALSE(data2.readonly());
        print_range(data2.all());
    }

    data = data.snapshot();
    EXPECT_TRUE(data.readonly());
    EXPECT_EQ(data.all().count(), 5);

    {
        snapshot_sortedmap<int, string> data2 = data.snapshot();
        EXPECT_TRUE(data2.readonly());
        data2 = data2.snapshot();
        EXPECT_TRUE(data2.readonly());
        print_range(data2.all());
        data2 = data;
        EXPECT_TRUE(data2.readonly());
        print_range(data2.all());
    }
}

TEST(snapshot, multi_version) {
    const int n = 1000;
    vector<snapshot_sortedmap<int, string>> snapshots;
    {
        snapshot_sortedmap<int, string> ss;
        snapshots.push_back(ss.snapshot());
        for (int i = 0; i < n; i++) {
            ss.insert(i, to_string(i));
            snapshots.push_back(ss.snapshot());
        }
    }
    for (int i = 0; i < n + 1; i++) {
        EXPECT_EQ(snapshots[i].all().count(), i);
        EXPECT_TRUE(snapshots[i].readonly());
    }
}

TEST(snapshot, multi_version_gc) {
    snapshot_sortedmap<int, string> final_ss;
    {
        snapshot_sortedmap<int, string> ss;
        const int n = 1000;
        vector<snapshot_sortedmap<int, string>> snapshots;
        {
            snapshots.push_back(ss.snapshot());
            for (int i = 0; i < n; i++) {
                ss.insert(i, to_string(i));
                snapshots.push_back(ss.snapshot());
            }
        }
        for (int i = 0; i < n + 1; i++) {
            EXPECT_EQ(snapshots[i].all().count(), i);
            EXPECT_TRUE(snapshots[i].readonly());
        }
        {
            for (int i = 0; i < n; i++) {
                ss.remove_key(i);
                snapshots.push_back(ss.snapshot());
            }
        }
        for (int i = 0; i < n; i++) {
            EXPECT_EQ(snapshots[n + i].all().count(), n - i);
            EXPECT_TRUE(snapshots[n + i].readonly());
        }
        final_ss = snapshots.back();
    }
    EXPECT_TRUE(final_ss.readonly());
    EXPECT_EQ(final_ss.all().count(), 0);
    Log::debug("total data count = %d", final_ss.total_data_count());
}


TEST(snapshot, gc) {
    {
        snapshot_sortedmap<int, string> ss;
        ss.insert(1, "hi");
        {
            auto snap = ss.snapshot();
            EXPECT_EQ(ss.all().count(), 1);
            print_range(ss.all());
            ss.remove_key(1);
            EXPECT_EQ(snap.all().count(), 1);
            EXPECT_EQ(ss.all().count(), 0);
        }
        print_range(ss.all());
    }

    // assignment
    {
        snapshot_sortedmap<int, string> ss;
        ss.insert(1, "hi");
        ss.insert(2, "hello");
        snapshot_sortedmap<int, string> ss2;
        ss = ss2;
        EXPECT_FALSE(ss.readonly());
        EXPECT_FALSE(ss2.readonly());
        EXPECT_EQ(ss.total_data_count(), (size_t) 0);
        EXPECT_EQ(ss2.total_data_count(), (size_t) 0);
    }
}

TEST(snapshot, versions_and_gc) {
    snapshot_sortedmap<int, string>* s1 = new snapshot_sortedmap<int, string>;
    EXPECT_EQ(s1->version(), 0);
    s1->insert(1, "hi");
    EXPECT_EQ(s1->version(), 1);
    s1->insert(2, "hello");
    EXPECT_EQ(s1->version(), 2);
    snapshot_sortedmap<int, string>* s2 = new snapshot_sortedmap<int, string>(s1->snapshot());
    EXPECT_EQ(s2->version(), 2);
    s1->remove_key(1);
    EXPECT_EQ(s1->version(), 3);
    s1->remove_key(2);
    EXPECT_EQ(s1->version(), 4);
    EXPECT_EQ(s1->total_data_count(), 2u);
    Log::debug("total data count = %d", s1->total_data_count());
    delete s2;
    // now "1 => hi" is garbage collected
    EXPECT_EQ(s1->total_data_count(), 1u);
    Log::debug("total data count = %d", s1->total_data_count());
    delete s1;
}

TEST(snapshot, fast_key_removal) {
    snapshot_sortedmap<int, string> ss;
    ss.insert(1, "hi");
    ss.insert(2, "hello");
    EXPECT_EQ(ss.total_data_count(), (size_t) 2);
    ss.remove_key(1);
    EXPECT_EQ(ss.total_data_count(), (size_t) 1);
    ss.remove_key(2);
    EXPECT_EQ(ss.total_data_count(), (size_t) 0);
}


TEST(snapshot, version_validation) {
    versioned_value<string> v(1987, "");
    EXPECT_FALSE(v.valid_at(1986));
    EXPECT_TRUE(v.valid_at(1987));
    EXPECT_TRUE(v.valid_at(1988));
    v.remove(1988);
    EXPECT_FALSE(v.valid_at(1986));
    EXPECT_TRUE(v.valid_at(1987));
    EXPECT_FALSE(v.valid_at(1988));
    EXPECT_FALSE(v.invalid_after(1986));
    EXPECT_TRUE(v.invalid_after(1987));
    EXPECT_TRUE(v.invalid_after(1988));
}

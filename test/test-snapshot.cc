#include <string>

#include "base/all.h"
#include "memdb/snapshot.h"

using namespace base;
using namespace mdb;
using namespace std;

TEST(snapshot, snapshot_memory_size_should_be_very_small) {
    Log::debug("sizeof(snapshot_sortedmap<int, string>) = %d", sizeof(snapshot_sortedmap<int, string>));
    EXPECT_LE(sizeof(snapshot_sortedmap<int, string>), 32u);
}

TEST(snapshot, snapshot_on_empty_table) {
    snapshot_sortedmap<int, string> data;
    EXPECT_FALSE(data.has_readonly_snapshot());
    EXPECT_TRUE(data.has_writable_snapshot());
    EXPECT_EQ(data.snapshot_count(), 1u);
    {
        snapshot_sortedmap<int, string> snapshot = data.snapshot();
        EXPECT_EQ(data.snapshot_count(), 2u);
        EXPECT_TRUE(data.has_readonly_snapshot());
        EXPECT_EQ(snapshot.snapshot_count(), 2u);
        data.snapshot();
        data.snapshot();
        snapshot = snapshot.snapshot();
        EXPECT_EQ(snapshot.snapshot_count(), 2u);
        EXPECT_EQ(data.version(), 0);
        EXPECT_EQ(snapshot.version(), 0);
        EXPECT_FALSE(data.readonly());

        // make data a snapshot
        data = snapshot.snapshot();
        EXPECT_FALSE(data.has_writable_snapshot());
        EXPECT_TRUE(data.readonly());
        EXPECT_TRUE(snapshot.readonly());
        EXPECT_EQ(data.snapshot_count(), 2u);
    }
    EXPECT_EQ(data.snapshot_count(), 1u);
}

static void print_range(snapshot_sortedmap<int, string>::range_type range) {
    while (range) {
        pair<const int&, const string&> kv_pair = range.next();
        Log::debug("%d => %s", kv_pair.first, kv_pair.second.c_str());
    }
}

TEST(snapshot, versioned_query) {
    snapshot_sortedmap<int, string> data;
    data.insert(1, "hello");
    auto range1 = data.all();
    data.insert(2, "world");
    auto range2 = data.all();
    data.erase(1);
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
        const int n = 10000;
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
                ss.erase(i);
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
    Log::debug("total data count = %d", final_ss.gc_size());
}


TEST(snapshot, gc) {
    {
        snapshot_sortedmap<int, string> ss;
        ss.insert(1, "hi");
        {
            auto snap = ss.snapshot();
            EXPECT_EQ(ss.all().count(), 1);
            print_range(ss.all());
            ss.erase(1);
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
        EXPECT_EQ(ss.gc_size(), (size_t) 0);
        EXPECT_EQ(ss2.gc_size(), (size_t) 0);
    }
}

TEST(snapshot, versions_and_gc) {
    snapshot_sortedmap<int, string>* s1 = new snapshot_sortedmap<int, string>;
    EXPECT_EQ(s1->version(), 0);
    s1->insert(1, "hi");
    EXPECT_EQ(s1->version(), 1);
    snapshot_sortedmap<int, string>* sa = new snapshot_sortedmap<int, string>(s1->snapshot());
    s1->insert(2, "hello");
    EXPECT_EQ(s1->version(), 2);
    print_range(s1->all());
    snapshot_sortedmap<int, string>* s2 = new snapshot_sortedmap<int, string>(s1->snapshot());
    EXPECT_EQ(s2->version(), 2);
    s1->erase(1);
    EXPECT_EQ(s1->version(), 3);
    s1->erase(2);
    EXPECT_EQ(s1->version(), 4);
    EXPECT_EQ(s1->gc_size(), 2u);
    Log::debug("total data count = %d", s1->gc_size());
    print_range(s2->all());
    print_range(s1->all());
    delete s2;
    s1->gc_run();
    EXPECT_EQ(s1->gc_size(), 2u);
    delete sa;
    s1->gc_run();
    print_range(s1->all());
    s1->gc_run();
    EXPECT_EQ(s1->gc_size(), 1u);
    Log::debug("total data count = %d", s1->gc_size());
    delete s1;
}

TEST(snapshot, fast_key_removal) {
    snapshot_sortedmap<int, string> ss;
    ss.insert(1, "hi");
    ss.insert(2, "hello");
    EXPECT_EQ(ss.gc_size(), (size_t) 2);
    ss.erase(1);
    EXPECT_EQ(ss.gc_size(), (size_t) 1);
    ss.erase(2);
    EXPECT_EQ(ss.gc_size(), (size_t) 0);
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
}

TEST(snapshot, remove_writer_gc) {
    snapshot_sortedmap<int, string>* ss = new snapshot_sortedmap<int, string>;
    ss->insert(1, "hi");
    snapshot_sortedmap<int, string> snap = ss->snapshot();
    EXPECT_FALSE(ss->readonly());
    EXPECT_EQ(snap.all().count(), 1);
    EXPECT_TRUE(snap.readonly());
    for (int i = 0; i < 10; i++) {
        ss->insert(i, to_string(i));
    }
    snapshot_sortedmap<int, string>* snap2 = new snapshot_sortedmap<int, string>(ss->snapshot());
    EXPECT_TRUE(snap2->readonly());
    EXPECT_EQ(snap.all().count(), 1);
    EXPECT_EQ(snap2->all().count(), 11);
    Log::debug("snap2 content:");
    print_range(snap2->all());
    for (int i = 0; i < 10000; i++) {
        ss->insert(i, to_string(i));
    }
    ss->gc_run();
    EXPECT_EQ(ss->gc_size(), (size_t) 10011);
    EXPECT_EQ(snap.gc_size(), (size_t) 10011);
    EXPECT_EQ(snap2->gc_size(), (size_t) 10011);
    delete ss;
    snap.gc_run();
    EXPECT_EQ(snap.gc_size(), (size_t) 11);
    EXPECT_EQ(snap2->gc_size(), (size_t) 11);
    Log::debug("snap2 content:");
    print_range(snap2->all());
    EXPECT_EQ(snap2->all().count(), 11);
    delete snap2;
    snap.gc_run();
    EXPECT_EQ(snap.gc_size(), 1u);
    EXPECT_EQ(snap.all().count(), 1);
}

TEST(snapshot, benchmark) {
    multimap<int, string> baseline;
    snapshot_sortedmap<int, string> ssmap;
    Timer timer;
    Log::debug("insert 10000 elements into multimap");
    timer.start();
    for (int i = 0; i < 1000000; i++) {
        insert_into_map(baseline, i, to_string(i));
    }
    timer.stop();
    Log::debug("op/s = %d", int(1000000 / timer.elapsed()));
    Log::debug("insert 10000 elements into snapshot_sortedmap");
    timer.start();
    for (int i = 0; i < 1000000; i++) {
        insert_into_map(ssmap, i, to_string(i));
    }
    timer.stop();
    Log::debug("op/s = %d", int(1000000 / timer.elapsed()));
    Log::debug("create 400000 snapshots of 1000000 element snapshot_sortedmap");
    timer.start();
    for (int i = 0; i < 400000; i++) {
        auto snapshot = ssmap.snapshot();
    }
    timer.stop();
    Log::debug("op/s = %d", int(400000 / timer.elapsed()));
}

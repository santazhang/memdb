#include <sstream>

#include "memdb/locking.h"
#include "base/all.h"

using namespace base;
using namespace mdb;
using namespace std;

static void debug_print(const RWLock& lock) {
    ostringstream ostr;
    if (lock.is_wlocked()) {
        ostr << "wlocked by: " << lock.wlock_owner();
    }
    if (lock.is_rlocked()) {
        ostr << "rlocked by:";
        for (auto& it : lock.rlock_owner()) {
            ostr << " " << it;
        }
    }
    if (ostr.str() == "") {
        ostr << "not locked";
    }
    Log::debug("%s", ostr.str().c_str());
}

TEST(locking, basic_op) {
    RWLock lock;
    debug_print(lock);
    EXPECT_FALSE(lock.is_rlocked());
    EXPECT_FALSE(lock.is_wlocked());
    EXPECT_TRUE(lock.wlock_by(1001));
    EXPECT_TRUE(lock.wlock_by(1001));
    debug_print(lock);
    EXPECT_FALSE(lock.wlock_by(1002));
    EXPECT_TRUE(lock.rlock_by(1001));
    EXPECT_TRUE(lock.unlock_by(1001));
    EXPECT_FALSE(lock.unlock_by(1001));
    EXPECT_FALSE(lock.unlock_by(1002));
    EXPECT_TRUE(lock.wlock_by(1002));
    EXPECT_TRUE(lock.unlock_by(1002));
    debug_print(lock);
    EXPECT_FALSE(lock.is_rlocked());
    EXPECT_FALSE(lock.is_wlocked());
}

TEST(locking, double_locking) {
    RWLock lock;
    EXPECT_TRUE(lock.rlock_by(1001));
    EXPECT_TRUE(lock.rlock_by(1001));
    EXPECT_TRUE(lock.is_rlocked());
    EXPECT_FALSE(lock.is_wlocked());
    EXPECT_TRUE(lock.unlock_by(1001));
    EXPECT_FALSE(lock.is_rlocked());
    EXPECT_FALSE(lock.is_wlocked());
    EXPECT_TRUE(lock.wlock_by(1001));
    EXPECT_TRUE(lock.wlock_by(1001));
    EXPECT_FALSE(lock.is_rlocked());
    EXPECT_TRUE(lock.is_wlocked());
}

TEST(locking, lock_upgrade) {
    RWLock lock;
    EXPECT_TRUE(lock.rlock_by(1001));
    EXPECT_TRUE(lock.wlock_by(1001));
    debug_print(lock);
    EXPECT_FALSE(lock.is_rlocked());
    EXPECT_TRUE(lock.is_wlocked());
}

TEST(locking, cannot_lock_upgrade) {
    RWLock lock;
    EXPECT_TRUE(lock.rlock_by(1001));
    EXPECT_TRUE(lock.rlock_by(1002));
    EXPECT_FALSE(lock.wlock_by(1001));
    debug_print(lock);
    EXPECT_TRUE(lock.is_rlocked());
    EXPECT_FALSE(lock.is_wlocked());
}

TEST(locking, writer_block_reader) {
    RWLock lock;
    EXPECT_TRUE(lock.wlock_by(1001));
    EXPECT_FALSE(lock.rlock_by(1002));
    EXPECT_FALSE(lock.is_rlocked());
    EXPECT_TRUE(lock.is_wlocked());
    EXPECT_TRUE(lock.unlock_by(1001));
    EXPECT_TRUE(lock.rlock_by(1002));
    EXPECT_TRUE(lock.is_rlocked());
    EXPECT_FALSE(lock.is_wlocked());
}

TEST(locking, reader_block_writer) {
    RWLock lock;
    EXPECT_TRUE(lock.rlock_by(1001));
    EXPECT_FALSE(lock.wlock_by(1002));
    EXPECT_FALSE(lock.is_wlocked());
    EXPECT_TRUE(lock.is_rlocked());
    EXPECT_TRUE(lock.unlock_by(1001));
    EXPECT_TRUE(lock.wlock_by(1002));
    EXPECT_TRUE(lock.is_wlocked());
    EXPECT_FALSE(lock.is_rlocked());
}

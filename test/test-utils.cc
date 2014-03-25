#include <iostream>
#include <vector>
#include <list>
#include <set>
#include <map>

#include "memdb/utils.h"
#include "base/all.h"

using namespace base;
using namespace mdb;
using namespace std;

TEST(utils, stringhash32) {
    EXPECT_EQ(stringhash32("hello"), stringhash32("hello"));
    EXPECT_NEQ(stringhash32("hello"), stringhash32("world"));
    Log::debug("stringhash32('hello') = %u", stringhash32("hello"));
}

TEST(utils, stringhash64) {
    EXPECT_EQ(stringhash64("hello"), stringhash64("hello"));
    EXPECT_NEQ(stringhash64("hello"), stringhash64("world"));
    Log::debug("stringhash64('hello') = %llu", stringhash64("hello"));
}

TEST(utils, inthash32) {
    EXPECT_EQ(inthash32(1987, 1001), inthash32(1987, 1001));
    EXPECT_NEQ(inthash32(1987, 1989), inthash32(1987, 1001));
    Log::debug("inthash32(1987, 1001) = %u", inthash32(1987, 1001));
}

TEST(utils, inthash64) {
    EXPECT_EQ(inthash64(1987, 1001), inthash64(1987, 1001));
    EXPECT_NEQ(inthash64(1987, 1989), inthash64(1987, 1001));
    Log::debug("inthash64(1987, 1001) = %llu", inthash64(1987, 1001));
}

template<class T>
const T& get_value(const T& v, std::true_type) {
    return v;
}

template<class T>
const typename T::second_type& get_value(const T& v, std::false_type) {
    return v.second;
}

template <class Container>
static void print_container_values(const Container& container) {
    for (auto it = container.begin(); it != container.end(); ++it) {
        cout << "value in container: " << get_value(*it, std::is_same<int, typename Container::value_type>()) << endl;
    }
}

TEST(utils, value_enumerator) {
    print_container_values(set<int>({1, 2, 3, 4}));
    print_container_values(vector<int>({1, 2, 3, 4}));
    map<int, int> m;
    m[1] = 2;
    m[3] = 4;
    print_container_values(m);
}

#include <unistd.h>

#include "MurmurHash3.h"

#include "utils.h"

namespace mdb {

uint32_t stringhash32(const void* data, int len) {
    uint32_t hash_value;
    static int seed = getpid();
    MurmurHash3_x86_32(data, len, seed, &hash_value);
    return hash_value;
}

uint64_t stringhash64(const void* data, int len) {
    uint64_t hash_value[2];
    static int seed = getpid();
    MurmurHash3_x64_128(data, len, seed, hash_value);
    return hash_value[0] ^ hash_value[1];
}

// https://gist.github.com/badboy/6267743
/*
static inline uint32_t mix32_func1(uint32_t key) {
    key = ~key + (key << 15); // key = (key << 15) - key - 1;
    key = key ^ (key >> 12);
    key = key + (key << 2);
    key = key ^ (key >> 4);
    key = key * 2057; // key = (key + (key << 3)) + (key << 11);
    key = key ^ (key >> 16);
    return key;
}

static inline uint32_t mix32_func2(uint32_t a) {
    a = (a+0x7ed55d16) + (a<<12);
    a = (a^0xc761c23c) ^ (a>>19);
    a = (a+0x165667b1) + (a<<5);
    a = (a+0xd3a2646c) ^ (a<<9);
    a = (a+0xfd7046c5) + (a<<3);
    a = (a^0xb55a4f09) ^ (a>>16);
    return a;
}
*/

static inline uint32_t mix32_func3(uint32_t key) {
    uint32_t c2 = 0x27d4eb2d; // a prime or an odd constant
    key = (key ^ 61) ^ (key >> 16);
    key = key + (key << 3);
    key = key ^ (key >> 4);
    key = key * c2;
    key = key ^ (key >> 15);
    return key;
}

// http://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key
/*
static inline uint32_t mix32_func4(uint32_t k) {
    k *= 357913941;
    k ^= k << 24;
    k += ~357913941;
    k ^= k >> 31;
    k ^= k << 31;
    return k;
}

static inline uint32_t mix32_func5(uint32_t x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x);
    return x;
}
*/

static inline uint64_t mix64(uint64_t key) {
    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return key;
}

uint32_t inthash32(uint32_t* key, int key_length) {
    uint32_t h = 0;
    for (int i = 0; i < key_length; i++) {
        h ^= mix32_func3(key[i]);
    }
    return h;
}

uint64_t inthash64(uint64_t* key, int key_length) {
    uint64_t h = 0;
    for (int i = 0; i < key_length; i++) {
        h ^= mix64(key[i]);
    }
    return h;
}

} // namespace mdb

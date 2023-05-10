#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "pebblesdb/db.h"
#include "pebblesdb/options.h"
#include "pebblesdb/slice.h"
#include "util/lock.h"

#define IDX_PERSISTENT

using SequenceNumber = uint64_t;

#define force_inline __attribute__((always_inline)) inline
#define NORETURN __attribute__((noreturn))
// #define barrier() asm volatile("" ::: "memory")
static force_inline void compiler_barrier() { asm volatile("" ::: "memory"); }

typedef uint64_t ua_uint64_t __attribute__((aligned(1)));
typedef uint32_t ua_uint32_t __attribute__((aligned(1)));
typedef uint16_t ua_uint16_t __attribute__((aligned(1)));

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define CACHE_LINE_SIZE 64
#define FLUSH_ALIGN ((uintptr_t)CACHE_LINE_SIZE)
#define NVM_BLOCK_SIZE 256

namespace leveldb {
// for getting primary key
struct PKeyItem {
  SequenceNumber seq;
  std::string pkey;

  PKeyItem() {}
  PKeyItem(SequenceNumber s, const Slice& p)
      : seq(s), pkey(p.data(), p.size()) {}
};

struct alignas(64) SGetShare {
  std::atomic<bool> stop_flag{false};
  std::atomic<bool> end_flag{false};
  std::atomic<int> g_epoch{0};
  std::atomic<int> head{0};
  std::atomic<int> tail{0};
  std::vector<PKeyItem> pkey_vec;
  // Slice skey;
  std::vector<int> th_id;
  DB* pri_db;
  SpinLock spin_lock;
};

struct alignas(64) SGetLocal {
  ReadOptions options;
  std::vector<KeyValuePair>* value_list;
  std::atomic<bool> finish_flag{false};
};

struct alignas(64) SGetLocalNaive {
  ReadOptions options;
  std::atomic<int> head{0};
  std::atomic<bool> finish_flag{false};
  std::vector<KeyValuePair>* value_list;
  std::vector<PKeyItem> pkey_vec;
};

}  // namespace leveldb

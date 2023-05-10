#pragma once

#include "db/sec_idx/pmem_arena.h"
#include "db/sec_idx/seq_cnt.h"
#include <cmath>
#include <cstring>
#include <iostream>
#include <pthread.h>
#include <vector>

namespace CCEH_NAMESPACE {

typedef uint64_t Key_t;
typedef uint64_t Value_t;

const Key_t SENTINEL = -2; // 11111...110
const Key_t INVALID = -1;  // 11111...111

const Value_t NONE = 0x0;

struct Pair {
  Key_t key;
  Value_t value;

  Pair(void) : key{INVALID} {}

  Pair(Key_t _key, Value_t _value) : key{_key}, value{_value} {}

  Pair &operator=(const Pair &other) {
    key = other.key;
    value = other.value;
    return *this;
  }

  void *operator new(size_t size) = delete;

  void *operator new[](size_t size) = delete;
};


#define f_seed 0xc70697UL
#define s_seed 0xc70697UL

// CAS
#define CAS(_p, _u, _v)                                            \
  __atomic_compare_exchange_n(_p, _u, _v, false, __ATOMIC_ACQ_REL, \
                              __ATOMIC_ACQUIRE)

constexpr size_t kSegmentBits = 8;
constexpr size_t kMask = (1 << kSegmentBits) - 1;
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentSize = (1 << kSegmentBits) * 16 * 4;
constexpr size_t kNumPairPerCacheLine = 4;
constexpr size_t kNumCacheLine = 8;


class Directory;
class CCEH {
 public:
  CCEH(std::string pool);
  CCEH(std::string pool, size_t initCap);
  ~CCEH(void);
  void Prefetch(const Key_t &);
  bool Insert(const Key_t &, const Value_t &);
  // bool InsertOnly(const Key_t &, Value_t);
  bool Delete(const Key_t &);
  Value_t Get(const Key_t &);
  bool SetSeqCnt(const Key_t &, const Value_t &);
  bool CheckSeqCnt(const Key_t &, const Value_t &);
  Value_t FindAnyway(const Key_t &);
  double Utilization(void);
  size_t Capacity(void);
  bool Recovery(void);

//  private:
  Directory *dir;
  // static ::PMemAllocator *index_allocator_;
  PMemAllocator *index_allocator_;
};


struct alignas(64) Segment {
  static const size_t kNumSlot = kSegmentSize / sizeof(Pair);

  Segment(void) : local_depth{0} {}

  Segment(size_t depth) : local_depth{depth} {}

  ~Segment(void) {
    is_deleted = true;
  }

  bool suspend(void) {
    int64_t val;
    do {
      val = sema;
      if (val < 0)
        return false;
    } while (!CAS(&sema, &val, -1));

    int64_t wait = 0 - val - 1;
    while (val && sema != wait) {
      asm("nop");
    }
    return true;
  }

  bool lock(void) {
    int64_t val = sema;
    while (val > -1) {
      if (CAS(&sema, &val, val + 1))
        return true;
      val = sema;
    }
    return false;
  }

  void unlock(void) {
    int64_t val = sema;
    while (!CAS(&sema, &val, val - 1)) {
      val = sema;
    }
  }

  // void *operator new(size_t size) {
  //   // return malloc(size);
  //   return CCEH::index_allocator_->Alloc(size);
  // }

  // void *operator new[](size_t size) {
  //   // return malloc(size);
  //   return CCEH::index_allocator_->Alloc(size);
  // }

  // void operator delete(void *ptr) {
  //   // free(ptr);
  //   CCEH::index_allocator_->Free(ptr);
  // }
  // void operator delete[](void *ptr) {
  //   // free(ptr);
  //   CCEH::index_allocator_->Free(ptr);
  // }

  int Insert(Key_t &, Value_t, size_t, size_t);
  bool Insert4split(Key_t &, Value_t, size_t);
  bool Put(Key_t &, Value_t, size_t);
  Segment **Split(PMemAllocator *allocator);
  size_t numElem(void);

  Pair _[kNumSlot];
  int64_t sema = 0;
  size_t local_depth;
  bool is_deleted = false;
};

struct alignas(64) Directory {
  static const size_t kDefaultDepth = 10;
  Segment **_;
  int64_t sema = 0;
  size_t capacity;
  size_t depth;

  bool suspend(void) {
    int64_t val;
    do {
      val = sema;
      if (val < 0)
        return false;
    } while (!CAS(&sema, &val, -1));

    int64_t wait = 0 - val - 1;
    while (val && sema != wait) {
      asm("nop");
    }
    return true;
  }

  bool lock(void) {
    int64_t val = sema;
    while (val > -1) {
      if (CAS(&sema, &val, val + 1))
        return true;
      val = sema;
    }
    return false;
  }

  void unlock(void) {
    int64_t val = sema;
    while (!CAS(&sema, &val, val - 1)) {
      val = sema;
    }
  }

  Directory(void) {
    depth = kDefaultDepth;
    capacity = pow(2, depth);
    _ = new Segment *[capacity];
    memset(_, 0, sizeof(Segment *) * capacity);
    // _ = (Segment **)AllocSegment(capacity);
    sema = 0;
  }

  Directory(size_t _depth) {
    depth = _depth;
    capacity = pow(2, depth);
    _ = new Segment *[capacity];
    memset(_, 0, sizeof(Segment *) * capacity);
    // _ = (Segment **)AllocSegment(capacity);
    sema = 0;
  }

  ~Directory(void) {
    delete[] _;
  }

  // void *operator new(size_t size) {
  //   return malloc(size);
  //   // return CCEH::index_allocator_->Alloc(size);
  // }

  // void operator delete(void *ptr) {
  //   // delete
  // }

  void SanityCheck(void *);
  void LSBUpdate(int, int, int, int, Segment **);
};


} // namespace CCEH_NAMESPACE

#include "CCEH.h"
#include "db/sec_idx/persist.h"
#include "hash.h"
#include <bitset>
#include <cassert>
#include <cmath>
#include <iostream>
#include <sys/types.h>
#include <thread>
#include <unordered_map>


#define INPLACE

namespace CCEH_NAMESPACE {

bool Segment::Insert4split(Key_t &key, Value_t value, size_t loc) {
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (loc + i) % kNumSlot;
    if (_[slot].key == INVALID) {
      _[slot].key = key;
      _[slot].value = value;
      return true;
    }
  }
  return false;
}

Segment **Segment::Split(PMemAllocator *allocator) {
#ifdef INPLACE
  Segment **split = new Segment *[2];
  split[0] = this;
  split[1] = new (allocator->Alloc(sizeof(Segment))) Segment(local_depth + 1);

  auto pattern = ((size_t)1 << (sizeof(Key_t) * 8 - local_depth - 1));
  for (unsigned i = 0; i < kNumSlot; ++i) {
    if (_[i].key != INVALID) {
      auto f_hash = hash_funcs[0](&_[i].key, sizeof(Key_t), f_seed);
      if (f_hash & pattern) {
        if (!split[1]->Insert4split(_[i].key, _[i].value,
                                    (f_hash & kMask) * kNumPairPerCacheLine)) {
          // auto s_hash = hash_funcs[2](&_[i].key, sizeof(Key_t), s_seed);
          // if (!split[1]->Insert4split(_[i].key, _[i].value,
          //                             (s_hash & kMask) *
          //                             kNumPairPerCacheLine)) {
          std::cerr
              << "[" << __func__
              << "]: something wrong -- need to adjust probing distance, key "
              << _[i].key << std::endl;
          // }
        }
      }
    }
  }

  idx_clwb_fence((char *)split[1], sizeof(Segment));

  return split;
#else
  Segment **split = new Segment *[2];
  split[0] = new (allocator->Alloc(sizeof(Segment))) Segment(local_depth + 1);
  split[1] = new (allocator->Alloc(sizeof(Segment))) Segment(local_depth + 1);

  auto pattern = ((size_t)1 << (sizeof(Key_t) * 8 - local_depth - 1));
  for (unsigned i = 0; i < kNumSlot; ++i) {
    auto f_hash = hash_funcs[0](&_[i].key, sizeof(Key_t), f_seed);
    if (f_hash & pattern) {
      if (!split[1]->Insert4split(_[i].key, _[i].value,
                                  (f_hash & kMask) * kNumPairPerCacheLine)) {
        // auto s_hash = hash_funcs[2](&_[i].key, sizeof(Key_t), s_seed);
        // if (!split[1]->Insert4split(_[i].key, _[i].value,
        //                             (s_hash & kMask) * kNumPairPerCacheLine)) {
          std::cerr << "[" << __func__
               << "]: something wrong -- need to adjust probing distance"
               << std::endl;
        // }
      }
    } else {
      if (!split[0]->Insert4split(_[i].key, _[i].value,
                                  (f_hash & kMask) * kNumPairPerCacheLine)) {
        // auto s_hash = hash_funcs[2](&_[i].key, sizeof(Key_t), s_seed);
        // if (!split[0]->Insert4split(_[i].key, _[i].value,
        //                             (s_hash & kMask) * kNumPairPerCacheLine)) {
          std::cerr << "[" << __func__
               << "]: something wrong -- need to adjust probing distance"
               << std::endl;
        // }
      }
    }
  }

  idx_clflush((char *)split[0], sizeof(Segment));
  idx_clflush((char *)split[1], sizeof(Segment));

  return split;
#endif
}

CCEH::CCEH(std::string pool) : dir{new Directory(2)} {
  index_allocator_ = new MMAPAllocator(pool, 4ul << 30);
  for (unsigned i = 0; i < dir->capacity; ++i) {
    void *p = index_allocator_->Alloc(sizeof(Segment));
    dir->_[i] = new (p) Segment(dir->depth);
  }
	idx_clwb_fence((char *)dir->_, sizeof(Segment *) * dir->capacity);
}

CCEH::CCEH(std::string pool, size_t initCap)
    : dir{new Directory(static_cast<size_t>(log2(initCap)))} {
  index_allocator_ = new MMAPAllocator(pool, 4ul << 30);
  for (unsigned i = 0; i < dir->capacity; ++i) {
    void *p = index_allocator_->Alloc(sizeof(Segment));
    dir->_[i] = new (p) Segment(static_cast<size_t>(log2(initCap)));
  }
	idx_clwb_fence((char *)dir->_, sizeof(Segment *) * dir->capacity);
}

CCEH::~CCEH(void) {
  Segment *prev = nullptr;
  for (unsigned i = 0; i < dir->capacity; ++i) {
    if (dir->_[i] != prev) {
      prev = dir->_[i];
      // delete dir->_[i];
      index_allocator_->Free(prev, sizeof(Segment));
    }
  }
  delete dir;
}

bool CCEH::Insert(const Key_t &key, const Value_t &value) {
  auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
  auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;

RETRY:
  auto x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
  auto target = dir->_[x];

  if (!target) {
    std::this_thread::yield();
    goto RETRY;
  }

  /* acquire segment exclusive lock */
  if (!target->lock()) {
    std::this_thread::yield();
    goto RETRY;
  }

  auto target_check = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
  if (target != dir->_[target_check]) {
    target->unlock();
    std::this_thread::yield();
    goto RETRY;
  }

  // try update
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto loc = (f_idx + i) % Segment::kNumSlot;
    if (target->_[loc].key == key) {
      Pair *entry = &target->_[loc];
      entry->value = value;
      idx_clwb_fence(entry, sizeof(Pair));
      target->unlock();
      return false;
    }
  }

  // insert hash 1
  auto target_local_depth = target->local_depth;
  auto pattern = (f_hash >> (8 * sizeof(f_hash) - target->local_depth));
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto loc = (f_idx + i) % Segment::kNumSlot;
    auto _key = target->_[loc].key;
    if ((((hash_funcs[0](&_key, sizeof(Key_t), f_seed) >>
           (8 * sizeof(f_hash) - target_local_depth)) != pattern) ||
         (target->_[loc].key == INVALID)) &&
        (target->_[loc].key != SENTINEL)) {
      if (CAS(&target->_[loc].key, &_key, SENTINEL)) {
        target->_[loc].value = value;
        target->_[loc].key = key;
        idx_clwb_fence((char *)&target->_[loc], sizeof(Pair));
        /* release segment exclusive lock */
        target->unlock();
        return true;
      }
    }
  }

  // // insert hash 2
  // auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
  // auto s_idx = (s_hash & kMask) * kNumPairPerCacheLine;
  // for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
  //   auto loc = (s_idx + i) % Segment::kNumSlot;
  //   auto _key = target->_[loc].key;
  //   /* validity check for entry keys */
  //   if ((((hash_funcs[0](&_key, sizeof(Key_t), f_seed) >>
  //          (8 * sizeof(f_hash) - target_local_depth)) != pattern) ||
  //        (target->_[loc].key == INVALID)) &&
  //       (target->_[loc].key != SENTINEL)) {
  //     if (CAS(&target->_[loc].key, &_key, SENTINEL)) {
  //       Shortcut shortcut(reinterpret_cast<char *>(target), i);
  //       le_helper.shortcut = shortcut;
  //       target->_[loc].value = value;
  //       sfence();
  //       target->_[loc].key = key;
  //       idx_clwb_fence((char *)&target->_[loc], sizeof(Pair));
  //       /* release segment exclusive lock */
  //       target->unlock();
  //       return;
  //     }
  //   }
  // }

  // COLLISION!!
  /* need to split segment but release the exclusive lock first to avoid
   * deadlock */
  target->unlock();

  if (!target->suspend()) {
    std::this_thread::yield();
    goto RETRY;
  }

  /* need to check whether the target segment has been split */
#ifdef INPLACE
  if (target_local_depth != target->local_depth) {
    target->sema = 0;
    std::this_thread::yield();
    goto RETRY;
  }
#else
  if (target_local_depth != dir->_[x]->local_depth) {
    target->sema = 0;
    std::this_thread::yield();
    goto RETRY;
  }
#endif

  Segment **s = target->Split(this->index_allocator_);

DIR_RETRY:
  /* need to double the directory */
  if (target_local_depth == dir->depth) {
    if (!dir->suspend()) {
      std::this_thread::yield();
      goto DIR_RETRY;
    }

    x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    auto dir_old = dir;
    auto d = dir->_;
    auto _dir = new Directory(dir->depth + 1);
    for (unsigned i = 0; i < dir->capacity; ++i) {
      if (i == x) {
        _dir->_[2 * i] = s[0];
        _dir->_[2 * i + 1] = s[1];
      } else {
        _dir->_[2 * i] = d[i];
        _dir->_[2 * i + 1] = d[i];
      }
    }
    idx_clwb_fence((char *)_dir->_, sizeof(Segment *) * _dir->capacity);
    idx_clwb_fence((char *)_dir, sizeof(Directory));
    dir = _dir;
    idx_clwb_fence((char *)&dir, sizeof(void *));
#ifdef INPLACE
    s[0]->local_depth++;
    idx_clwb_fence((char *)&s[0]->local_depth, sizeof(size_t));
    /* release segment exclusive lock */
    s[0]->sema = 0;
#endif

    /* TBD */
    delete dir_old;
  } else { // normal segment split
    while (!dir->lock()) {
      asm("nop");
    }

    x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    if (dir->depth == target_local_depth + 1) {
      if (x % 2 == 0) {
        dir->_[x + 1] = s[1];
#ifdef INPLACE
        idx_clwb_fence((char *)&dir->_[x + 1], 8);
#else
        dir->_[x] = s[0];
        idx_clwb_fence((char *)&dir->_[x], 16);
#endif
      } else {
        dir->_[x] = s[1];
#ifdef INPLACE
        idx_clwb_fence((char *)&dir->_[x], 8);
#else
        dir->_[x - 1] = s[0];
        idx_clwb_fence((char *)&dir->_[x - 1], 16);
#endif
      }
      dir->unlock();
#ifdef INPLACE
      s[0]->local_depth++;
      idx_clwb_fence((char *)&s[0]->local_depth, sizeof(size_t));
      /* release target segment exclusive lock */
      s[0]->sema = 0;
#endif
    } else {
      int stride = pow(2, dir->depth - target_local_depth);
      auto loc = x - (x % stride);
      for (int i = 0; i < stride / 2; ++i) {
        dir->_[loc + stride / 2 + i] = s[1];
      }
#ifdef INPLACE
      idx_clwb_fence((char *)&dir->_[loc + stride / 2], sizeof(void *) * stride / 2);
#else
      for (int i = 0; i < stride / 2; ++i) {
        dir->_[loc + i] = s[0];
      }
      idx_clwb_fence((char *)&dir->_[loc], sizeof(void *) * stride);
#endif
      dir->unlock();
#ifdef INPLACE
      s[0]->local_depth++;
      idx_clwb_fence((char *)&s[0]->local_depth, sizeof(size_t));
      /* release target segment exclusive lock */
      s[0]->sema = 0;
#endif
    }
  }
  delete[] s;
  std::this_thread::yield();
  goto RETRY;
}

bool CCEH::SetSeqCnt(const Key_t &key, const Value_t &val) {
  auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
  auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;

RETRY:
  auto x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
  auto target = dir->_[x];

  if (!target) {
    std::this_thread::yield();
    goto RETRY;
  }

  /* acquire segment exclusive lock */
  if (!target->lock()) {
    std::this_thread::yield();
    goto RETRY;
  }

  auto target_check = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
  if (target != dir->_[target_check]) {
    target->unlock();
    std::this_thread::yield();
    goto RETRY;
  }

  // try update
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto loc = (f_idx + i) % Segment::kNumSlot;
    if (target->_[loc].key == key) {
      Pair *entry = &target->_[loc];
      SeqCnt entry_val;
      entry_val._data = entry->value;
      entry_val.seq_ = val;
      entry_val.cnt_++;
      entry->value = entry_val._data;
      idx_clwb_fence(entry, sizeof(Pair));
      target->unlock();
      return false;
    }
  }

  // insert hash 1
  auto target_local_depth = target->local_depth;
  auto pattern = (f_hash >> (8 * sizeof(f_hash) - target->local_depth));
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto loc = (f_idx + i) % Segment::kNumSlot;
    auto _key = target->_[loc].key;
    if ((((hash_funcs[0](&_key, sizeof(Key_t), f_seed) >>
           (8 * sizeof(f_hash) - target_local_depth)) != pattern) ||
         (target->_[loc].key == INVALID)) &&
        (target->_[loc].key != SENTINEL)) {
      if (CAS(&target->_[loc].key, &_key, SENTINEL)) {
        SeqCnt entry_val(val, 1);
        target->_[loc].value = entry_val._data;
        target->_[loc].key = key;
        /* release segment exclusive lock */
        target->unlock();
        return true;
      }
    }
  }


  // COLLISION!!
  /* need to split segment but release the exclusive lock first to avoid
   * deadlock */
  target->unlock();

  if (!target->suspend()) {
    std::this_thread::yield();
    goto RETRY;
  }

  /* need to check whether the target segment has been split */
#ifdef INPLACE
  if (target_local_depth != target->local_depth) {
    target->sema = 0;
    std::this_thread::yield();
    goto RETRY;
  }
#else
  if (target_local_depth != dir->_[x]->local_depth) {
    target->sema = 0;
    std::this_thread::yield();
    goto RETRY;
  }
#endif

  Segment **s = target->Split(this->index_allocator_);

DIR_RETRY:
  /* need to double the directory */
  if (target_local_depth == dir->depth) {
    if (!dir->suspend()) {
      std::this_thread::yield();
      goto DIR_RETRY;
    }

    x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    auto dir_old = dir;
    auto d = dir->_;
    auto _dir = new Directory(dir->depth + 1);
    for (unsigned i = 0; i < dir->capacity; ++i) {
      if (i == x) {
        _dir->_[2 * i] = s[0];
        _dir->_[2 * i + 1] = s[1];
      } else {
        _dir->_[2 * i] = d[i];
        _dir->_[2 * i + 1] = d[i];
      }
    }
    
    dir = _dir;
    
#ifdef INPLACE
    s[0]->local_depth++;
    /* release segment exclusive lock */
    s[0]->sema = 0;
#endif

    /* TBD */
    delete dir_old;
  } else { // normal segment split
    while (!dir->lock()) {
      asm("nop");
    }

    x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
    if (dir->depth == target_local_depth + 1) {
      if (x % 2 == 0) {
        dir->_[x + 1] = s[1];
#ifdef INPLACE
#else
        dir->_[x] = s[0];
#endif
      } else {
        dir->_[x] = s[1];
#ifdef INPLACE
#else
        dir->_[x - 1] = s[0];
#endif
      }
      dir->unlock();
#ifdef INPLACE
      s[0]->local_depth++;
      /* release target segment exclusive lock */
      s[0]->sema = 0;
#endif
    } else {
      int stride = pow(2, dir->depth - target_local_depth);
      auto loc = x - (x % stride);
      for (int i = 0; i < stride / 2; ++i) {
        dir->_[loc + stride / 2 + i] = s[1];
      }
#ifdef INPLACE
#else
      for (int i = 0; i < stride / 2; ++i) {
        dir->_[loc + i] = s[0];
      }
#endif
      dir->unlock();
#ifdef INPLACE
      s[0]->local_depth++;
      /* release target segment exclusive lock */
      s[0]->sema = 0;
#endif
    }
  }
  delete[] s;
  std::this_thread::yield();
  goto RETRY;
}

bool CCEH::CheckSeqCnt(const Key_t &key, const Value_t &val) {
  auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
  auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;

RETRY:
  // while (dir->sema < 0) {
  //   asm("nop");
  // }

  auto x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
  auto target = dir->_[x];

  if (!target) {
    std::this_thread::yield();
    goto RETRY;
  }

#ifdef INPLACE
  /* acquire segment shared lock */
  // if (!target->lock()) {
  //   std::this_thread::yield();
  //   goto RETRY;
  // }
#endif

  auto target_check = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
  if (target != dir->_[target_check]) {
#ifdef INPLACE
    // target->unlock();
#endif
    std::this_thread::yield();
    goto RETRY;
  }

    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto loc = (f_idx + i) % Segment::kNumSlot;
    if (target->_[loc].key == key) {
      SeqCnt entry_val;
      entry_val._data = target->_[loc].value;
      bool equal = entry_val.seq_ == val;
      if (!equal) {
        entry_val.cnt_--;
        if (entry_val.cnt_ == 0) {
          // delete this entry
          target->_[loc].key = INVALID;
        } else {
          target->_[loc].value = entry_val._data;
        }
      }
#ifdef INPLACE
      /* key found, relese segment shared lock */
      // target->unlock();
#endif
      return equal;
    }
  }

#ifdef INPLACE
  /* key not found, release segment shared lock */
  // target->unlock();
#endif
  return true;
}


// TODO
bool CCEH::Delete(const Key_t &key) { return false; }

void CCEH::Prefetch(const Key_t &key) {
  auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);

  auto x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
  __builtin_prefetch(&(dir->_[x]->sema));
}

Value_t CCEH::Get(const Key_t &key) {
  auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
  auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;

RETRY:
  while (dir->sema < 0) {
    asm("nop");
  }

  auto x = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
  auto target = dir->_[x];

  if (!target) {
    std::this_thread::yield();
    goto RETRY;
  }

#ifdef INPLACE
  /* acquire segment shared lock */
  if (!target->lock()) {
    std::this_thread::yield();
    goto RETRY;
  }
#endif

  auto target_check = (f_hash >> (8 * sizeof(f_hash) - dir->depth));
  if (target != dir->_[target_check]) {
#ifdef INPLACE
    target->unlock();
#endif
    std::this_thread::yield();
    goto RETRY;
  }

  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto loc = (f_idx + i) % Segment::kNumSlot;
    if (target->_[loc].key == key) {
      Value_t v = target->_[loc].value;
#ifdef INPLACE
      /* key found, relese segment shared lock */
      target->unlock();
#endif
      return v;
    }
  }

//   auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
//   auto s_idx = (s_hash & kMask) * kNumPairPerCacheLine;

//   for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
//     auto loc = (s_idx + i) % Segment::kNumSlot;
//     if (target->_[loc].key == key) {
//       Value_t v = target->_[loc].value;
// #ifdef INPLACE
//       /* key found, relese segment shared lock */
//       target->unlock();
// #endif
//       return v;
//     }
//   }

#ifdef INPLACE
  /* key not found, release segment shared lock */
  target->unlock();
#endif
  return NONE;
}

double CCEH::Utilization(void) {
  size_t sum = 0;
  size_t cnt = 0;
  for (size_t i = 0; i < dir->capacity; cnt++) {
    auto target = dir->_[i];
    auto stride = pow(2, dir->depth - target->local_depth);
    auto pattern = (i >> (dir->depth - target->local_depth));
    for (unsigned j = 0; j < Segment::kNumSlot; ++j) {
      auto key_hash = hash_funcs[0](&target->_[j].key, sizeof(Key_t), f_seed);
      if (((key_hash >> (8 * sizeof(key_hash) - target->local_depth)) ==
           pattern) &&
          (target->_[j].key != INVALID)) {
        sum++;
      }
    }
    i += stride;
  }
  return ((double)sum) / ((double)cnt * Segment::kNumSlot) * 100.0;
}

size_t CCEH::Capacity(void) {
  size_t cnt = 0;
  for (int i = 0; i < dir->capacity; cnt++) {
    auto target = dir->_[i];
    auto stride = pow(2, dir->depth - target->local_depth);
    i += stride;
  }
  return cnt * Segment::kNumSlot;
}

size_t Segment::numElem(void) {
  size_t sum = 0;
  for (unsigned i = 0; i < kNumSlot; ++i) {
    if (_[i].key != INVALID) {
      sum++;
    }
  }
  return sum;
}

bool CCEH::Recovery(void) {
  bool recovered = false;
  size_t i = 0;
  while (i < dir->capacity) {
    size_t depth_cur = dir->_[i]->local_depth;
    size_t stride = pow(2, dir->depth - depth_cur);
    size_t buddy = i + stride;
    if (buddy == dir->capacity)
      break;
    for (int j = buddy - 1; i < j; j--) {
      if (dir->_[j]->local_depth != depth_cur) {
        dir->_[j] = dir->_[i];
      }
    }
    i = i + stride;
  }
  if (recovered) {
    idx_clwb_fence((char *)&dir->_[0], sizeof(void *) * dir->capacity);
  }
  return recovered;
}

// for debugging
Value_t CCEH::FindAnyway(const Key_t &key) {
  using namespace std;
  for (size_t i = 0; i < dir->capacity; ++i) {
    for (size_t j = 0; j < Segment::kNumSlot; ++j) {
      if (dir->_[i]->_[j].key == key) {
        cout << "segment(" << i << ")" << endl;
        cout << "global_depth(" << dir->depth << "), local_depth("
             << dir->_[i]->local_depth << ")" << endl;
        cout << "pattern: "
             << bitset<sizeof(int64_t)>(i >>
                                        (dir->depth - dir->_[i]->local_depth))
             << endl;
        cout << "Key MSB: "
             << bitset<sizeof(int64_t)>(
                    h(&key, sizeof(key)) >>
                    (8 * sizeof(key) - dir->_[i]->local_depth))
             << endl;
        return dir->_[i]->_[j].value;
      }
    }
  }
  return NONE;
}

} // namespace CCEH_NAMESPACE

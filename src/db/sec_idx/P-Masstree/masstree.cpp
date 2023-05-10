#include "masstree.h"
#include "Epoche.cpp"

#include "db/sec_idx/murmur_hash2.h"

#include <set>

using namespace MASS;

namespace masstree {

// static constexpr uint64_t CACHE_LINE_SIZE = 64;

static inline void fence() { asm volatile("" : : : "memory"); }

static inline void mfence() { asm volatile("sfence" ::: "memory"); }

static inline void clflush(const void *data, int len, bool front, bool back) {
  volatile char *ptr = (char *)((unsigned long)data & ~(CACHE_LINE_SIZE - 1));
  // if (front) mfence();
  for (; ptr < (char *)data + len; ptr += CACHE_LINE_SIZE) {
// #ifdef CLFLUSH
//     asm volatile("clflush %0" : "+m"(*(volatile char *)ptr));
// #elif CLFLUSH_OPT
//     asm volatile(".byte 0x66; clflush %0" : "+m"(*(volatile char *)(ptr)));
// #elif CLWB
//     asm volatile(".byte 0x66; xsaveopt %0" : "+m"(*(volatile char *)(ptr)));
// #endif
    asm volatile(".byte 0x66; xsaveopt %0" : "+m"(*(volatile char *)(ptr)));
  }
  if (back) mfence();
}

static inline void movnt64(uint64_t *dest, uint64_t const &src, bool front,
                           bool back) {
  assert(((uint64_t)dest & 7) == 0);
  if (front) mfence();
  _mm_stream_si64((long long int *)dest, *(long long int *)&src);
  if (back) mfence();
}

static inline void prefetch_(const void *ptr) {
  typedef struct {
    char x[CACHE_LINE_SIZE];
  } cacheline_t;
  asm volatile("prefetcht0 %0" : : "m"(*(const cacheline_t *)ptr));
}

#ifdef LOCK_INIT
static tbb::concurrent_vector<std::mutex *> lock_initializer;
void lock_initialization() {
  printf("lock table size = %lu\n", lock_initializer.size());
  for (uint64_t i = 0; i < lock_initializer.size(); i++) {
    lock_initializer[i]->unlock();
  }
}
#endif

// Attention: for bswap performed, when using memcmp to compare a 9-byte str,
// we should compare all the 16 bytes
// for the last 1 byte is at the end of the second uint64_t.
inline size_t aligned_len(const size_t &x) {
  return (x + sizeof(uint64_t) - 1) / sizeof(uint64_t) * sizeof(uint64_t);
}

int keycmp(const uint64_t a[], const uint64_t b[], size_t key_len) {
  // For memcmp is used both for equal and for "more than"
  // So only use memcmp to compare "num_u64 * sizeof(uint64_t)" bytes is not
  // enough, for it only offers true result of "equal" or "not_equal", not "less
  // than" or "more than". For sign comparison, we should use original uint64_t
  // array to do element-wise comparison.
  size_t num_u64 = (key_len + sizeof(uint64_t) - 1) / sizeof(uint64_t);
  for (size_t i = 0; i < num_u64; i++) {
    if (a[i] != b[i]) {
      return a[i] < b[i] ? -1 : 1;
    }
  }
  return 0;
}

masstree::masstree() {
  pmem_allocator_ = new MMAPAllocator("masstree_pool", 4ul << 30);
  pkey_block_allocator_ = new MMAPAllocator("masstree_pkb_pool", 8ul << 30);
  void *ptr = pmem_allocator_->Alloc(sizeof(leafnode));
  memset(ptr, 0, sizeof(leafnode));
  leafnode *init_root = new (ptr) leafnode(0);
  clflush((char *)init_root, sizeof(leafnode), false, true);
  root_.store(init_root, std::memory_order_release);
  clflush((char *)&root_, sizeof(void *), false, true);
}

// masstree::masstree(void *new_root) {
//   pmem_allocator_ = new MMAPAllocator("masstree_pool", 4ul << 30);
//   pkey_block_allocator_ = new MMAPAllocator("masstree_pkb_pool", 4ul << 30);
//   clflush((char *)new_root, sizeof(leafnode), false, true);
//   root_.store(new_root, std::memory_order_release);
//   clflush((char *)&root_, sizeof(void *), false, true);
// }

ThreadInfo masstree::getThreadInfo() { return ThreadInfo(this->epoche); }
ThreadInfo *masstree::getpThreadInfo() { return new ThreadInfo(this->epoche); }

leafnode::leafnode(uint32_t level) : permutation(permuter::make_empty()) {
  level_ = level;
  next.store(NULL, std::memory_order_release);
  leftmost_ptr = NULL;
}

leafnode::leafnode(void *left, uint64_t key, void *right, uint32_t level = 1)
    : permutation(permuter::make_empty()) {
  level_ = level;
  next.store(NULL, std::memory_order_release);
  leftmost_ptr = reinterpret_cast<leafnode *>(left);
  entry[0].key = key;
  entry[0].value = right;

  permutation = permuter::make_sorted(1);
}

// void *leafnode::operator new(size_t size) {
//   void *ptr;
//   int ret = posix_memalign(&ptr, CACHE_LINE_SIZE, size);
//   if (ret != 0) {
//     printf("%s Allocation error by posix_memalign\n", __func__);
//     exit(ret);
//   }
//   memset(ptr, 0, size);
//   return ptr;
// }

// void leafnode::operator delete(void *addr) { free(addr); }

bool leafnode::isLocked(uint64_t version) const {
  return ((version & 0b10) == 0b10);
}

void leafnode::writeLock() {
  uint64_t version;
  version = typeVersionLockObsolete.load();
  int needRestart = UNLOCKED;
  upgradeToWriteLockOrRestart(version, needRestart);
}

void leafnode::writeLockOrRestart(int &needRestart) {
  uint64_t version;
  needRestart = UNLOCKED;
  version = readLockOrRestart(needRestart);
  if (needRestart) return;

  upgradeToWriteLockOrRestart(version, needRestart);
}

bool leafnode::tryLock(int &needRestart) {
  uint64_t version;
  needRestart = UNLOCKED;
  version = readLockOrRestart(needRestart);
  if (needRestart) return false;

  upgradeToWriteLockOrRestart(version, needRestart);
  if (needRestart) return false;

  return true;
}

void leafnode::upgradeToWriteLockOrRestart(uint64_t &version,
                                           int &needRestart) {
  if (typeVersionLockObsolete.compare_exchange_strong(version,
                                                      version + 0b10)) {
    version = version + 0b10;
  } else {
    needRestart = LOCKED;
  }
}

void leafnode::writeUnlock(bool isOverWrite) {
  if (isOverWrite)
    typeVersionLockObsolete.fetch_add(0b10);
  else
    typeVersionLockObsolete.fetch_sub(0b10);
}

uint64_t leafnode::readLockOrRestart(int &needRestart) const {
  uint64_t version;
  version = typeVersionLockObsolete.load();
  needRestart = UNLOCKED;

  if (isLocked(version))
    needRestart = LOCKED;
  else if (isObsolete(version))
    needRestart = OBSOLETE;

  return version;
}

bool leafnode::isObsolete(uint64_t version) { return (version & 1) == 1; }

void leafnode::checkOrRestart(uint64_t startRead, int &needRestart) const {
  readUnlockOrRestart(startRead, needRestart);
}

void leafnode::readUnlockOrRestart(uint64_t startRead, int &needRestart) const {
  needRestart = (int)(startRead != typeVersionLockObsolete.load());
}

int leafnode::compare_key(const uint64_t a, const uint64_t b) {
  if (a == b)
    return 0;
  else
    return a < b ? -1 : 1;
}

key_indexed_position leafnode::key_lower_bound_by(uint64_t key) {
  permuter perm = permutation;
  int l = 0, r = perm.size();
  while (l < r) {
    int m = (l + r) >> 1;
    int mp = perm[m];
    int cmp = compare_key(key, entry[mp].key);
    if (cmp < 0)
      r = m;
    else if (cmp == 0)
      return key_indexed_position(m, mp);
    else
      l = m + 1;
  }
  return key_indexed_position(l, -1);
}

key_indexed_position leafnode::key_lower_bound(uint64_t key) {
  permuter perm = permutation;
  int l = 0, r = perm.size();
  while (l < r) {
    int m = (l + r) >> 1;
    int mp = perm[m];
    int cmp = compare_key(key, entry[mp].key);
    if (cmp < 0)
      r = m;
    else if (cmp == 0)
      return key_indexed_position(m, mp);
    else
      l = m + 1;
  }

  return (l - 1 < 0 ? key_indexed_position(l - 1, -1)
                    : key_indexed_position(l - 1, perm[l - 1]));
}

leafnode *leafnode::advance_to_key(const uint64_t &key) {
  const leafnode *n = this;

  leafnode *snapshot_n;
  if ((snapshot_n = n->next.load(std::memory_order_acquire)) &&
      compare_key(key, snapshot_n->highest) >= 0) {
    n = snapshot_n;
  }

  return const_cast<leafnode *>(n);
}

permuter leafnode::permute() { return permutation; }

void leafnode::prefetch() const {
  for (int i = 64; i < std::min((16 * LEAF_WIDTH) + 1, 4 * 64); i += 64)
    prefetch_((const char *)this + i);
}

leafvalue *masstree::make_leaf(const char *key, size_t key_len,
                               uint64_t value) {
  void *aligned_alloc;
  size_t len = (key_len % sizeof(uint64_t)) == 0
                   ? key_len
                   : (((key_len) / sizeof(uint64_t)) + 1) * sizeof(uint64_t);

  if (value == 0) {
    int ret = posix_memalign(&aligned_alloc, CACHE_LINE_SIZE,
                             sizeof(leafvalue) + len + sizeof(uint64_t));
    if (ret != 0) {
      printf("%s Allocation error by posix_memalign\n", __func__);
      exit(ret);
    }
  } else {
    aligned_alloc =
        pmem_allocator_->Alloc(sizeof(leafvalue) + len + sizeof(uint64_t));
  }

  leafvalue *lv = reinterpret_cast<leafvalue *>(aligned_alloc);
  memset(lv, 0, sizeof(leafvalue) + len + sizeof(uint64_t));

  lv->value = value;
  lv->key_len = key_len;  // key_len or len??
  memcpy(lv->fkey, key, key_len);

  for (uint64_t i = 0; i < (len / sizeof(uint64_t)); i++)
    lv->fkey[i] = __builtin_bswap64(lv->fkey[i]);

  if (value != 0)
    clflush((char *)lv, sizeof(leafvalue) + len + sizeof(uint64_t), false,
            true);
  return lv;
}

leafvalue *leafnode::smallest_leaf(size_t key_len, uint64_t value) {
  void *aligned_alloc;
  size_t len = (key_len % sizeof(uint64_t)) == 0
                   ? key_len
                   : (((key_len) / sizeof(uint64_t)) + 1) * sizeof(uint64_t);

  int ret =
      posix_memalign(&aligned_alloc, CACHE_LINE_SIZE, sizeof(leafvalue) + len);
  if (ret != 0) {
    printf("%s Allocation error by posix_memalign\n", __func__);
    exit(ret);
  }

  leafvalue *lv = reinterpret_cast<leafvalue *>(aligned_alloc);
  memset(lv, 0, sizeof(leafvalue) + len);

  lv->value = value;
  lv->key_len = key_len;  // key_len or len??

  for (uint64_t i = 0; i < (len / sizeof(uint64_t)); i++) lv->fkey[i] = 0ULL;

  if (value != 0) clflush((char *)lv, sizeof(leafvalue) + len, false, true);
  return lv;
}

void leafnode::make_new_layer(masstree *t, leafnode *l,
                              key_indexed_position &kx_, leafvalue *olv,
                              leafvalue *nlv, uint32_t depth) {
  int kcmp = compare_key(olv->fkey[depth], nlv->fkey[depth]);

  leafnode *twig_head = l;
  leafnode *twig_tail = l;
  while (kcmp == 0) {
    void *ptr = t->pmem_allocator_->Alloc(sizeof(leafnode));
    memset(ptr, 0, sizeof(leafnode));
    leafnode *nl = new (ptr) leafnode(0);
    nl->assign_initialize_for_layer(0, olv->fkey[depth]);
    if (twig_head != l)
      twig_tail->entry[0].value = nl;
    else
      twig_head = nl;
    nl->permutation = permuter::make_sorted(1);
    twig_tail = nl;
    depth++;
    kcmp = compare_key(olv->fkey[depth], nlv->fkey[depth]);
  }

  void *ptr = t->pmem_allocator_->Alloc(sizeof(leafnode));
  memset(ptr, 0, sizeof(leafnode));
  leafnode *nl = new (ptr) leafnode(0);
  nl->assign_initialize(0, kcmp < 0 ? olv->fkey[depth] : nlv->fkey[depth],
                        kcmp < 0 ? SET_LV(olv) : SET_LV(nlv));
  nl->assign_initialize(1, kcmp < 0 ? nlv->fkey[depth] : olv->fkey[depth],
                        kcmp < 0 ? SET_LV(nlv) : SET_LV(olv));

  nl->permutation = permuter::make_sorted(2);

  fence();
  if (twig_tail != l) twig_tail->entry[0].value = nl;
  twig_tail = nl;
  if (twig_head != l) {
    leafnode *iter = twig_head;
    mfence();
    for (; iter != twig_tail && iter != NULL;
         iter = reinterpret_cast<leafnode *>(iter->entry[0].value)) {
      clflush((char *)iter, sizeof(leafnode), false, false);
    }
    clflush((char *)twig_tail, sizeof(leafnode), false, false);
    mfence();

    l->entry[kx_.p].value = twig_head;
    clflush((char *)l->entry_addr(kx_.p) + 8, sizeof(uintptr_t), false, true);
  } else {
    clflush((char *)nl, sizeof(leafnode), false, true);

    l->entry[kx_.p].value = nl;
    clflush((char *)l->entry_addr(kx_.p) + 8, sizeof(uintptr_t), false, true);
  }
}

void leafnode::check_for_recovery(masstree *t, leafnode *left, leafnode *right,
                                  void *root, uint32_t depth, leafvalue *lv) {
  permuter perm = left->permute();

  for (int i = perm.size() - 1; i >= 0; i--) {
    if (left->key(perm[i]) >= right->highest) {
      perm.remove_to_back(i);
    } else {
      break;
    }
  }

  if (left->permutation.size() != perm.size()) {
    left->permutation = perm.value();
    clflush((char *)&left->permutation, sizeof(permuter), false, true);
  }

  if (depth > 0) {
    key_indexed_position pkx_;
    leafnode *p = correct_layer_root(root, lv, depth, pkx_);
    if (p->value(pkx_.p) == left) {
      void *ptr = t->pmem_allocator_->Alloc(sizeof(leafnode));
      memset(ptr, 0, sizeof(leafnode));
      leafnode *new_root =
          new (ptr) leafnode(left, right->highest, right, left->level() + 1);
      clflush((char *)new_root, sizeof(leafnode), false, true);
      p->entry[pkx_.p].value = new_root;
      clflush((char *)&p->entry[pkx_.p].value, sizeof(uintptr_t), false, true);
      p->writeUnlock(false);

      right->writeUnlock(false);
      left->writeUnlock(false);
    } else {
      root = p;
      t->split(p->entry[pkx_.p].value, root, depth, lv, right->highest, right,
               left->level() + 1, left, false);
    }
  } else {
    if (t->root() == left) {
      void *ptr = t->pmem_allocator_->Alloc(sizeof(leafnode));
      memset(ptr, 0, sizeof(leafnode));
      leafnode *new_root =
          new (ptr) leafnode(left, right->highest, right, left->level() + 1);
      clflush((char *)new_root, sizeof(leafnode), false, true);
      t->setNewRoot(new_root);

      right->writeUnlock(false);
      left->writeUnlock(false);
    } else {
      t->split(NULL, NULL, 0, NULL, right->highest, right, left->level() + 1,
               left, false);
    }
  }
}

void masstree::put(uint64_t key, void *value, ThreadInfo &threadEpocheInfo) {
  EpocheGuard epocheGuard(threadEpocheInfo);
  key_indexed_position kx_;
  leafnode *next = NULL, *p = NULL;
  void *snapshot_v = NULL;

  int needRestart;
  uint64_t v;

from_root:
  p = reinterpret_cast<leafnode *>(this->root_.load(std::memory_order_acquire));
  while (p->level() != 0) {
  inter_retry:
    next = p->advance_to_key(key);
    if (next != p) {
      // check for recovery
      if (p->tryLock(needRestart)) {
        if (next->tryLock(needRestart))
          p->check_for_recovery(this, p, next, NULL, 0, NULL);
        else
          p->writeUnlock(false);
      }
      p = next;
      goto inter_retry;
    }

    v = p->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto inter_retry;
      else
        goto from_root;
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(key);

    if (kx_.i >= 0)
      snapshot_v = p->value(kx_.p);
    else
      snapshot_v = p->leftmost();

    p->checkOrRestart(v, needRestart);
    if (needRestart)
      goto inter_retry;
    else
      p = reinterpret_cast<leafnode *>(snapshot_v);
  }

  leafnode *l = reinterpret_cast<leafnode *>(p);
leaf_retry:
  next = l->advance_to_key(key);
  if (next != l) {
    // check for recovery
    if (l->tryLock(needRestart)) {
      if (next->tryLock(needRestart))
        l->check_for_recovery(this, l, next, NULL, 0, NULL);
      else
        l->writeUnlock(false);
    }

    l = next;
    goto leaf_retry;
  }

  l->writeLockOrRestart(needRestart);
  if (needRestart) {
    if (needRestart == LOCKED)
      goto leaf_retry;
    else  // needRestart == OBSOLETE
      goto from_root;
  }

  next = l->advance_to_key(key);
  if (next != l) {
    l->writeUnlock(false);
    l = next;
    goto leaf_retry;
  }

  l->prefetch();
  fence();

  kx_ = l->key_lower_bound_by(key);
  if (kx_.p >= 0 && l->key(kx_.p) == key) {
    l->assign_value(kx_.p, value);
    l->writeUnlock(false);
  } else {
    if (!(l->leaf_insert(this, NULL, 0, NULL, key, value, kx_))) {
      put(key, value, threadEpocheInfo);
    }
  }
}

// void masstree::put(char *key, uint64_t value, ThreadInfo &threadEpocheInfo) {
//   EpocheGuard epocheGuard(threadEpocheInfo);
//   void *root = NULL;
//   key_indexed_position kx_;
//   uint32_t depth = 0;
//   leafnode *next = NULL, *p = NULL;
//   leafvalue *lv = make_leaf(key, strlen(key), value);
//   void *snapshot_v = NULL;

//   int needRestart;
//   uint64_t v;

// restart:
//   root = this->root_.load(std::memory_order_acquire);
//   depth = 0;
//   p = reinterpret_cast<leafnode *>(root);

// from_root:
//   while (p->level() != 0) {
//   inter_retry:
//     next = p->advance_to_key(lv->fkey[depth]);
//     if (next != p) {
//       // check for recovery
//       if (p->tryLock(needRestart)) {
//         if (next->tryLock(needRestart))
//           p->check_for_recovery(this, p, next, root, depth, lv);
//         else
//           p->writeUnlock(false);
//       }
//       p = next;
//       goto inter_retry;
//     }

//     v = p->readLockOrRestart(needRestart);
//     if (needRestart) {
//       if (needRestart == LOCKED)
//         goto inter_retry;
//       else
//         goto restart;
//     }

//     p->prefetch();
//     fence();

//     kx_ = p->key_lower_bound(lv->fkey[depth]);

//     if (kx_.i >= 0)
//       snapshot_v = p->value(kx_.p);
//     else
//       snapshot_v = p->leftmost();

//     p->checkOrRestart(v, needRestart);
//     if (needRestart)
//       goto inter_retry;
//     else
//       p = reinterpret_cast<leafnode *>(snapshot_v);
//   }

//   leafnode *l = reinterpret_cast<leafnode *>(p);
// leaf_retry:
//   next = l->advance_to_key(lv->fkey[depth]);
//   if (next != l) {
//     // check for recovery
//     if (l->tryLock(needRestart)) {
//       if (next->tryLock(needRestart))
//         l->check_for_recovery(this, l, next, root, depth, lv);
//       else
//         l->writeUnlock(false);
//     }
//     l = next;
//     goto leaf_retry;
//   }

//   l->writeLockOrRestart(needRestart);
//   if (needRestart) {
//     if (needRestart == LOCKED)
//       goto leaf_retry;
//     else  // needRestart == OBSOLETE
//       goto restart;
//   }

//   next = l->advance_to_key(lv->fkey[depth]);
//   if (next != l) {
//     l->writeUnlock(false);
//     l = next;
//     goto leaf_retry;
//   }

//   l->prefetch();
//   fence();

//   kx_ = l->key_lower_bound_by(lv->fkey[depth]);
//   if (kx_.p >= 0) {
//     // i)   If there is additional layer, retry B+tree traversing from the next
//     // layer
//     if (!IS_LV(l->value(kx_.p))) {
//       p = reinterpret_cast<leafnode *>(l->value(kx_.p));
//       root = l;
//       depth++;
//       l->writeUnlock(false);
//       goto from_root;
//       // ii)  Atomically update value for the matching key
//     } else if (IS_LV(l->value(kx_.p)) &&
//                (LV_PTR(l->value(kx_.p)))->key_len == lv->key_len &&
//                memcmp(lv->fkey, (LV_PTR(l->value(kx_.p)))->fkey,
//                       aligned_len(lv->key_len)) == 0) {
//       (LV_PTR(l->value(kx_.p)))->value = value;
//       clflush((char *)&(LV_PTR(l->value(kx_.p)))->value, sizeof(void *), false,
//               true);
//       l->writeUnlock(false);
//       // iii) Allocate additional layers (B+tree's roots) up to
//       //      the number of common prefixes (8bytes unit).
//       //      Insert two keys to the leafnode in the last layer
//       //      During these processes, this leafnode must be locked
//     } else {
//       l->make_new_layer(this, l, kx_, LV_PTR(l->value(kx_.p)), lv, ++depth);
//       l->writeUnlock(false);
//     }
//   } else {
//     if (!(l->leaf_insert(this, root, depth, lv, lv->fkey[depth], SET_LV(lv),
//                          kx_))) {
//       put(key, value, threadEpocheInfo);
//     }
//   }
// }

void masstree::put(const Slice &key, const Slice &value, SequenceNumber seq,
                   ThreadInfo &threadEpocheInfo, clht *ht) {
  EpocheGuard epocheGuard(threadEpocheInfo);
  void *root = NULL;
  key_indexed_position kx_;
  uint32_t depth = 0;
  leafnode *next = NULL, *p = NULL;
  leafvalue *tmp_lv = make_leaf(key.data(), key.size(), 0);
  void *snapshot_v = NULL;

  int needRestart;
  uint64_t v;

restart:
  root = this->root_.load(std::memory_order_acquire);
  depth = 0;
  p = reinterpret_cast<leafnode *>(root);

from_root:
  while (p->level() != 0) {
  inter_retry:
    next = p->advance_to_key(tmp_lv->fkey[depth]);
    if (next != p) {
      // check for recovery
      if (p->tryLock(needRestart)) {
        if (next->tryLock(needRestart))
          p->check_for_recovery(this, p, next, root, depth, tmp_lv);
        else
          p->writeUnlock(false);
      }
      p = next;
      goto inter_retry;
    }

    v = p->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto inter_retry;
      else
        goto restart;
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(tmp_lv->fkey[depth]);

    if (kx_.i >= 0)
      snapshot_v = p->value(kx_.p);
    else
      snapshot_v = p->leftmost();

    p->checkOrRestart(v, needRestart);
    if (needRestart)
      goto inter_retry;
    else
      p = reinterpret_cast<leafnode *>(snapshot_v);
  }

  leafnode *l = reinterpret_cast<leafnode *>(p);
leaf_retry:
  next = l->advance_to_key(tmp_lv->fkey[depth]);
  if (next != l) {
    // check for recovery
    if (l->tryLock(needRestart)) {
      if (next->tryLock(needRestart))
        l->check_for_recovery(this, l, next, root, depth, tmp_lv);
      else
        l->writeUnlock(false);
    }
    l = next;
    goto leaf_retry;
  }

  l->writeLockOrRestart(needRestart);
  if (needRestart) {
    if (needRestart == LOCKED)
      goto leaf_retry;
    else  // needRestart == OBSOLETE
      goto restart;
  }

  next = l->advance_to_key(tmp_lv->fkey[depth]);
  if (next != l) {
    l->writeUnlock(false);
    l = next;
    goto leaf_retry;
  }

  l->prefetch();
  fence();

  kx_ = l->key_lower_bound_by(tmp_lv->fkey[depth]);
  if (kx_.p >= 0) {
    leafvalue *cur_lv = LV_PTR(l->value(kx_.p));
    // i)   If there is additional layer, retry B+tree traversing from the next
    // layer
    if (!IS_LV(l->value(kx_.p))) {
      p = reinterpret_cast<leafnode *>(l->value(kx_.p));
      root = l;
      depth++;
      l->writeUnlock(false);
      goto from_root;
      // ii)  Atomically update value for the matching key
    } else if (IS_LV(l->value(kx_.p)) && cur_lv->key_len == tmp_lv->key_len &&
               memcmp(tmp_lv->fkey, cur_lv->fkey,
                      aligned_len(tmp_lv->key_len)) == 0) {
      l->leaf_update_pkey(this, kx_, key, value, seq, ht);
      l->writeUnlock(false);
      // iii) Allocate additional layers (B+tree's roots) up to
      //      the number of common prefixes (8bytes unit).
      //      Insert two keys to the leafnode in the last layer
      //      During these processes, this leafnode must be locked
    } else {
      // new layer
      // assert(key.size() > 8);  // should not happen for 8B key
      char *prev = (char *)cur_lv->value;
      PKeyBlock *old_pb = get_pkey_block_addr(prev);
      int min_slot, max_slot;
      l->get_slot_range_in_same_pkey_block(kx_.i, &min_slot, &max_slot);
      if (min_slot < kx_.i) {
        l->split_to_new_pkey_block(this, min_slot, kx_.i - 1, ht);
      }
      if (max_slot > kx_.i) {
        l->split_to_new_pkey_block(this, kx_.i + 1, max_slot, ht);
      }

      l->split_to_new_pkey_block(this, kx_.i, kx_.i, ht);
      PKeyBlock *new_pb = get_pkey_block_addr((char *)cur_lv->value);

      char *new_val = new_pb->Append(value, seq, nullptr);
      if (new_val == nullptr) {
        PKeyBlock *ano_new_pb =
            (PKeyBlock *)pkey_block_allocator_->Alloc(PKEY_BLOCK_SIZE);
        ano_new_pb->init();
        new_val = ano_new_pb->Append(value, seq, nullptr);
      }
      leafvalue *lv = make_leaf(key.data(), key.size(), (uint64_t)new_val);

      l->make_new_layer(this, l, kx_, cur_lv, lv, ++depth);
      l->writeUnlock(false);
      pkey_block_allocator_->Free(old_pb, PKEY_BLOCK_SIZE);
    }
  } else {
    // not found, insert
    if (!(l->leaf_insert_pkey(this, root, depth, tmp_lv->fkey[depth], key,
                              value, seq, kx_, ht))) {
      put(key, value, seq, threadEpocheInfo, ht);
    }
  }
  free(tmp_lv);
}

void masstree::del(uint64_t key, ThreadInfo &threadEpocheInfo) {
  EpocheGuard epocheGuard(threadEpocheInfo);
  void *root = NULL;
  key_indexed_position kx_;
  leafnode *next = NULL;
  void *snapshot_v = NULL;

  int needRestart;
  uint64_t v;

restart:
  root = this->root_.load(std::memory_order_acquire);
  leafnode *p = reinterpret_cast<leafnode *>(root);
  while (p->level() != 0) {
  inter_retry:
    next = p->advance_to_key(key);
    if (next != p) {
      // check for recovery
      if (p->tryLock(needRestart)) {
        if (next->tryLock(needRestart)) {
          p->check_for_recovery(this, p, next, NULL, 0, NULL);
        } else
          p->writeUnlock(false);
      }
      p = next;
      goto inter_retry;
    }

    v = p->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto inter_retry;
      else
        goto restart;
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(key);

    if (kx_.i >= 0)
      snapshot_v = p->value(kx_.p);
    else
      snapshot_v = p->leftmost();

    p->checkOrRestart(v, needRestart);
    if (needRestart)
      goto inter_retry;
    else
      p = reinterpret_cast<leafnode *>(snapshot_v);
  }

  leafnode *l = reinterpret_cast<leafnode *>(p);
leaf_retry:
  next = l->advance_to_key(key);
  if (next != l) {
    // check for recovery
    if (l->tryLock(needRestart)) {
      if (next->tryLock(needRestart)) {
        l->check_for_recovery(this, l, next, NULL, 0, NULL);
      } else
        l->writeUnlock(false);
    }

    l = next;
    goto leaf_retry;
  }

  l->writeLockOrRestart(needRestart);
  if (needRestart) {
    if (needRestart == LOCKED)
      goto leaf_retry;
    else
      goto restart;
  }

  next = l->advance_to_key(key);
  if (next != l) {
    l->writeUnlock(false);
    l = next;
    goto leaf_retry;
  }

  l->prefetch();
  fence();

  kx_ = l->key_lower_bound_by(key);
  if (kx_.p < 0) {
    l->writeUnlock(false);
    return;
  }

  if (!(l->leaf_delete(this, NULL, 0, NULL, kx_, threadEpocheInfo))) {
    del(key, threadEpocheInfo);
  }
}

void masstree::del(char *key, ThreadInfo &threadEpocheInfo) {
  EpocheGuard epocheGuard(threadEpocheInfo);
  void *root = NULL;
  key_indexed_position kx_;
  uint32_t depth = 0;
  leafnode *next = NULL;
  void *snapshot_v = NULL;

  leafvalue *lv = make_leaf(key, strlen(key), 0);

  int needRestart;
  uint64_t v;

restart:
  depth = 0;
  root = this->root_.load(std::memory_order_acquire);
  leafnode *p = reinterpret_cast<leafnode *>(root);
from_root:
  while (p->level() != 0) {
  inter_retry:
    next = p->advance_to_key(lv->fkey[depth]);
    if (next != p) {
      // check for recovery
      if (p->tryLock(needRestart)) {
        if (next->tryLock(needRestart))
          p->check_for_recovery(this, p, next, root, depth, lv);
        else
          p->writeUnlock(false);
      }
      p = next;
      goto inter_retry;
    }

    v = p->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto inter_retry;
      else
        goto restart;
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(lv->fkey[depth]);

    if (kx_.i >= 0)
      snapshot_v = p->value(kx_.p);
    else
      snapshot_v = p->leftmost();

    p->checkOrRestart(v, needRestart);
    if (needRestart)
      goto inter_retry;
    else
      p = reinterpret_cast<leafnode *>(snapshot_v);
  }

  leafnode *l = reinterpret_cast<leafnode *>(p);
leaf_retry:
  next = l->advance_to_key(lv->fkey[depth]);
  if (next != l) {
    // check for recovery
    if (l->tryLock(needRestart)) {
      if (next->tryLock(needRestart))
        l->check_for_recovery(this, l, next, root, depth, lv);
      else
        l->writeUnlock(false);
    }

    l = next;
    goto leaf_retry;
  }

  l->writeLockOrRestart(needRestart);
  if (needRestart) {
    if (needRestart == LOCKED)
      goto leaf_retry;
    else
      goto restart;
  }

  next = l->advance_to_key(lv->fkey[depth]);
  if (next != l) {
    l->writeUnlock(false);
    l = next;
    goto leaf_retry;
  }

  l->prefetch();
  fence();

  kx_ = l->key_lower_bound_by(lv->fkey[depth]);
  if (kx_.p >= 0) {
    // i)   If there is additional layer, retry B+tree traversing from the next
    // layer
    if (!IS_LV(l->value(kx_.p))) {
      p = reinterpret_cast<leafnode *>(l->value(kx_.p));
      root = l;
      depth++;
      l->writeUnlock(false);
      goto from_root;
      // ii)  Checking false-positive result and starting to delete it
    } else if (IS_LV(l->value(kx_.p)) &&
               (LV_PTR(l->value(kx_.p)))->key_len == lv->key_len &&
               memcmp(lv->fkey, (LV_PTR(l->value(kx_.p)))->fkey,
                      aligned_len(lv->key_len)) == 0) {
      if (!(l->leaf_delete(this, root, depth, lv, kx_, threadEpocheInfo))) {
        free(lv);
        del(key, threadEpocheInfo);
      }
    } else {
      l->writeUnlock(false);
      free(lv);
      return;
    }
  } else {
    l->writeUnlock(false);
    free(lv);
    return;
  }
}

inline void leafnode::assign_initialize(int p, const uint64_t &key,
                                        void *value) {
  entry[p].key = key;
  entry[p].value = value;
}

inline void leafnode::assign_initialize(int p, leafnode *x, int xp) {
  entry[p].key = x->entry[xp].key;
  entry[p].value = x->entry[xp].value;
}

inline void leafnode::assign_initialize_for_layer(int p, const uint64_t &key) {
  entry[p].key = key;
}

int leafnode::split_into(leafnode *nr, int p, const uint64_t &key, void *value,
                         uint64_t &split_key) {
  int width = this->permutation.size();
  int mid = width / 2 + 1;

  permuter perml = this->permutation;
  permuter pv = perml.value_from(mid - (p < mid));
  for (int x = mid; x <= width; ++x) {
    if (x == p)
      nr->assign_initialize(x - mid, key, value);
    else {
      nr->assign_initialize(x - mid, this, pv & 15);
      pv >>= 4;
    }
  }

  permuter permr = permuter::make_sorted(width + 1 - mid);
  if (p >= mid) permr.remove_to_back(p - mid);
  nr->permutation = permr.value();

  // leafnode::link_split(this, nr);
  nr->highest = nr->entry[0].key;
  nr->next.store(this->next.load(std::memory_order_acquire),
                 std::memory_order_release);
  clflush((char *)nr, sizeof(leafnode), false, true);
  this->next.store(nr, std::memory_order_release);
  clflush((char *)(&this->next), sizeof(uintptr_t), false, true);

  split_key = nr->highest;
  return p >= mid ? 1 + (mid == LEAF_WIDTH) : 0;
}

// split and insert key
int leafnode::split_into_pkey(masstree *t, leafnode *nr, int p,
                              const uint64_t &fkey, const Slice &key,
                              const Slice &value, SequenceNumber seq,
                              uint64_t &split_key, clht *ht) {
  int width = this->permutation.size();
  int mid = width / 2 + 1;

  permuter perml = this->permutation;
  int split_pos = mid - (p < mid);
  permuter pv = perml.value_from(split_pos);
  // if (p < mid), start from [mid - 1] (7); else start from mid (8)
  if (IS_LV(this->value(perml[split_pos]))) {
    int min_slot, max_slot;
    get_slot_range_in_same_pkey_block(split_pos, &min_slot, &max_slot);
    if (min_slot < split_pos) {
      PKeyBlock *old_pb = get_pkey_block_addr(
          (char *)(LV_PTR(this->value(perml[split_pos])))->value);
      split_to_new_pkey_block(t, min_slot, split_pos - 1, ht);
      split_to_new_pkey_block(t, split_pos, max_slot, ht);
      t->pkey_block_allocator_->Free(old_pb, PKEY_BLOCK_SIZE);
    } else {
      // already separated
    }
  }

  void *p_val = nullptr;
  if (p >= mid) {
    // new value
    PKeyBlock *pb = nullptr;
    int prev_slot = p;
    if (p > mid && IS_LV(this->value(perml[p - 1]))) {
      prev_slot = p - 1;
      pb = get_pkey_block_addr(
          (char *)(LV_PTR(this->value(perml[p - 1])))->value);
    } else if (p < width && IS_LV(this->value(perml[p]))) {
      pb = get_pkey_block_addr((char *)(LV_PTR(this->value(perml[p])))->value);
    } else {
      pb = (PKeyBlock *)t->pkey_block_allocator_->Alloc(PKEY_BLOCK_SIZE);
      pb->init();
    }

    char *new_val = pb->Append(value, seq, nullptr);
    if (new_val == nullptr) {
      int min_slot, max_slot;
      get_slot_range_in_same_pkey_block(prev_slot, &min_slot, &max_slot);
      bool free_cur_pb = false;
      new_val = insert_and_split_to_new_pkey_block(
          t, p, min_slot, max_slot, false, pb, free_cur_pb, value, seq, ht);
      if (free_cur_pb) {
        t->pkey_block_allocator_->Free(pb, PKEY_BLOCK_SIZE);
      }
    }

    leafvalue *new_lv =
        t->make_leaf(key.data(), key.size(), (uint64_t)new_val);
    p_val = SET_LV(new_lv);
  }


  for (int x = mid; x <= width; ++x) {
    if (x == p)
      nr->assign_initialize(x - mid, fkey, p_val);
    else {
      nr->assign_initialize(x - mid, this, pv & 15);
      pv >>= 4;
    }
  }

  permuter permr = permuter::make_sorted(width + 1 - mid);
  if (p >= mid) permr.remove_to_back(p - mid);
  nr->permutation = permr.value();

  // leafnode::link_split(this, nr);
  nr->highest = nr->entry[0].key;
  nr->next.store(this->next.load(std::memory_order_acquire),
                 std::memory_order_release);
  clflush((char *)nr, sizeof(leafnode), false, true);
  this->next.store(nr, std::memory_order_release);
  clflush((char *)(&this->next), sizeof(uintptr_t), false, true);

  split_key = nr->highest;
  return p >= mid ? 1 + (mid == LEAF_WIDTH) : 0;
}

void leafnode::split_into_inter(leafnode *nr, uint64_t &split_key) {
  int width = this->permutation.size();
  int mid = width / 2 + 1;

  permuter perml = this->permutation;
  permuter pv = perml.value_from(mid);
  for (int x = mid; x < width; ++x) {
    nr->assign_initialize(x - mid, this, pv & 15);
    pv >>= 4;
  }

  permuter permr = permuter::make_sorted(width - mid);
  nr->permutation = permr.value();

  nr->leftmost_ptr =
      reinterpret_cast<leafnode *>(this->entry[perml[mid - 1]].value);
  nr->highest = this->entry[perml[mid - 1]].key;
  nr->next.store(this->next.load(std::memory_order_acquire),
                 std::memory_order_release);
  clflush((char *)nr, sizeof(leafnode), false, true);
  this->next.store(nr, std::memory_order_release);
  clflush((char *)(&this->next), sizeof(uintptr_t), false, true);

  split_key = nr->highest;
}

void leafnode::assign(int p, const uint64_t &key, void *value) {
  entry[p].key = key;
  fence();
  entry[p].value = value;
}

void leafnode::assign_value(int p, void *value) {
  entry[p].value = value;
  clflush((char *)&entry[p].value, sizeof(void *), false, true);
}

void *leafnode::entry_addr(int p) { return &entry[p]; }

void masstree::setNewRoot(void *new_root) {
  this->root_.store(new_root, std::memory_order_release);
  clflush((char *)&this->root_, sizeof(void *), false, true);
}

leafnode *leafnode::correct_layer_root(void *root, leafvalue *lv,
                                       uint32_t depth,
                                       key_indexed_position &pkx_) {
  int needRestart;
  leafnode *oldp;
  leafnode *p = reinterpret_cast<leafnode *>(root);

leaf_retry:
  oldp = p->advance_to_key(lv->fkey[depth - 1]);
  if (oldp != p) {
    p = oldp;
    goto leaf_retry;
  }

  p->writeLockOrRestart(needRestart);
  if (needRestart) goto leaf_retry;

  oldp = p->advance_to_key(lv->fkey[depth - 1]);
  if (oldp != p) {
    p->writeUnlock(false);
    p = oldp;
    goto leaf_retry;
  }

  p->prefetch();
  fence();

  pkx_ = p->key_lower_bound_by(lv->fkey[depth - 1]);
  if (pkx_.p < 0) {
    printf("[correct_layer_root] cannot find layer's root\n");
    printf("key = %lu, depth = %u\n", lv->fkey[depth - 1], depth);
    for (int i = 0; i < p->permutation.size(); i++) {
      printf("key[%d] = %lu\n", i, p->entry[p->permutation[i]].key);
    }
    exit(0);
  }

  root = p;
  return p;
}

leafnode *leafnode::search_for_leftsibling(std::atomic<void *> *aroot,
                                           void **root, uint64_t key,
                                           uint32_t level, leafnode *right) {
  leafnode *p = NULL;
  key_indexed_position kx_;
  leafnode *next = NULL;
  void *snapshot_v = NULL;

  int needRestart;
  uint64_t v;

from_root:
  p = reinterpret_cast<leafnode *>(
      aroot ? aroot->load(std::memory_order_acquire) : *root);
  while (p->level() > level) {
  inter_retry:
    next = p->advance_to_key(key);
    if (next != p) {
      p = next;
      goto inter_retry;
    }

    v = p->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto inter_retry;
      else
        goto from_root;
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(key);

    if (kx_.i >= 0)
      snapshot_v = p->value(kx_.p);
    else
      snapshot_v = p->leftmost();

    p->checkOrRestart(v, needRestart);
    if (needRestart)
      goto inter_retry;
    else
      p = reinterpret_cast<leafnode *>(snapshot_v);
  }

leaf_retry:
  next = p->advance_to_key(key);
  if (next != p) {
    p = next;
    goto leaf_retry;
  }

  if (p->tryLock(needRestart)) {
    next = p->advance_to_key(key);
    if (next != p) {
      p->writeUnlock(false);
      p = next;
      goto leaf_retry;
    }
  } else {
    if (needRestart == OBSOLETE) goto from_root;

    if (p == right) return p;
    goto leaf_retry;
  }

  return p;
}

void *leafnode::leaf_insert(masstree *t, void *root, uint32_t depth,
                            leafvalue *lv, uint64_t key, void *value,
                            key_indexed_position &kx_) {
  bool isOverWrite = false;
  void *ret = NULL;

  // permutation based insert
  if (this->permutation.size() < LEAF_WIDTH) {
    kx_.p = this->permutation.back();
    if (entry[kx_.p].value != NULL) isOverWrite = true;

    this->assign(kx_.p, key, value);
    clflush((char *)(&this->entry[kx_.p]), sizeof(kv), false, true);

    permuter cp = this->permutation.value();
    cp.insert_from_back(kx_.i);
    fence();
    this->permutation = cp.value();
    clflush((char *)(&this->permutation), sizeof(permuter), false, true);

    this->writeUnlock(isOverWrite);
    ret = this;
  } else {
    // overflow
    // leafnode split

    // TODO: Need to add an additional context here checking crash and running
    // recovery mechanism to avoid the duplicate split for the node that was
    // split, but the permutor was not updated to only reflect the half of the
    // entries.
    // * Algorithm sketch:
    // i) compare the high key of right sibling with the highest key of left
    // sibling. ii) If the highest key of left sibling is larger than the high
    // key of right sibling,
    //     1) invalidate the right sibling by atomically changing the next
    //     pointer of left
    //        sibling to the next pointer of right sibling. And, continue to the
    //        split process. (current implementation reflects this way, but will
    //        be changed to second method)
    //     2) replay the original split process from the third step that removes
    //     the half of
    //        the entries from the left sibling. (this would be more reasonable
    //        in terms of reusing the existing split mechanism)
    if (this->next.load(std::memory_order_acquire) != NULL &&
        this->key(this->permutation[this->permutation.size() - 1]) >
            this->next.load(std::memory_order_acquire)->highest) {
      this->next.store(this->next.load(std::memory_order_acquire)
                           ->next.load(std::memory_order_acquire),
                       std::memory_order_release);
      clflush((char *)&this->next, sizeof(leafnode *), false, true);
    }

    void *ptr = t->pmem_allocator_->Alloc(sizeof(leafnode));
    memset(ptr, 0, sizeof(leafnode));
    leafnode *new_sibling = new (ptr) leafnode(this->level_);
    new_sibling->writeLock();
    uint64_t split_key;
    int split_type =
        this->split_into(new_sibling, kx_.i, key, value, split_key);

    leafnode *nl = reinterpret_cast<leafnode *>(this);
    leafnode *nr = reinterpret_cast<leafnode *>(new_sibling);

    permuter perml = nl->permutation;
    int width = perml.size();
    perml.set_size(width - nr->permutation.size());

    if (width != LEAF_WIDTH) perml.exchange(perml.size(), LEAF_WIDTH - 1);

    nl->permutation = perml.value();
    clflush((char *)(&nl->permutation), sizeof(permuter), false, true);

    if (depth > 0) {
      key_indexed_position pkx_;
      leafnode *p = correct_layer_root(root, lv, depth, pkx_);
      if (p->value(pkx_.p) == this) {
        void *ptr = t->pmem_allocator_->Alloc(sizeof(leafnode));
        memset(ptr, 0, sizeof(leafnode));
        leafnode *new_root =
            new (ptr) leafnode(this, split_key, new_sibling, level_ + 1);
        clflush((char *)new_root, sizeof(leafnode), false, true);
        p->entry[pkx_.p].value = new_root;
        clflush((char *)&p->entry[pkx_.p].value, sizeof(uintptr_t), false,
                true);
        p->writeUnlock(false);
      } else {
        root = p;
        t->split(p->entry[pkx_.p].value, root, depth, lv, split_key,
                 new_sibling, level_ + 1, NULL, false);
      }
    } else {
      if (t->root() == this) {
        void *ptr = t->pmem_allocator_->Alloc(sizeof(leafnode));
        memset(ptr, 0, sizeof(leafnode));
        leafnode *new_root =
            new (ptr) leafnode(this, split_key, new_sibling, level_ + 1);
        clflush((char *)new_root, sizeof(leafnode), false, true);
        t->setNewRoot(new_root);
      } else {
        t->split(NULL, NULL, 0, NULL, split_key, new_sibling, level_ + 1, NULL,
                 false);
      }
    }

    // permutation base final insertion
    if (split_type == 0) {
      kx_.p = perml.back();
      if (nl->entry[kx_.p].value != NULL) isOverWrite = true;

      nl->assign(kx_.p, key, value);
      clflush((char *)(&nl->entry[kx_.p]), sizeof(kv), false, true);

      permuter cp = nl->permutation.value();
      cp.insert_from_back(kx_.i);
      fence();
      nl->permutation = cp.value();
      clflush((char *)(&nl->permutation), sizeof(permuter), false, true);
      ret = nl;
    } else {
      kx_.i = kx_.p = kx_.i - perml.size();

      permuter cp = nr->permutation.value();
      cp.insert_from_back(kx_.i);
      fence();
      nr->permutation = cp.value();
      clflush((char *)(&nr->permutation), sizeof(permuter), false, true);
      ret = nr;
    }

    nr->writeUnlock(false);
    nl->writeUnlock(isOverWrite);
  }

  return ret;
}

void *leafnode::leaf_insert_pkey(masstree *t, void *root, uint32_t depth,
                                 uint64_t fkey, const Slice &key,
                                 const Slice &value, SequenceNumber seq,
                                 key_indexed_position &kx_, clht *ht) {
  bool isOverWrite = false;
  void *ret = NULL;

  // permutation based insert
  if (this->permutation.size() < LEAF_WIDTH) {
    permuter cp = this->permutation;

    PKeyBlock *pb;
    int prev_slot = kx_.i;
    if (kx_.i > 0 && IS_LV(this->value(cp[kx_.i - 1]))) {
      prev_slot = kx_.i - 1;
      pb = get_pkey_block_addr(
          (char *)(LV_PTR(this->value(cp[prev_slot])))->value);
    } else if (kx_.i < cp.size() && IS_LV(this->value(cp[kx_.i]))) {
      pb = get_pkey_block_addr((char *)(LV_PTR(this->value(cp[kx_.i])))->value);
    } else {
      pb = (PKeyBlock *)t->pkey_block_allocator_->Alloc(PKEY_BLOCK_SIZE);
      pb->init();
    }

    char *ptr = pb->Append(value, seq, nullptr);
    if (ptr) {
      leafvalue *new_lv =
          t->make_leaf(key.data(), key.size(), (uint64_t)ptr);
      kx_.p = this->permutation.back();
      if (entry[kx_.p].value != NULL) isOverWrite = true;

      this->assign(kx_.p, fkey, SET_LV(new_lv));
      clflush((char *)(&this->entry[kx_.p]), sizeof(kv), false, false);

      permuter cp = this->permutation.value();
      cp.insert_from_back(kx_.i);
      fence();
      this->permutation = cp.value();
      clflush((char *)(&this->permutation), sizeof(permuter), false, true);

      this->writeUnlock(isOverWrite);
      ret = this;
    } else {
      // need split pkey block
      int min_slot, max_slot;
      get_slot_range_in_same_pkey_block(prev_slot, &min_slot, &max_slot);
      bool free_cur_pb = false;
      ptr = insert_and_split_to_new_pkey_block(
          t, kx_.i, min_slot, max_slot, false, pb, free_cur_pb, value, seq, ht);
      // note that new_ptr contains [min_slot, max_slot + 1]
      leafvalue *new_lv = t->make_leaf(key.data(), key.size(), (uint64_t)ptr);
      kx_.p = this->permutation.back();
      if (entry[kx_.p].value != NULL) isOverWrite = true;

      this->assign(kx_.p, fkey, SET_LV(new_lv));
      clflush((char *)(&this->entry[kx_.p]), sizeof(kv), false, true);

      permuter cp = this->permutation.value();
      cp.insert_from_back(kx_.i);
      fence();
      this->permutation = cp.value();
      clflush((char *)(&this->permutation), sizeof(permuter), false, true);

      this->writeUnlock(isOverWrite);
      if (free_cur_pb) {
        t->pkey_block_allocator_->Free(pb, PKEY_BLOCK_SIZE);
      }
      ret = this;
    }
  } else {
    // overflow
    // leafnode split

    // TODO: Need to add an additional context here checking crash and running
    // recovery mechanism to avoid the duplicate split for the node that was
    // split, but the permutor was not updated to only reflect the half of the
    // entries.
    // * Algorithm sketch:
    // i) compare the high key of right sibling with the highest key of left
    // sibling. ii) If the highest key of left sibling is larger than the high
    // key of right sibling,
    //     1) invalidate the right sibling by atomically changing the next
    //     pointer of left
    //        sibling to the next pointer of right sibling. And, continue to the
    //        split process. (current implementation reflects this way, but will
    //        be changed to second method)
    //     2) replay the original split process from the third step that removes
    //     the half of
    //        the entries from the left sibling. (this would be more reasonable
    //        in terms of reusing the existing split mechanism)
    if (this->next.load(std::memory_order_acquire) != NULL &&
        this->key(this->permutation[this->permutation.size() - 1]) >
            this->next.load(std::memory_order_acquire)->highest) {
      this->next.store(this->next.load(std::memory_order_acquire)
                           ->next.load(std::memory_order_acquire),
                       std::memory_order_release);
      clflush((char *)&this->next, sizeof(leafnode *), false, true);
    }

    void *ptr = t->pmem_allocator_->Alloc(sizeof(leafnode));
    memset(ptr, 0, sizeof(leafnode));
    leafnode *new_sibling = new (ptr) leafnode(this->level_);
    new_sibling->writeLock();
    uint64_t split_key;
    int split_type = this->split_into_pkey(t, new_sibling, kx_.i, fkey, key,
                                           value, seq, split_key, ht);

    leafnode *nl = reinterpret_cast<leafnode *>(this);
    leafnode *nr = reinterpret_cast<leafnode *>(new_sibling);

    permuter perml = nl->permutation;
    int width = perml.size();
    perml.set_size(width - nr->permutation.size());

    if (width != LEAF_WIDTH) perml.exchange(perml.size(), LEAF_WIDTH - 1);

    nl->permutation = perml.value();
    clflush((char *)(&nl->permutation), sizeof(permuter), false, true);

    if (depth > 0) {
      key_indexed_position pkx_;
      leafvalue *tmp_lv = t->make_leaf(key.data(), key.size(), 0);
      leafnode *p = correct_layer_root(root, tmp_lv, depth, pkx_);
      if (p->value(pkx_.p) == this) {
        void *ptr = t->pmem_allocator_->Alloc(sizeof(leafnode));
        memset(ptr, 0, sizeof(leafnode));
        leafnode *new_root =
            new (ptr) leafnode(this, split_key, new_sibling, level_ + 1);
        clflush((char *)new_root, sizeof(leafnode), false, true);
        p->entry[pkx_.p].value = new_root;
        clflush((char *)&p->entry[pkx_.p].value, sizeof(uintptr_t), false,
                true);
        p->writeUnlock(false);
      } else {
        root = p;
        t->split(p->entry[pkx_.p].value, root, depth, tmp_lv, split_key,
                 new_sibling, level_ + 1, NULL, false);
      }
      free(tmp_lv);
    } else {
      if (t->root() == this) {
        void *ptr = t->pmem_allocator_->Alloc(sizeof(leafnode));
        memset(ptr, 0, sizeof(leafnode));
        leafnode *new_root =
            new (ptr) leafnode(this, split_key, new_sibling, level_ + 1);
        clflush((char *)new_root, sizeof(leafnode), false, true);
        t->setNewRoot(new_root);
      } else {
        t->split(NULL, NULL, 0, NULL, split_key, new_sibling, level_ + 1, NULL,
                 false);
      }
    }
    // permutation base final insertion
    if (split_type == 0) {
      kx_.p = perml.back();
      if (nl->entry[kx_.p].value != NULL) isOverWrite = true;

      PKeyBlock *pb = nullptr;
      int prev_slot = kx_.i;
      if (kx_.i > 0 && IS_LV(this->value(perml[kx_.i - 1]))) {
        prev_slot = kx_.i - 1;
        pb = get_pkey_block_addr(
            (char *)(LV_PTR(this->value(perml[prev_slot])))->value);
      } else if (kx_.i < perml.size() && IS_LV(this->value(perml[kx_.i]))) {
        pb = get_pkey_block_addr(
            (char *)(LV_PTR(this->value(perml[kx_.i])))->value);
      } else {
        pb = (PKeyBlock *)t->pkey_block_allocator_->Alloc(PKEY_BLOCK_SIZE);
        pb->init();
      }

      char *ptr = pb->Append(value, seq, nullptr);
      if (!ptr) {
        int min_slot, max_slot;
        get_slot_range_in_same_pkey_block(prev_slot, &min_slot, &max_slot);
        bool free_cur_pb = false;
        ptr = insert_and_split_to_new_pkey_block(t, kx_.i, min_slot, max_slot,
                                                 false, pb, free_cur_pb, value,
                                                 seq, ht);
        if (free_cur_pb) {
          t->pkey_block_allocator_->Free(pb, PKEY_BLOCK_SIZE);
        }
      }

      leafvalue *new_lv = t->make_leaf(key.data(), key.size(), (uint64_t)ptr);

      nl->assign(kx_.p, fkey, SET_LV(new_lv));
      clflush((char *)(&nl->entry[kx_.p]), sizeof(kv), false, true);

      permuter cp = nl->permutation.value();
      cp.insert_from_back(kx_.i);
      fence();
      nl->permutation = cp.value();
      clflush((char *)(&nl->permutation), sizeof(permuter), false, true);
      ret = nl;
    } else {
      kx_.i = kx_.p = kx_.i - perml.size();

      permuter cp = nr->permutation.value();
      cp.insert_from_back(kx_.i);
      fence();
      nr->permutation = cp.value();
      clflush((char *)(&nr->permutation), sizeof(permuter), false, true);
      ret = nr;
    }


    nr->writeUnlock(false);
    nl->writeUnlock(isOverWrite);
  }

  return ret;
}

void leafnode::leaf_update_pkey(masstree *t, key_indexed_position &kx_,
                                const Slice &key, const Slice &value,
                                SequenceNumber seq, clht *ht) {
  leafvalue *cur_lv = LV_PTR(this->value(kx_.p));
  char *prev = (char *)cur_lv->value;
  PKeyBlock *pb = get_pkey_block_addr(prev);
  char *ret = pb->Append(value, seq, prev);
  if (!ret) {
    int min_slot, max_slot;
    get_slot_range_in_same_pkey_block(kx_.i, &min_slot, &max_slot);
    bool free_cur_pb = false;
    insert_and_split_to_new_pkey_block(t, kx_.i, min_slot, max_slot, true, pb,
                                       free_cur_pb, value, seq, ht);
    if (free_cur_pb) {
      t->pkey_block_allocator_->Free(pb, PKEY_BLOCK_SIZE);
    }
  } else {
    cur_lv->value = (uint64_t)ret;
    clflush(&cur_lv->value, sizeof(void *), false, true);
  }
}

void *leafnode::leaf_delete(masstree *t, void *root, uint32_t depth,
                            leafvalue *lv, key_indexed_position &kx_,
                            ThreadInfo &threadInfo) {
  int merge_state;
  void *ret = NULL;

  // permutation based remove
  if (this->permutation.size() > LEAF_THRESHOLD) {
    permuter cp = this->permutation.value();
    cp.remove_to_back(kx_.i);
    fence();
    this->permutation = cp.value();
    clflush((char *)(&this->permutation), sizeof(permuter), false, true);
    if (lv != NULL)
      threadInfo.getEpoche().markNodeForDeletion((LV_PTR(this->value(kx_.p))),
                                                 threadInfo);
    this->writeUnlock(false);
    ret = this;
  } else {
    // Underflow
    // Merge
    permuter cp;
    leafnode *nl, *nr;
    nr = reinterpret_cast<leafnode *>(this);

    if (depth > 0) {
      key_indexed_position pkx_;
      leafnode *p = correct_layer_root(root, lv, depth, pkx_);
      if (p->value(pkx_.p) == nr) {
        cp = nr->permutation.value();
        cp = cp.make_empty();
        fence();
        nr->permutation = cp.value();
        clflush((char *)(&nr->permutation), sizeof(permuter), false, true);
        p->writeUnlock(false);
        nr->writeUnlock(false);
        return nr;
      } else {
        nl = search_for_leftsibling(NULL, &p->entry[pkx_.p].value,
                                    nr->highest ? nr->highest - 1 : nr->highest,
                                    nr->level_, nr);
        merge_state =
            t->merge(p->entry[pkx_.p].value, reinterpret_cast<void *>(p), depth,
                     lv, nr->highest, nr->level_ + 1, threadInfo);
        if (merge_state == 16) {
          p = correct_layer_root(root, lv, depth, pkx_);
          p->entry[pkx_.p].value = nr;
          clflush((char *)&p->entry[pkx_.p].value, sizeof(void *), false, true);
          p->writeUnlock(false);
        }
      }
    } else {
      if (t->root() == nr) {
        cp = nr->permutation.value();
        cp = cp.make_empty();
        fence();
        nr->permutation = cp.value();
        clflush((char *)(&nr->permutation), sizeof(permuter), false, true);
        nr->writeUnlock(false);
        return nr;
      } else {
        nl = search_for_leftsibling(t->root_dp(), NULL,
                                    nr->highest ? nr->highest - 1 : nr->highest,
                                    nr->level_, nr);
        merge_state = t->merge(NULL, NULL, 0, NULL, nr->highest, nr->level_ + 1,
                               threadInfo);
        if (merge_state == 16) t->setNewRoot(nr);
      }
    }

    // Final step for node reclamation
    // next pointer is changed, except for leftmost child
    if (merge_state >= 0 && merge_state < 16) {
      nl->next.store(nr->next.load(std::memory_order_acquire),
                     std::memory_order_release);
      clflush((char *)(&nl->next), sizeof(leafnode *), false, true);
    }

    cp = nr->permutation.value();
    cp = cp.make_empty();
    nr->permutation = cp.value();
    clflush((char *)(&nr->permutation), sizeof(permuter), false, true);

    if (nl != nr) {
      if (merge_state >= 0 && merge_state < 16) {
        nr->writeUnlockObsolete();
        threadInfo.getEpoche().markNodeForDeletion(nr, threadInfo);
      } else {
        nr->writeUnlock(false);
      }
      nl->writeUnlock(false);
    } else {
      if (merge_state >= 0 && merge_state < 16) {
        nr->writeUnlockObsolete();
        threadInfo.getEpoche().markNodeForDeletion(nr, threadInfo);
      } else {
        nr->writeUnlock(false);
      }
    }
    ret = nr;
  }

  assert(ret != NULL);
  return ret;
}

void *leafnode::inter_insert(masstree *t, void *root, uint32_t depth,
                             leafvalue *lv, uint64_t key, void *value,
                             key_indexed_position &kx_, leafnode *child,
                             bool child_isOverWrite) {
  bool isOverWrite = false;
  void *ret;

  // permutation based insert
  if (this->permutation.size() < LEAF_WIDTH) {
    kx_.p = this->permutation.back();
    if (entry[kx_.p].value != NULL) isOverWrite = true;

    this->assign(kx_.p, key, value);
    clflush((char *)(&this->entry[kx_.p]), sizeof(kv), false, true);

    permuter cp = this->permutation.value();
    cp.insert_from_back(kx_.i);
    fence();
    this->permutation = cp.value();
    clflush((char *)(&this->permutation), sizeof(permuter), false, true);

    if (child != NULL) {
      child->next.load(std::memory_order_acquire)->writeUnlock(false);
      child->writeUnlock(child_isOverWrite);
    }

    this->writeUnlock(isOverWrite);
    ret = this;
  } else {
    // overflow
    // internode split

    // TODO: Need to add an additional context here checking crash and running
    // recovery mechanism to avoid the duplicate split for the node that was
    // split, but the permutor was not updated to only reflect the half of the
    // entries.
    // * Algorithm sketch:
    // i) compare the high key of right sibling with the highest key of left
    // sibling. ii) If the highest key of left sibling is larger than the high
    // key of right sibling,
    //     1) invalidate the right sibling by atomically changing the next
    //     pointer of left
    //        sibling to the next pointer of right sibling. And, continue to the
    //        split process. (current implementation reflects this way, but will
    //        be changed to second method)
    //     2) replay the original split process from the third step that removes
    //     the half of
    //        the entries from the left sibling. (this would be more reasonable
    //        in terms of reusing the existing split mechanism)
    if (this->next != NULL &&
        this->key(this->permutation[this->permutation.size() - 1]) >
            this->next.load(std::memory_order_acquire)->highest) {
      this->next.store(this->next.load(std::memory_order_acquire)
                           ->next.load(std::memory_order_acquire),
                       std::memory_order_release);
      clflush((char *)&this->next, sizeof(leafnode *), false, true);
    }

    void *ptr = t->pmem_allocator_->Alloc(sizeof(leafnode));
    memset(ptr, 0, sizeof(leafnode));
    leafnode *new_sibling = new (ptr) leafnode(this->level_);
    new_sibling->writeLock();
    uint64_t split_key;
    this->split_into_inter(new_sibling, split_key);

    leafnode *nl = reinterpret_cast<leafnode *>(this);
    leafnode *nr = reinterpret_cast<leafnode *>(new_sibling);

    permuter perml = nl->permutation;
    int width = perml.size();
    // Removing mid-1 entry
    perml.set_size(width - (nr->permutation.size() + 1));

    if (width != LEAF_WIDTH) perml.exchange(perml.size(), LEAF_WIDTH - 1);

    nl->permutation = perml.value();
    clflush((char *)(&nl->permutation), sizeof(permuter), false, true);

    if (key < split_key) {
      kx_.p = nl->permutation.back();
      if (nl->entry[kx_.p].value != NULL) isOverWrite = true;

      nl->assign(kx_.p, key, value);
      clflush((char *)(&nl->entry[kx_.p]), sizeof(kv), false, true);

      permuter cp = nl->permutation.value();
      cp.insert_from_back(kx_.i);
      fence();
      nl->permutation = cp.value();
      clflush((char *)(&nl->permutation), sizeof(permuter), false, true);

      ret = nl;
    } else {
      kx_ = nr->key_lower_bound_by(key);
      kx_.p = nr->permutation.back();
      nr->assign(kx_.p, key, value);
      clflush((char *)(&nr->entry[kx_.p]), sizeof(kv), false, true);

      permuter cp = nr->permutation.value();
      cp.insert_from_back(kx_.i);
      fence();
      nr->permutation = cp.value();
      clflush((char *)(&nr->permutation), sizeof(permuter), false, true);

      ret = nr;
    }

    // lock coupling (hand-over-hand locking)
    if (child != NULL) {
      child->next.load(std::memory_order_acquire)->writeUnlock(false);
      child->writeUnlock(child_isOverWrite);
    }

    if (depth > 0) {
      key_indexed_position pkx_;
      leafnode *p = correct_layer_root(root, lv, depth, pkx_);
      if (p->value(pkx_.p) == this) {
        void *ptr = t->pmem_allocator_->Alloc(sizeof(leafnode));
        memset(ptr, 0, sizeof(leafnode));
        leafnode *new_root =
            new (ptr) leafnode(this, split_key, new_sibling, level_ + 1);
        clflush((char *)new_root, sizeof(leafnode), false, true);
        p->entry[pkx_.p].value = new_root;
        clflush((char *)&p->entry[pkx_.p].value, sizeof(uintptr_t), false,
                true);
        p->writeUnlock(false);

        this->next.load(std::memory_order_acquire)->writeUnlock(false);
        this->writeUnlock(isOverWrite);
      } else {
        root = p;
        t->split(p->entry[pkx_.p].value, root, depth, lv, split_key,
                 new_sibling, level_ + 1, this, isOverWrite);
      }
    } else {
      if (t->root() == this) {
        void *ptr = t->pmem_allocator_->Alloc(sizeof(leafnode));
        memset(ptr, 0, sizeof(leafnode));
        leafnode *new_root =
            new (ptr) leafnode(this, split_key, new_sibling, level_ + 1);
        clflush((char *)new_root, sizeof(leafnode), false, true);
        t->setNewRoot(new_root);

        this->next.load(std::memory_order_acquire)->writeUnlock(false);
        this->writeUnlock(isOverWrite);
      } else {
        t->split(NULL, NULL, 0, NULL, split_key, new_sibling, level_ + 1, this,
                 isOverWrite);
      }
    }
  }

  return ret;
}

int leafnode::inter_delete(masstree *t, void *root, uint32_t depth,
                           leafvalue *lv, key_indexed_position &kx_,
                           ThreadInfo &threadInfo) {
  int ret, merge_state;

  // permutation based remove
  if (this->permutation.size() >= LEAF_THRESHOLD) {
    permuter cp;
    if (kx_.i >= 0) {
      cp = this->permutation.value();
      cp.remove_to_back(kx_.i);
      fence();
      this->permutation = cp.value();
      clflush((char *)(&this->permutation), sizeof(permuter), false, true);
    }

    this->writeUnlock(false);
  } else {
    // Underflow
    // Merge
    permuter cp;
    leafnode *nl, *nr;
    nr = reinterpret_cast<leafnode *>(this);

    if (depth > 0) {
      key_indexed_position pkx_;
      leafnode *p = correct_layer_root(root, lv, depth, pkx_);
      if (p->value(pkx_.p) == nr) {
        kx_.i = 16;
        p->writeUnlock(false);
        nr->writeUnlockObsolete();
        threadInfo.getEpoche().markNodeForDeletion(nr, threadInfo);
        return (ret = kx_.i);
      } else {
        nl = search_for_leftsibling(NULL, &p->entry[pkx_.p].value,
                                    nr->highest ? nr->highest - 1 : nr->highest,
                                    nr->level_, nr);
        merge_state = t->merge(p->entry[pkx_.p].value, root, depth, lv,
                               nr->highest, nr->level_ + 1, threadInfo);
      }
    } else {
      if (t->root() == nr) {
        kx_.i = 16;
        nr->writeUnlockObsolete();
        threadInfo.getEpoche().markNodeForDeletion(nr, threadInfo);
        return (ret = kx_.i);
      } else {
        nl = search_for_leftsibling(t->root_dp(), NULL,
                                    nr->highest ? nr->highest - 1 : nr->highest,
                                    nr->level_, nr);
        merge_state = t->merge(NULL, NULL, 0, NULL, nr->highest, nr->level_ + 1,
                               threadInfo);
      }
    }

    // Final step for internal node reclamation
    if (merge_state >= 0 && merge_state < 16) {
      nl->next.store(nr->next.load(std::memory_order_acquire),
                     std::memory_order_release);
      clflush((char *)(&nl->next), sizeof(leafnode *), false, true);
    } else if (merge_state == 16) {
      kx_.i = 16;
    }

    if (nl != nr) {
      if ((merge_state >= 0 && merge_state < 16) || merge_state == 16) {
        nr->writeUnlockObsolete();
        threadInfo.getEpoche().markNodeForDeletion(nr, threadInfo);
      } else {
        nr->writeUnlock(false);
      }
      nl->writeUnlock(false);
    } else {
      if ((merge_state >= 0 && merge_state < 16) || merge_state == 16) {
        nr->writeUnlockObsolete();
        threadInfo.getEpoche().markNodeForDeletion(nr, threadInfo);
      } else {
        nr->writeUnlock(false);
      }
    }
  }

  return (ret = kx_.i);
}

void leafnode::get_slot_range_in_same_pkey_block(int cur_slot, int *min_slot,
                                                 int *max_slot) {
  permuter perm = permutation;
  char *cur_val = (char *)(LV_PTR(value(perm[cur_slot])))->value;
  PKeyBlock *cur_pb = get_pkey_block_addr(cur_val);
  int i;
  for (i = cur_slot - 1; i >= 0; i--) {
    if (!IS_LV(value(perm[i]))) break;
    char *pkh_ptr = (char *)(LV_PTR(value(perm[i])))->value;
    PKeyBlock *p = get_pkey_block_addr(pkh_ptr);
    assert(p);
    if (p != cur_pb) break;
  }
  *min_slot = i + 1;
  int num_entries = perm.size();
  for (i = cur_slot + 1; i < num_entries; i++) {
    if (!IS_LV(value(perm[i]))) break;
    char *pkh_ptr = (char *)(LV_PTR(value(perm[i])))->value;
    PKeyBlock *p = get_pkey_block_addr(pkh_ptr);
    assert(p);
    if (p != cur_pb) break;
  }
  *max_slot = i - 1;
}

char *leafnode::insert_and_split_to_new_pkey_block(
    masstree *t, int ins_slot, int min_slot, int max_slot, bool update,
    PKeyBlock *cur_pb, bool &free_cur_pb, const Slice &value,
    SequenceNumber seq, clht *ht) {
  permuter perm = permutation;
  assert(min_slot <= max_slot);
  assert(max_slot < perm.size() && ins_slot <= perm.size());
  int slot_cnt = update ? max_slot - min_slot + 1 : max_slot - min_slot + 2;

  assert(ins_slot == max_slot + 1 ||
         get_pkey_block_addr(
             (char *)(LV_PTR(this->value(perm[ins_slot])))->value) == cur_pb);

  PKeyBlock *new_pb =
      (PKeyBlock *)t->pkey_block_allocator_->Alloc(PKEY_BLOCK_SIZE);
  new_pb->init();

  int i = min_slot;
  char *insert_val = nullptr;
  free_cur_pb = false;
  for (int j = 0; j < slot_cnt; j++) {
    std::string buffer;
    buffer.reserve(1024);
    int cur_cnt = 0;
    leafvalue *lv = LV_PTR(this->value(perm[i]));

    if (j != ins_slot - min_slot) {
      // other slots
      PKeyHeader *ph = (PKeyHeader *)lv->value;
      // for each discrete pkeys
      while (ph && (uint64_t)ph != (uint64_t)cur_pb &&
             get_pkey_block_addr(ph) == cur_pb) {
        free_cur_pb = true;
        char *p = (char *)ph;
        p += sizeof(PKeyHeader);
        // for each pkey with one pkey_header
        for (int k = 0; k < ph->count; k++) {
          PKeyEntry *pe = (PKeyEntry *)p;
          if (!pe->deleted) {
            bool pass = false;
            if (ht) {
              // validate
              uint64_t pkey_hash = pe->size <= sizeof(uint64_t)
                                       ? *(uint64_t *)pe->data
                                       : MurmurHash64A(pe->data, pe->size);
              pass = !clht_check_seqcnt(ht->ht, pkey_hash, pe->seq, true);
            }
            if (!pass) {
              ++cur_cnt;
              buffer.append(p, sizeof(PKeyEntry) + pe->size);
            }
          }
          p += sizeof(PKeyEntry) + pe->size;
        }
        ph = ph->get_prev();
      }
      if (ph && (PKeyBlock *)ph != cur_pb) {
        assert(get_pkey_block_addr(ph)->exclusive);
      }

      if (cur_cnt > 0) {
        if (new_pb->HalfFull() || !new_pb->HasSpaceFor(buffer.size())) {
          // new block
          new_pb->Persist();
          new_pb =
              (PKeyBlock *)t->pkey_block_allocator_->Alloc(PKEY_BLOCK_SIZE);
          new_pb->init();
        }
        lv->value = (uint64_t)new_pb->AppendBatchNoPersist(buffer, cur_cnt, ph);
      } else {
        if (ph && (PKeyBlock *)ph != get_pkey_block_addr(ph) &&
            get_pkey_block_addr(ph) != cur_pb) {
          // has values in other pkey blocks
          assert(get_pkey_block_addr(ph)->exclusive);
          char *ptr = new_pb->CollectPKeysAndAppendMultiBlocks(
              t->pkey_block_allocator_, ph, ht);
          if (ptr) {
            new_pb = get_pkey_block_addr(ptr);
            lv->value = (uint64_t)ptr;
          } else {
            lv->value = (uint64_t)new_pb;
          }
        } else {
          lv->value = (uint64_t)new_pb;
        }
      }
      clflush((char *)&(lv->value), sizeof(uint64_t), false, false);
      i++;
    } else {
      // current slot
      std::vector<int> offset;
      if (value.size() > 0) {
        PKeyEntry pe;
        pe.size = value.size();
        pe.seq = seq;
        offset.push_back(buffer.size());
        buffer.append((char *)&pe, sizeof(PKeyEntry));
        buffer.append(value.data(), value.size());
      }
      PKeyHeader *ph = nullptr;
      std::set<PKeyBlock *> gc_set;
      if (update) {
        // append old pkeys
        assert(i == ins_slot);
        ph = (PKeyHeader *)lv->value;
        // for each discrete pkeys
        while (ph && (uint64_t)ph != (uint64_t)cur_pb &&
               get_pkey_block_addr(ph) == cur_pb) {
          free_cur_pb = true;
          char *p = (char *)ph;
          p += sizeof(PKeyHeader);
          // for each pkey with one pkey_header
          for (int k = 0; k < ph->count; k++) {
            PKeyEntry *pe = (PKeyEntry *)p;
            if (!pe->deleted) {
              bool pass = false;
              if (ht) {
                // validate
                uint64_t pkey_hash = pe->size <= sizeof(uint64_t)
                                         ? *(uint64_t *)pe->data
                                         : MurmurHash64A(pe->data, pe->size);
                pass = !clht_check_seqcnt(ht->ht, pkey_hash, pe->seq, true);
              }
              if (!pass) {
                offset.push_back(buffer.size());
                buffer.append(p, sizeof(PKeyEntry) + pe->size);
              }
            }
            p += sizeof(PKeyEntry) + pe->size;
          }
          ph = ph->get_prev();
        }
        if (ph && (uint64_t)ph != (uint64_t)cur_pb) {
          assert(get_pkey_block_addr(ph)->exclusive);
          // GC
          if (ph->deleted_count >= ph->count / 2) {
            CollectPKeys(ph, buffer, offset, gc_set, ht);
            ph = nullptr;
          }
        }
      }

      char *cur_ptr = new_pb->AppendMultiBlocks(t->pkey_block_allocator_,
                                                buffer, offset, ph);
      assert(cur_ptr);
      new_pb = get_pkey_block_addr(cur_ptr);

      if (update) {
        lv->value = (uint64_t)cur_ptr;
        clflush((char *)&(lv->value), sizeof(uint64_t), false, false);
        ++i;
      }
      insert_val = cur_ptr;

      // free old pkey blocks
      for (PKeyBlock *old_pb : gc_set) {
        t->pkey_block_allocator_->Free(old_pb, PKEY_BLOCK_SIZE);
      }
    }
  }
  new_pb->Persist();
  return insert_val;
}

void leafnode::split_to_new_pkey_block(masstree *t, int min_slot,
                                       int max_slot, clht *ht) {
  permuter perm = this->permute();
  PKeyBlock *cur_pb =
      get_pkey_block_addr((char *)(LV_PTR(this->value(perm[min_slot])))->value);
  PKeyBlock *new_pb =
      (PKeyBlock *)t->pkey_block_allocator_->Alloc(PKEY_BLOCK_SIZE);
  new_pb->init();

  for (int i = min_slot; i <= max_slot; ++i) {
    std::string buffer;
    buffer.reserve(512);
    int cur_cnt = 0;
    assert(IS_LV(this->value(perm[i])));
    leafvalue *lv = LV_PTR(this->value(perm[i]));
    PKeyHeader *ph = (PKeyHeader *)lv->value;
    while (ph && (PKeyBlock *)ph != cur_pb &&
           get_pkey_block_addr(ph) == cur_pb) {
      char *p = (char *)ph;
      p += sizeof(PKeyHeader);
      // for each pkey with one pkey_header
      for (int k = 0; k < ph->count; k++) {
        PKeyEntry *pe = (PKeyEntry *)p;
        if (!pe->deleted) {
          bool pass = false;
          if (ht) {
            // validate
            uint64_t pkey_hash = pe->size <= sizeof(uint64_t)
                                     ? *(uint64_t *)pe->data
                                     : MurmurHash64A(pe->data, pe->size);
            pass = !clht_check_seqcnt(ht->ht, pkey_hash, pe->seq, true);
          }
          if (!pass) {
            ++cur_cnt;
            buffer.append(p, sizeof(PKeyEntry) + pe->size);
          }
        }
        p += sizeof(PKeyEntry) + pe->size;
      }
      ph = ph->get_prev();
    }
    if (ph && (uint64_t)ph != (uint64_t)cur_pb) {
      assert(get_pkey_block_addr(ph)->exclusive);
      // GC
      // TODO:
    }

    if (cur_cnt > 0) {
      if (new_pb->HalfFull() || !new_pb->HasSpaceFor(buffer.size())) {
        // new block
        new_pb->Persist();
        new_pb = (PKeyBlock *)t->pkey_block_allocator_->Alloc(PKEY_BLOCK_SIZE);
        new_pb->init();
      }
      char *ptr = new_pb->AppendBatchNoPersist(buffer, cur_cnt, ph);
      assert(ptr);
      lv->value = (uint64_t)ptr;
    } else {
      if (ph && (PKeyBlock *)ph != get_pkey_block_addr(ph) &&
          get_pkey_block_addr(ph) != cur_pb) {
        assert(get_pkey_block_addr(ph)->exclusive);
        char *ptr = new_pb->CollectPKeysAndAppendMultiBlocks(
            t->pkey_block_allocator_, ph, ht);
        if (ptr) {
          new_pb = get_pkey_block_addr(ptr);
          lv->value = (uint64_t)ptr;
        } else {
          lv->value = (uint64_t)new_pb;
        }
      } else {
        lv->value = (uint64_t)new_pb;
      }
    }
    clflush((char *)&(lv->value), sizeof(uint64_t), false, false);
  }
  new_pb->Persist();
}

void masstree::split(void *left, void *root, uint32_t depth, leafvalue *lv,
                     uint64_t key, void *right, uint32_t level, void *child,
                     bool isOverWrite) {
  leafnode *p = NULL;
  key_indexed_position kx_;
  leafnode *next = NULL;
  void *snapshot_v = NULL;

  int needRestart;
  uint64_t v;

  if (depth > 0) {
    if (level > reinterpret_cast<leafnode *>(left)->level()) return;
    p = reinterpret_cast<leafnode *>(left);
    reinterpret_cast<leafnode *>(root)->writeUnlock(false);
  } else {
    if (level >
        reinterpret_cast<leafnode *>(root_.load(std::memory_order_acquire))
            ->level())
      return;
    p = reinterpret_cast<leafnode *>(root_.load(std::memory_order_acquire));
  }

from_root:
  while (p->level() > level) {
  inter_retry:
    next = p->advance_to_key(key);
    if (next != p) {
      p = next;
      goto inter_retry;
    }

    v = p->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto inter_retry;
      else {
        if (depth > 0)
          p = reinterpret_cast<leafnode *>(left);
        else
          p = reinterpret_cast<leafnode *>(
              root_.load(std::memory_order_acquire));
        goto from_root;
      }
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(key);

    if (kx_.i >= 0)
      snapshot_v = p->value(kx_.p);
    else
      snapshot_v = p->leftmost();

    p->checkOrRestart(v, needRestart);
    if (needRestart)
      goto inter_retry;
    else
      p = reinterpret_cast<leafnode *>(snapshot_v);
  }

leaf_retry:
  next = p->advance_to_key(key);
  if (next != p) {
    p = next;
    goto leaf_retry;
  }

  p->writeLockOrRestart(needRestart);
  if (needRestart) {
    if (needRestart == LOCKED)
      goto leaf_retry;
    else {
      if (depth > 0)
        p = reinterpret_cast<leafnode *>(left);
      else
        p = reinterpret_cast<leafnode *>(root_.load(std::memory_order_acquire));
      goto from_root;
    }
  }

  next = p->advance_to_key(key);
  if (next != p) {
    p->writeUnlock(false);
    p = next;
    goto leaf_retry;
  }

  p->prefetch();
  fence();

  kx_ = p->key_lower_bound_by(key);
  if (kx_.p >= 0 || key == p->highest_()) {
    p->writeUnlock(false);
    reinterpret_cast<leafnode *>(right)->writeUnlock(false);
    reinterpret_cast<leafnode *>(child)->writeUnlock(false);
    return;
  }

  if (!p->inter_insert(this, root, depth, lv, key, right, kx_,
                       reinterpret_cast<leafnode *>(child), isOverWrite)) {
    split(left, root, depth, lv, key, right, level, child, isOverWrite);
  }
}

int masstree::merge(void *left, void *root, uint32_t depth, leafvalue *lv,
                    uint64_t key, uint32_t level, ThreadInfo &threadInfo) {
  leafnode *p = NULL;
  key_indexed_position kx_;
  leafnode *next = NULL;
  void *snapshot_v = NULL;

  int needRestart;
  uint64_t v;

  if (depth > 0) {
    // if (level > reinterpret_cast<leafnode *>(left)->level())
    //     return ;
    p = reinterpret_cast<leafnode *>(left);
    reinterpret_cast<leafnode *>(root)->writeUnlock(false);
  } else {
    // if (level > reinterpret_cast<leafnode *>(this->root_)->level())
    //     return ;
    p = reinterpret_cast<leafnode *>(
        this->root_.load(std::memory_order_acquire));
  }

from_root:
  while (p->level() > level) {
  inter_retry:
    next = p->advance_to_key(key);
    if (next != p) {
      p = next;
      goto inter_retry;
    }

    v = p->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto inter_retry;
      else {
        if (depth > 0)
          p = reinterpret_cast<leafnode *>(left);
        else
          p = reinterpret_cast<leafnode *>(
              this->root_.load(std::memory_order_acquire));
        goto from_root;
      }
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(key);

    if (kx_.i >= 0)
      snapshot_v = p->value(kx_.p);
    else
      snapshot_v = p->leftmost();

    p->checkOrRestart(v, needRestart);
    if (needRestart)
      goto inter_retry;
    else
      p = reinterpret_cast<leafnode *>(snapshot_v);
  }

leaf_retry:
  next = p->advance_to_key(key);
  if (next != p) {
    p = next;
    goto leaf_retry;
  }

  p->writeLockOrRestart(needRestart);
  if (needRestart) {
    if (needRestart == LOCKED)
      goto leaf_retry;
    else {
      if (depth > 0)
        p = reinterpret_cast<leafnode *>(left);
      else
        p = reinterpret_cast<leafnode *>(
            this->root_.load(std::memory_order_acquire));
      goto from_root;
    }
  }

  next = p->advance_to_key(key);
  if (next != p) {
    p->writeUnlock(false);
    p = next;
    goto leaf_retry;
  }

  p->prefetch();
  fence();

  kx_ = p->key_lower_bound(key);

  return p->inter_delete(this, root, depth, lv, kx_, threadInfo);
}

void *masstree::get(uint64_t key, ThreadInfo &threadEpocheInfo) {
  EpocheGuard epocheGuard(threadEpocheInfo);
  void *root = NULL;
  key_indexed_position kx_;
  leafnode *next = NULL;
  void *snapshot_v = NULL;

  int needRestart;
  uint64_t v;

restart:
  root = this->root_.load(std::memory_order_acquire);
  leafnode *p = reinterpret_cast<leafnode *>(root);
  while (p->level() != 0) {
  inter_retry:
    next = p->advance_to_key(key);
    if (next != p) {
      p = next;
      goto inter_retry;
    }

    v = p->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto inter_retry;
      else
        goto restart;
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(key);

    if (kx_.i >= 0)
      snapshot_v = p->value(kx_.p);
    else
      snapshot_v = p->leftmost();

    p->checkOrRestart(v, needRestart);
    if (needRestart)
      goto inter_retry;
    else
      p = reinterpret_cast<leafnode *>(snapshot_v);
  }

  leafnode *l = reinterpret_cast<leafnode *>(p);
leaf_retry:
  next = l->advance_to_key(key);
  if (next != l) {
    l = next;
    goto leaf_retry;
  }

  v = l->readLockOrRestart(needRestart);
  if (needRestart) {
    if (needRestart == LOCKED)
      goto leaf_retry;
    else
      goto restart;
  }

  l->prefetch();
  fence();

  kx_ = l->key_lower_bound_by(key);

  if (kx_.p >= 0)
    snapshot_v = l->value(kx_.p);
  else
    snapshot_v = NULL;

  l->checkOrRestart(v, needRestart);
  if (needRestart)
    goto leaf_retry;
  else {
    if (!snapshot_v) {
      next = l->advance_to_key(key);
      if (next != l) {
        l = next;
        goto leaf_retry;
      }
#if 0
            printf("should not enter here\n");
            printf("key = %lu, searched key = %lu, key index = %d\n", key, l->key(kx_.p), kx_.p);
            permuter cp = l->permute();
            for (int i = 0; i < cp.size(); i++) {
                printf("key = %lu\n", l->key(cp[i]));
            }

            if (l->next_()) {
                cp = l->next_()->permute();
                printf("next high key = %lu\n", l->next_()->highest_());
                for (int i = 0; i < cp.size(); i++) {
                    printf("next key = %lu\n", l->next_()->key(cp[i]));
                }
            }
#endif
    }
    return snapshot_v;
  }
}

void *masstree::get(const Slice &key, ThreadInfo &threadEpocheInfo) {
  EpocheGuard epocheGuard(threadEpocheInfo);
  void *root = NULL;
  key_indexed_position kx_;
  uint32_t depth = 0;
  leafnode *next = NULL;
  void *snapshot_v = NULL;

  int needRestart;
  uint64_t v;

  leafvalue *tmp_lv = make_leaf(key.data(), key.size(), 0);

restart:
  depth = 0;
  root = this->root_.load(std::memory_order_acquire);
  leafnode *p = reinterpret_cast<leafnode *>(root);
from_root:
  while (p->level() != 0) {
  inter_retry:
    next = p->advance_to_key(tmp_lv->fkey[depth]);
    if (next != p) {
      p = next;
      goto inter_retry;
    }

    v = p->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto inter_retry;
      else
        goto restart;
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(tmp_lv->fkey[depth]);

    if (kx_.i >= 0)
      snapshot_v = p->value(kx_.p);
    else
      snapshot_v = p->leftmost();

    p->checkOrRestart(v, needRestart);
    if (needRestart)
      goto inter_retry;
    else
      p = reinterpret_cast<leafnode *>(snapshot_v);
  }

  leafnode *l = reinterpret_cast<leafnode *>(p);
leaf_retry:
  next = l->advance_to_key(tmp_lv->fkey[depth]);
  if (next != l) {
    l = next;
    goto leaf_retry;
  }

  v = l->readLockOrRestart(needRestart);
  if (needRestart) {
    if (needRestart == LOCKED)
      goto leaf_retry;
    else
      goto restart;
  }

  l->prefetch();
  fence();

  kx_ = l->key_lower_bound_by(tmp_lv->fkey[depth]);
  if (kx_.p >= 0) {
    snapshot_v = l->value(kx_.p);
    if (!IS_LV(snapshot_v)) {
      // If there is additional layer, traverse B+tree in the next layer
      l->checkOrRestart(v, needRestart);
      if (needRestart)
        goto leaf_retry;
      else {
        p = reinterpret_cast<leafnode *>(snapshot_v);
        depth++;
        goto from_root;
      }
    } else {
      snapshot_v = (void *)(LV_PTR(snapshot_v));
    }
  } else {
    snapshot_v = NULL;
  }

  l->checkOrRestart(v, needRestart);
  if (needRestart)
    goto leaf_retry;
  else {
    if (snapshot_v) {
      if (((leafvalue *)(snapshot_v))->key_len == tmp_lv->key_len &&
          memcmp(((leafvalue *)(snapshot_v))->fkey, tmp_lv->fkey,
                 aligned_len(tmp_lv->key_len)) == 0) {
        snapshot_v = (void *)(((leafvalue *)(snapshot_v))->value);
      } else {
        snapshot_v = NULL;
      }
    } else {
      next = l->advance_to_key(tmp_lv->fkey[depth]);
      if (next != l) {
        l = next;
        goto leaf_retry;
      }
#if 0
            printf("should not enter here\n");
            printf("fkey = %s, key = %lu, searched key = %lu, key index = %d\n",
                    (char *)(lv->fkey), lv->fkey[depth], l->key(kx_.p), kx_.p);

            permuter cp = l->permute();
            for (int i = 0; i < cp.size(); i++) {
                printf("key = %lu\n", l->key(cp[i]));
                printf("fkey = %s\n", (char *)((LV_PTR(l->value(cp[i])))->fkey));
            }

            if (l->next_()) {
                cp = l->next_()->permute();
                for (int i = 0; i < cp.size(); i++) {
                    printf("next key = %lu\n", l->next_()->key(cp[i]));
                }
            }
#endif
    }

    free(tmp_lv);
    return snapshot_v;
  }
}

void leafnode::get_range(leafvalue *&lv, int num, int &count, uint64_t *buf,
                         leafnode *root, uint32_t depth) {
  key_indexed_position kx_;
  leafnode *next = NULL;
  void *snapshot_v = NULL, *snapshot_n = NULL;
  permuter perm;
  int backup, prev_count = count;

  int needRestart;
  uint64_t v;

restart:
  count = prev_count;
  leafnode *p = root;
  while (p->level() != 0) {
  inter_retry:
    next = p->advance_to_key(lv->fkey[depth]);
    if (next != p) {
      p = next;
      goto inter_retry;
    }

    v = p->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto inter_retry;
      else
        goto restart;
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(lv->fkey[depth]);

    if (kx_.i >= 0)
      snapshot_v = p->value(kx_.p);
    else
      snapshot_v = p->leftmost();

    p->checkOrRestart(v, needRestart);
    if (needRestart)
      goto inter_retry;
    else
      p = reinterpret_cast<leafnode *>(snapshot_v);
  }

  leafnode *l = reinterpret_cast<leafnode *>(p);
  while (count < num) {
  leaf_retry:
    backup = count;
    snapshot_n = l->next_();
    perm = l->permute();

    v = l->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto leaf_retry;
      else
        goto restart;
    }

    l->prefetch();
    fence();

    for (int i = 0; i < perm.size() && count < num; i++) {
      snapshot_v = l->value(perm[i]);
      if (!IS_LV(l->value(perm[i]))) {
        if (l->key(perm[i]) > lv->fkey[depth]) {
          p = reinterpret_cast<leafnode *>(snapshot_v);
          leafvalue *smallest = p->smallest_leaf(lv->key_len, lv->value);
          p->get_range(smallest, num, count, buf, p, depth + 1);
          free(smallest);
        } else if (l->key(perm[i]) == lv->fkey[depth]) {
          p = reinterpret_cast<leafnode *>(snapshot_v);
          p->get_range(lv, num, count, buf, p, depth + 1);
        }
      } else {
        snapshot_v = (LV_PTR(snapshot_v));
        if (l->key(perm[i]) > lv->fkey[depth]) {
          buf[count++] = reinterpret_cast<leafvalue *>(snapshot_v)->value;
        } else if (l->key(perm[i]) == lv->fkey[depth] &&
                   keycmp((LV_PTR(l->value(perm[i])))->fkey, lv->fkey,
                          lv->key_len) >= 0) {
          buf[count++] = reinterpret_cast<leafvalue *>(snapshot_v)->value;
        }
      }
    }

    l->checkOrRestart(v, needRestart);
    if (needRestart) {
      count = backup;
      continue;
    } else {
      if (snapshot_n == NULL)
        break;
      else
        l = reinterpret_cast<leafnode *>(snapshot_n);
    }
  }
}

int masstree::scan(char *min, int num, uint64_t *buf,
                   ThreadInfo &threadEpocheInfo) {
  EpocheGuard epocheGuard(threadEpocheInfo);
  void *root = NULL;
  key_indexed_position kx_;
  uint32_t depth = 0;
  leafnode *next = NULL;
  void *snapshot_v = NULL, *snapshot_n = NULL;
  permuter perm;
  int count, backup;

  leafvalue *lv = make_leaf(min, strlen(min), 0);

  int needRestart;
  uint64_t v;

restart:
  depth = 0;
  count = 0;
  root = this->root_.load(std::memory_order_acquire);
  leafnode *p = reinterpret_cast<leafnode *>(root);
  while (p->level() != 0) {
  inter_retry:
    next = p->advance_to_key(lv->fkey[depth]);
    if (next != p) {
      p = next;
      goto inter_retry;
    }

    v = p->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto inter_retry;
      else
        goto restart;
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(lv->fkey[depth]);

    if (kx_.i >= 0)
      snapshot_v = p->value(kx_.p);
    else
      snapshot_v = p->leftmost();

    p->checkOrRestart(v, needRestart);
    if (needRestart)
      goto inter_retry;
    else
      p = reinterpret_cast<leafnode *>(snapshot_v);
  }

  leafnode *l = reinterpret_cast<leafnode *>(p);
  while (count < num) {
  leaf_retry:
    backup = count;
    snapshot_n = l->next_();
    perm = l->permute();

    v = l->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto leaf_retry;
      else
        goto restart;
    }

    l->prefetch();
    fence();

    for (int i = 0; i < perm.size() && count < num; i++) {
      snapshot_v = l->value(perm[i]);
      if (!IS_LV(l->value(perm[i]))) {
        if (l->key(perm[i]) > lv->fkey[depth]) {
          p = reinterpret_cast<leafnode *>(snapshot_v);
          leafvalue *smallest = p->smallest_leaf(lv->key_len, lv->value);
          p->get_range(smallest, num, count, buf, p, depth + 1);
          free(smallest);
        } else if (l->key(perm[i]) == lv->fkey[depth]) {
          p = reinterpret_cast<leafnode *>(snapshot_v);
          p->get_range(lv, num, count, buf, p, depth + 1);
        }
      } else {
        snapshot_v = (LV_PTR(snapshot_v));
        if (l->key(perm[i]) > lv->fkey[depth]) {
          buf[count++] = reinterpret_cast<leafvalue *>(snapshot_v)->value;
        } else if (l->key(perm[i]) == lv->fkey[depth] &&
                   keycmp((LV_PTR(l->value(perm[i])))->fkey, lv->fkey,
                          lv->key_len) >= 0) {
          buf[count++] = reinterpret_cast<leafvalue *>(snapshot_v)->value;
        }
      }
    }

    l->checkOrRestart(v, needRestart);
    if (needRestart) {
      count = backup;
      continue;
    } else {
      if (snapshot_n == NULL)
        break;
      else
        l = reinterpret_cast<leafnode *>(snapshot_n);
    }
  }

  free(lv);
  return count;
}

int masstree::scan(uint64_t min, int num, uint64_t *buf,
                   ThreadInfo &threadEpocheInfo) {
  EpocheGuard epocheGuard(threadEpocheInfo);
  void *root = NULL;
  key_indexed_position kx_;
  leafnode *next = NULL;
  void *snapshot_v = NULL;
  leafnode *snapshot_n = NULL;
  permuter perm;
  int count, backup;

  int needRestart;
  uint64_t v;

restart:
  count = 0;
  root = this->root_.load(std::memory_order_acquire);
  leafnode *p = reinterpret_cast<leafnode *>(root);
  while (p->level() != 0) {
  inter_retry:
    next = p->advance_to_key(min);
    if (next != p) {
      p = next;
      goto inter_retry;
    }

    v = p->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto inter_retry;
      else
        goto restart;
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(min);

    if (kx_.i >= 0)
      snapshot_v = p->value(kx_.p);
    else
      snapshot_v = p->leftmost();

    p->checkOrRestart(v, needRestart);
    if (needRestart)
      goto inter_retry;
    else
      p = reinterpret_cast<leafnode *>(snapshot_v);
  }

  leafnode *l = reinterpret_cast<leafnode *>(p);
  while (count < num) {
  leaf_retry:
    backup = count;
    snapshot_n = l->next_();
    perm = l->permute();

    v = l->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto leaf_retry;
      else
        goto restart;
    }

    l->prefetch();
    fence();

    for (int i = 0; i < perm.size() && count < num; i++) {
      snapshot_v = l->value(perm[i]);
      if (l->key(perm[i]) >= min) {
        buf[count++] = (uint64_t)snapshot_v;
      }
    }

    l->checkOrRestart(v, needRestart);
    if (needRestart) {
      count = backup;
      continue;
    } else {
      if (snapshot_n == NULL)
        break;
      else
        l = snapshot_n;
    }
  }

  return count;
}

int masstree::scan(const Slice &min, int num, uint64_t *buf,
                   ThreadInfo &threadEpocheInfo) {
  EpocheGuard epocheGuard(threadEpocheInfo);
  void *root = NULL;
  key_indexed_position kx_;
  uint32_t depth = 0;
  leafnode *next = NULL;
  void *snapshot_v = NULL, *snapshot_n = NULL;
  permuter perm;
  int count, backup;

  leafvalue *lv = make_leaf(min.data(), min.size(), 0);

  int needRestart;
  uint64_t v;

restart:
  depth = 0;
  count = 0;
  root = this->root_.load(std::memory_order_acquire);
  leafnode *p = reinterpret_cast<leafnode *>(root);
  while (p->level() != 0) {
  inter_retry:
    next = p->advance_to_key(lv->fkey[depth]);
    if (next != p) {
      p = next;
      goto inter_retry;
    }

    v = p->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto inter_retry;
      else
        goto restart;
    }

    p->prefetch();
    fence();

    kx_ = p->key_lower_bound(lv->fkey[depth]);

    if (kx_.i >= 0)
      snapshot_v = p->value(kx_.p);
    else
      snapshot_v = p->leftmost();

    p->checkOrRestart(v, needRestart);
    if (needRestart)
      goto inter_retry;
    else
      p = reinterpret_cast<leafnode *>(snapshot_v);
  }

  leafnode *l = reinterpret_cast<leafnode *>(p);
  while (count < num) {
  leaf_retry:
    backup = count;
    snapshot_n = l->next_();
    perm = l->permute();

    v = l->readLockOrRestart(needRestart);
    if (needRestart) {
      if (needRestart == LOCKED)
        goto leaf_retry;
      else
        goto restart;
    }

    l->prefetch();
    fence();

    for (int i = 0; i < perm.size() && count < num; i++) {
      snapshot_v = l->value(perm[i]);
      if (!IS_LV(l->value(perm[i]))) {
        if (l->key(perm[i]) > lv->fkey[depth]) {
          p = reinterpret_cast<leafnode *>(snapshot_v);
          leafvalue *smallest = p->smallest_leaf(lv->key_len, lv->value);
          p->get_range(smallest, num, count, buf, p, depth + 1);
          free(smallest);
        } else if (l->key(perm[i]) == lv->fkey[depth]) {
          p = reinterpret_cast<leafnode *>(snapshot_v);
          p->get_range(lv, num, count, buf, p, depth + 1);
        }
      } else {
        snapshot_v = (LV_PTR(snapshot_v));
        if (l->key(perm[i]) > lv->fkey[depth]) {
          buf[count++] = reinterpret_cast<leafvalue *>(snapshot_v)->value;
        } else if (l->key(perm[i]) == lv->fkey[depth] &&
                   keycmp((LV_PTR(l->value(perm[i])))->fkey, lv->fkey,
                          lv->key_len) >= 0) {
          buf[count++] = reinterpret_cast<leafvalue *>(snapshot_v)->value;
        }
      }
    }

    l->checkOrRestart(v, needRestart);
    if (needRestart) {
      count = backup;
      continue;
    } else {
      if (snapshot_n == NULL)
        break;
      else
        l = reinterpret_cast<leafnode *>(snapshot_n);
    }
  }

  free(lv);
  return count;
}

}  // namespace masstree

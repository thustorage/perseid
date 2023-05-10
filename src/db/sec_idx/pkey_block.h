#pragma once

#include <stdint.h>
#include <stddef.h>
#include "pebblesdb/slice.h"
// #include "db/dbformat.h"

#include "persist.h"
#include "debug.h"
#include <libpmem.h>
#include <string>
#include <vector>
#include <set>
#include "clht/clht_lb_res.h"
#include "murmur_hash2.h"

using leveldb::Slice;
// using leveldb::SequenceNumber;


struct PKeyEntry {
  union {
    uint64_t _payload = 0;
    struct {
      uint64_t size : 7;
      uint64_t deleted : 1;
      uint64_t seq : 56;
    };
  };
  char data[0];
};

struct PKeyHeader {
  union {
    uint64_t _payload = 0;
    struct {
      uint64_t prev : 48;
      uint64_t count : 8;
      uint64_t deleted_count : 8;
    };
  };

  PKeyHeader *get_prev() {
    return (PKeyHeader *)(uint64_t)prev;
  }
  
  void print_pkeys() {
    PKeyHeader *ph = this;
    int total_cnt = 0;
    int ph_cnt = 0;
    while (ph) {
      ph_cnt++;
      PKeyEntry *pe = (PKeyEntry *)((char *)ph + sizeof(PKeyHeader));
      for (int i = 0; i < ph->count; i++) {
        std::string s(pe->data, pe->size);
        printf("%d, %d-%d p %p: size %ld deleted %ld seq %ld, val '%s'\n",
               total_cnt++, ph_cnt, i, pe, pe->size, pe->deleted, pe->seq,
               s.c_str());
        pe = (PKeyEntry *)((char *)pe + sizeof(PKeyEntry) + pe->size);
      }
      ph = ph->get_prev();
    }
  }
};

constexpr size_t PKEY_BLOCK_SIZE = 512ul;


struct PKeyBlock;
static inline PKeyBlock *get_pkey_block_addr(void *p) {
  return (PKeyBlock *)((uint64_t)p & ~(PKEY_BLOCK_SIZE - 1));
}

static inline void CollectPKeys(PKeyHeader *head, std::string &buffer,
                                std::vector<int> &offset,
                                std::set<PKeyBlock *> &gc_set, clht *ht) {
  PKeyHeader *ph = head;
  while (ph) {
    gc_set.insert(get_pkey_block_addr(ph));
    char *p = (char *)ph;
    p += sizeof(PKeyHeader);
    for (int k = 0; k < ph->count; ++k) {
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
}

struct PKeyBlock {
  PKeyBlock *prev_block = nullptr;
  volatile int tail = 0;
  // int padding;
  int exclusive = 0;
  char data[0];


  static constexpr int PKEY_BLOCK_DATA_OFFSET = 16;
  static constexpr size_t PKEY_BLOCK_DATA_SIZE =
      PKEY_BLOCK_SIZE - PKEY_BLOCK_DATA_OFFSET;

  void init(PKeyBlock *prev = nullptr) {
    assert(((uint64_t)this & (PKEY_BLOCK_SIZE - 1)) == 0);
    prev_block = prev;
    tail = 0;
    exclusive = 0;
  }

  bool HasSpaceFor(size_t size) {
    return tail + sizeof(PKeyHeader) + size <= PKEY_BLOCK_DATA_SIZE;
  }

  bool HalfFull() { return tail > PKEY_BLOCK_DATA_SIZE / 2; }

  // for now, prev is real pointer. TODO: change to offset
  char *Append(const Slice &pkey, SequenceNumber seq, void *prev) {
    int start_offset;
    int sz = sizeof(PKeyEntry) + sizeof(PKeyHeader) + pkey.size();
    while (true) {
      start_offset = __atomic_load_n(&tail, __ATOMIC_RELAXED);
      int end_offset = start_offset + sz;
      if (end_offset > PKEY_BLOCK_DATA_SIZE) {
        // should split
        return nullptr;
      }
      if (__atomic_compare_exchange_n(&tail, &start_offset, end_offset, true,
                                      __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        break;
      }
    }

    char *ret = data + start_offset;
    char *p = ret;
    PKeyHeader *ph = (PKeyHeader *)p;
    if (prev && get_pkey_block_addr(prev) != this &&
        get_pkey_block_addr(prev) != prev) {
      assert(get_pkey_block_addr(prev)->exclusive);
    }
    ph->prev =
        get_pkey_block_addr(prev) == prev ? (uint64_t) nullptr : (uint64_t)prev;
    ph->count = 1;
    ph->deleted_count = 0;
    p += sizeof(PKeyHeader);
    PKeyEntry *pe = (PKeyEntry *)p;
    pe->size = pkey.size();
    pe->deleted = false;
    pe->seq = seq;
    memcpy(pe->data, pkey.data(), pkey.size());
    clwb_fence(ret, sz);
    return ret;
  }

  char *AppendBatchNoPersist(const Slice &buffer, int count, void *prev) {
    // lock by tree index
    if (tail + sizeof(PKeyHeader) + buffer.size() > PKEY_BLOCK_DATA_SIZE) {
      return nullptr;
    }
    PKeyHeader *ph = (PKeyHeader *)(data + tail);
    if (prev && get_pkey_block_addr(prev) != this &&
        get_pkey_block_addr(prev) != prev) {
      assert(get_pkey_block_addr(prev)->exclusive);
    }
    ph->prev =
        get_pkey_block_addr(prev) == prev ? (uint64_t) nullptr : (uint64_t)prev;
    
    assert(count > 0);
    assert(count < 255);
    ph->count = count;
    ph->deleted_count = 0;
    memcpy(data + tail + sizeof(PKeyHeader), buffer.data(), buffer.size());
    tail += sizeof(PKeyHeader) + buffer.size();
    return (char *)ph;
  }

  // char *AppendBatchNoPersist(const Slice &buffer, int count, void *prev) {
  //   int sz = sizeof(PKeyHeader) + buffer.size();
  //   int start_offset;
  //   while (true) {
  //     start_offset = __atomic_load_n(&tail, __ATOMIC_RELAXED);
  //     int end_offset = start_offset + sz;
  //     if (end_offset > PKEY_BLOCK_DATA_SIZE) {
  //       // should split
  //       return nullptr;
  //     }
  //     if (__atomic_compare_exchange_n(&tail, &start_offset, end_offset, true,
  //                                     __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
  //       break;
  //     }
  //   }

  //   char *ret = data + start_offset;
  //   PKeyHeader *ph = (PKeyHeader *)ret;
  //   ph->prev =
  //       get_pkey_block_addr(prev) == prev ? (uint64_t) nullptr : (uint64_t)prev;

  //   assert(count > 0);
  //   assert(count < 255);
  //   ph->count = count;
  //   ph->deleted_count = 0;
  //   memcpy(ret + sizeof(PKeyHeader), buffer.data(), buffer.size());
  //   return ret;
  // }

  char *MovePKeys(PMemAllocator *allocator, PKeyHeader *head) {
    PKeyHeader *pkh = head;
    std::string buffer;
    buffer.reserve(1024);
    std::vector<int> offset;
    while (pkh) {
      char *p = (char *)pkh;
      p += sizeof(PKeyHeader);
      for (int k = 0; k < pkh->count; ++k) {
        PKeyEntry *pke = (PKeyEntry *)p;
        if (!pke->deleted) {
          offset.push_back(buffer.size());
          buffer.append(p, sizeof(PKeyEntry) + pke->size);
        }
        p += sizeof(PKeyEntry) + pke->size;
      }
      pkh = pkh->get_prev();
    }

    int tail_offset = buffer.size();
    int tail_vec_i = offset.size();

    PKeyBlock *prev_pkb = this;
    PKeyBlock *cur_pkb = this;
    char *prev_pkh = nullptr;

    while (tail_offset > 0) {
      size_t pkb_left_capacity =
          PKEY_BLOCK_DATA_SIZE - cur_pkb->tail;
      int i = 0;
      while (i < tail_vec_i &&
             tail_offset - offset[i] + sizeof(PKeyHeader) > pkb_left_capacity) {
        ++i;
      }
      if (i < tail_vec_i) {
        char *ptr = cur_pkb->AppendBatchNoPersist(
            Slice(buffer.data() + offset[i], tail_offset - offset[i]),
            tail_vec_i - i, prev_pkh);
        assert(ptr);
        prev_pkh = ptr;

        tail_offset = offset[i];
        tail_vec_i = i;
        prev_pkb = cur_pkb;
      }

      if (tail_offset > 0) {
        cur_pkb->Persist();

        cur_pkb = (PKeyBlock *)allocator->Alloc(PKEY_BLOCK_SIZE);
        cur_pkb->init(prev_pkb);
      }
    }

    cur_pkb->Persist();
    return prev_pkh;
  }

  char *AppendMultiBlocks(PMemAllocator *allocator, std::string &buffer,
                          std::vector<int> &offset, void *prev) {
    int tail_offset = buffer.size();
    int tail_vec_i = offset.size();
    PKeyBlock *cur_pb = this;
    char *prev_ph = (char *)prev;

    if (tail_offset > 0 && this->tail > 0 &&
        (this->HalfFull() || !this->HasSpaceFor(tail_offset))) {
      this->Persist();
      cur_pb = (PKeyBlock *)allocator->Alloc(PKEY_BLOCK_SIZE);
      cur_pb->init();
    }

    while (tail_offset > 0) {
      size_t pb_left_capacity = PKEY_BLOCK_DATA_SIZE - cur_pb->tail;
      if (cur_pb != this) {
        assert(pb_left_capacity == PKEY_BLOCK_DATA_SIZE);
      }
      int i = 0;
      while (i < tail_vec_i &&
             tail_offset - offset[i] + sizeof(PKeyHeader) > pb_left_capacity) {
        ++i;
      }
      if (i < tail_vec_i) {
        char *ptr = cur_pb->AppendBatchNoPersist(
            Slice(buffer.data() + offset[i], tail_offset - offset[i]),
            tail_vec_i - i, prev_ph);
        assert(ptr);
        prev_ph = ptr;

        tail_offset = offset[i];
        tail_vec_i = i;
      }
      if (tail_offset > 0) {
        cur_pb->exclusive = true;
        cur_pb->Persist();

        cur_pb = (PKeyBlock *)allocator->Alloc(PKEY_BLOCK_SIZE);
        cur_pb->init();
      }
    }

    // cur_pb->Persist();
    return prev_ph;
  }

  char *CollectPKeysAndAppendMultiBlocks(PMemAllocator *allocator,
                                         PKeyHeader *head, clht *ht) {
    std::string buffer;
    buffer.reserve(1024);
    std::vector<int> offset;
    std::set<PKeyBlock *> gc_set;
    CollectPKeys(head, buffer, offset, gc_set, ht);
    char *p = AppendMultiBlocks(allocator, buffer, offset, nullptr);
    // free old pkey blocks
    for (PKeyBlock *old_pb : gc_set) {
      allocator->Free(old_pb, PKEY_BLOCK_SIZE);
    }
    return p;
  }

  void Persist() {
    pmem_persist(this, PKEY_BLOCK_DATA_OFFSET + tail);
  }

  PKeyBlock(const PKeyBlock &) = delete;
  PKeyBlock &operator=(const PKeyBlock &) = delete;
};

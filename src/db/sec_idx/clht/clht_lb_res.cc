/*
 *   File: clht_lb_res.c
 *   Author: Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>
 *   Description: lock-based cache-line hash table with resizing.
 *   clht_lb_res.c is part of ASCYLIB
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>
 *	      	      Distributed Programming Lab (LPD), EPFL
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 *of this software and associated documentation files (the "Software"), to deal
 *in the Software without restriction, including without limitation the rights
 *to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *copies of the Software, and to permit persons to whom the Software is
 *furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 *all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *SOFTWARE.
 *
 */

#include <malloc.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <functional>
#include <stddef.h>

#include "clht_lb_res.h"

#include "db/sec_idx/seq_cnt.h"

__thread size_t check_ht_status_steps = CLHT_STATUS_INVOK_IN;

#include "assert.h"
#include "stdlib.h"

const char* clht_type_desc() { return "CLHT-LB-RESIZE"; }

inline int is_power_of_two(unsigned int x) {
  return ((x != 0) && !(x & (x - 1)));
}

static inline int is_odd(int x) { return x & 1; }

/** Jenkins' hash function for 64-bit integers. */
inline uint64_t __ac_Jenkins_hash_64(uint64_t key) {
  key += ~(key << 32);
  key ^= (key >> 22);
  key += ~(key << 13);
  key ^= (key >> 8);
  key += (key << 3);
  key ^= (key >> 15);
  key += ~(key << 27);
  key ^= (key >> 31);
  return key;
}

/* Create a new bucket. */
bucket_t* clht_bucket_create() {
  bucket_t* bucket = NULL;
  bucket = (bucket_t*)memalign(CACHE_LINE_SIZE, sizeof(bucket_t));
  if (bucket == NULL) {
    return NULL;
  }

  bucket->lock = 0;

  uint32_t j;
  for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
    bucket->key[j] = 0;
  }
  bucket->next = NULL;

  return bucket;
}

bucket_t* clht_bucket_create_stats(clht_hashtable_t* h, int* resize) {
  bucket_t* b = clht_bucket_create();
  if (IAF_U32(&h->num_expands) == h->num_expands_threshold) {
    /* printf("      -- hit threshold (%u ~ %u)\n", h->num_expands,
     * h->num_expands_threshold); */
    *resize = 1;
  }
  return b;
}

clht_hashtable_t* clht_hashtable_create(uint64_t num_buckets);

clht_t* clht_create(uint64_t num_buckets) {
  clht_t* w = (clht_t*)memalign(CACHE_LINE_SIZE, sizeof(clht_t));
  if (w == NULL) {
    printf("** malloc @ hatshtalbe\n");
    return NULL;
  }

  w->ht = clht_hashtable_create(num_buckets);
  if (w->ht == NULL) {
    free(w);
    return NULL;
  }
  w->resize_lock = LOCK_FREE;
  w->gc_lock = LOCK_FREE;
  w->status_lock = LOCK_FREE;
  w->version_list = NULL;
  w->version_min = 0;
  w->ht_oldest = w->ht;

  return w;
}

clht_hashtable_t* clht_hashtable_create(uint64_t num_buckets) {
  clht_hashtable_t* hashtable = NULL;

  if (num_buckets == 0) {
    return NULL;
  }

  /* Allocate the table itself. */
  hashtable =
      (clht_hashtable_t*)memalign(CACHE_LINE_SIZE, sizeof(clht_hashtable_t));
  if (hashtable == NULL) {
    printf("** malloc @ hatshtalbe\n");
    return NULL;
  }

  /* hashtable->table = calloc(num_buckets, (sizeof(bucket_t))); */
  hashtable->table =
      (bucket_t*)memalign(CACHE_LINE_SIZE, num_buckets * (sizeof(bucket_t)));
  if (hashtable->table == NULL) {
    printf("** alloc: hashtable->table\n");
    fflush(stdout);
    free(hashtable);
    return NULL;
  }

  memset(hashtable->table, 0, num_buckets * (sizeof(bucket_t)));

  uint64_t i;
  for (i = 0; i < num_buckets; i++) {
    hashtable->table[i].lock = LOCK_FREE;
    uint32_t j;
    for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
      hashtable->table[i].key[j] = 0;
    }
  }

  hashtable->num_buckets = num_buckets;
  hashtable->hash = num_buckets - 1;
  hashtable->version = 0;
  hashtable->table_tmp = NULL;
  hashtable->table_new = NULL;
  hashtable->table_prev = NULL;
  hashtable->num_expands = 0;
  hashtable->num_expands_threshold = (CLHT_PERC_EXPANSIONS * num_buckets);
  if (hashtable->num_expands_threshold == 0) {
    hashtable->num_expands_threshold = 1;
  }
  hashtable->is_helper = 1;
  hashtable->helper_done = 0;

  return hashtable;
}

/* Hash a key for a particular hash table. */
uint64_t clht_hash(clht_hashtable_t* hashtable, clht_addr_t key) {
  /* uint64_t hashval; */
  /* return __ac_Jenkins_hash_64(key) & (hashtable->hash); */
  /* return hashval % hashtable->num_buckets; */
  /* return key % hashtable->num_buckets; */
  /* return key & (hashtable->num_buckets - 1); */
  return key & (hashtable->hash);
  // return std::_Hash_bytes(&key, sizeof(clht_addr_t), 0xc70697UL) &
  //        (hashtable->hash);
}

/* Retrieve a key-value entry from a hash table. */
clht_val_t clht_get(clht_hashtable_t* hashtable, clht_addr_t key) {
  size_t bin = clht_hash(hashtable, key);
  CLHT_GC_HT_VERSION_USED(hashtable);
  volatile bucket_t* bucket = hashtable->table + bin;

  uint32_t j;
  do {
    for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
      clht_val_t val = bucket->val[j];
#ifdef __tile__
      _mm_lfence();
#endif
      if (bucket->key[j] == key) {
        if (likely(bucket->val[j] == val)) {
          return val;
        } else {
          if (likely(bucket->key[j] == key)) {
            return bucket->val[j];
          } else {
            return 0;
          }
        }
      }
    }

    bucket = bucket->next;
  } while (unlikely(bucket != NULL));
  return 0;
}

static inline int bucket_exists(volatile bucket_t* bucket, clht_addr_t key) {
  uint32_t j;
  do {
    for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
      if (bucket->key[j] == key) {
        return true;
      }
    }
    bucket = bucket->next;
  } while (unlikely(bucket != NULL));
  return false;
}

/* Insert a key-value entry into a hash table. */
int clht_put(clht_t* h, clht_addr_t key, clht_val_t val) {
  clht_hashtable_t* hashtable = h->ht;
  size_t bin = clht_hash(hashtable, key);
  volatile bucket_t* bucket = hashtable->table + bin;

// #if CLHT_READ_ONLY_FAIL == 1
//   if (bucket_exists(bucket, key)) {
//     return false;
//   }
// #endif

  clht_lock_t* lock = &bucket->lock;
  while (!LOCK_ACQ(lock, hashtable)) {
    hashtable = h->ht;
    size_t bin = clht_hash(hashtable, key);

    bucket = hashtable->table + bin;
    lock = &bucket->lock;
  }

  CLHT_GC_HT_VERSION_USED(hashtable);
  CLHT_CHECK_STATUS(h);
  clht_addr_t* empty = NULL;
  clht_val_t* empty_v = NULL;

  uint32_t j;
  // try update
  do {
    for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
      if (bucket->key[j] == key) {
        bucket->val[j] = val;
        LOCK_RLS(lock);
        return true;
      }
    }
    bucket = bucket->next;
  } while (bucket);

  bucket = hashtable->table + bin;
  do {
    for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
      if (bucket->key[j] == key) {
        LOCK_RLS(lock);
        abort();
        return false;
      } else if (empty == NULL && bucket->key[j] == 0) {
        empty = (clht_addr_t*)&bucket->key[j];
        empty_v = &bucket->val[j];
        break;
      }
    }

    if (empty) {
      *empty_v = val;
#ifdef __tile__
      /* keep the writes in order */
      _mm_sfence();
#endif
      *empty = key;
      LOCK_RLS(lock);
      return true;
    } else if (likely(bucket->next == NULL)) {
      DPP(put_num_failed_expand);
      int resize = 0;
      bucket_t* b = clht_bucket_create_stats(hashtable, &resize);
      b->val[0] = val;
#ifdef __tile__
      /* keep the writes in order */
      _mm_sfence();
#endif
      b->key[0] = key;
#ifdef __tile__
      /* make sure they are visible */
      _mm_sfence();
#endif
      bucket->next = b;
      LOCK_RLS(lock);
      if (unlikely(resize)) {
        /* ht_resize_pes(h, 1); */
        ht_status(h, 1, 0);
      }
      return true;
    }

    bucket = bucket->next;
  } while (true);
}


int clht_set_seqcnt(clht_t* h, clht_addr_t key, clht_val_t val) {
  clht_hashtable_t* hashtable = h->ht;
  size_t bin = clht_hash(hashtable, key);
  volatile bucket_t* bucket = hashtable->table + bin;

// #if CLHT_READ_ONLY_FAIL == 1
//   if (bucket_exists(bucket, key)) {
//     return false;
//   }
// #endif

  clht_lock_t* lock = &bucket->lock;
  while (!LOCK_ACQ(lock, hashtable)) {
    hashtable = h->ht;
    size_t bin = clht_hash(hashtable, key);

    bucket = hashtable->table + bin;
    lock = &bucket->lock;
  }

  CLHT_GC_HT_VERSION_USED(hashtable);
  CLHT_CHECK_STATUS(h);
  clht_addr_t* empty = NULL;
  clht_val_t* empty_v = NULL;

  uint32_t j;
  // try update
  do {
    for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
      if (bucket->key[j] == key) {
        // bucket->val[j] = val;
        ((SeqCnt *)(&bucket->val[j]))->SetNewSeq(val);
        LOCK_RLS(lock);
        return true;
      }
    }
    bucket = bucket->next;
  } while (bucket);

  bucket = hashtable->table + bin;
  do {
    for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
      if (bucket->key[j] == key) {
        LOCK_RLS(lock);
        abort();
        return false;
      } else if (empty == NULL && bucket->key[j] == 0) {
        empty = (clht_addr_t*)&bucket->key[j];
        empty_v = &bucket->val[j];
        break;
      }
    }

    if (empty) {
      SeqCnt entry_val(val, 1);
      *empty_v = entry_val._data;
#ifdef __tile__
      /* keep the writes in order */
      _mm_sfence();
#endif
      *empty = key;
      LOCK_RLS(lock);
      return true;
    } else if (likely(bucket->next == NULL)) {
      DPP(put_num_failed_expand);
      int resize = 0;
      bucket_t* b = clht_bucket_create_stats(hashtable, &resize);
      SeqCnt entry_val(val, 1);
      b->val[0] = entry_val._data;
#ifdef __tile__
      /* keep the writes in order */
      _mm_sfence();
#endif
      b->key[0] = key;
#ifdef __tile__
      /* make sure they are visible */
      _mm_sfence();
#endif
      bucket->next = b;
      LOCK_RLS(lock);
      if (unlikely(resize)) {
        /* ht_resize_pes(h, 1); */
        ht_status(h, 1, 0);
      }
      return true;
    }
    
    bucket = bucket->next;
  } while (true);
}

bool clht_check_seqcnt(clht_hashtable_t* hashtable, clht_addr_t key,
                       clht_val_t val, bool update_if_miss) {
  size_t bin = clht_hash(hashtable, key);
  CLHT_GC_HT_VERSION_USED(hashtable);
  volatile bucket_t* bucket = hashtable->table + bin;

  uint32_t j;
  do {
    for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
      // clht_val_t val = bucket->val[j];
#ifdef __tile__
      _mm_lfence();
#endif
      if (bucket->key[j] == key) {
        SeqCnt *entry_val = (SeqCnt *)&bucket->val[j];
        bool equal = entry_val->seq_ == val;
        if (!equal && update_if_miss) {
          entry_val->cnt_--;
          if (entry_val->cnt_ == 0) {
            // delete this entry
            bucket->key[j] = 0;
          }
        }
        return equal;
      }
    }

    bucket = bucket->next;
  } while (unlikely(bucket != NULL));
  return true;
}

/* Remove a key-value entry from a hash table. */
clht_val_t clht_remove(clht_t* h, clht_addr_t key) {
  clht_hashtable_t* hashtable = h->ht;
  size_t bin = clht_hash(hashtable, key);
  volatile bucket_t* bucket = hashtable->table + bin;

#if CLHT_READ_ONLY_FAIL == 1
  if (!bucket_exists(bucket, key)) {
    return false;
  }
#endif

  clht_lock_t* lock = &bucket->lock;
  while (!LOCK_ACQ(lock, hashtable)) {
    hashtable = h->ht;
    size_t bin = clht_hash(hashtable, key);

    bucket = hashtable->table + bin;
    lock = &bucket->lock;
  }

  CLHT_GC_HT_VERSION_USED(hashtable);
  CLHT_CHECK_STATUS(h);

  uint32_t j;
  do {
    for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
      if (bucket->key[j] == key) {
        clht_val_t val = bucket->val[j];
        bucket->key[j] = 0;
        LOCK_RLS(lock);
        return val;
      }
    }
    bucket = bucket->next;
  } while (unlikely(bucket != NULL));
  LOCK_RLS(lock);
  return false;
}

static uint32_t clht_put_seq(clht_hashtable_t* hashtable, clht_addr_t key,
                             clht_val_t val, uint64_t bin) {
  volatile bucket_t* bucket = hashtable->table + bin;
  uint32_t j;

  do {
    for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
      if (bucket->key[j] == 0) {
        bucket->val[j] = val;
        bucket->key[j] = key;
        return true;
      }
    }

    if (bucket->next == NULL) {
      DPP(put_num_failed_expand);
      int null;
      bucket->next = clht_bucket_create_stats(hashtable, &null);
      bucket->next->val[0] = val;
      bucket->next->key[0] = key;
      return true;
    }

    bucket = bucket->next;
  } while (true);
}

static int bucket_cpy(volatile bucket_t* bucket, clht_hashtable_t* ht_new) {
  if (!LOCK_ACQ_RES(&bucket->lock)) {
    return 0;
  }
  uint32_t j;
  do {
    for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
      clht_addr_t key = bucket->key[j];
      if (key != 0) {
        uint64_t bin = clht_hash(ht_new, key);
        clht_put_seq(ht_new, key, bucket->val[j], bin);
      }
    }
    bucket = bucket->next;
  } while (bucket != NULL);

  return 1;
}

void ht_resize_help(clht_hashtable_t* h) {
  if ((int32_t)FAD_U32((volatile uint32_t*)&h->is_helper) <= 0) {
    return;
  }

  int32_t b;
  /* hash = num_buckets - 1 */
  for (b = h->hash; b >= 0; b--) {
    bucket_t* bu_cur = h->table + b;
    if (!bucket_cpy(
            bu_cur,
            h->table_tmp)) { /* reached a point where the resizer is handling */
      /* printf("[GC-%02d] helped  #buckets: %10zu = %5.1f%%\n",  */
      /* 	 clht_gc_get_id(), h->num_buckets - b, 100.0 * (h->num_buckets
       * - b) / h->num_buckets); */
      break;
    }
  }

  h->helper_done = 1;
}

int ht_resize_pes(clht_t* h, int is_increase, int by) {
  check_ht_status_steps = CLHT_STATUS_INVOK;

  clht_hashtable_t* ht_old = h->ht;

  if (TRYLOCK_ACQ(&h->resize_lock)) {
    return 0;
  }

  size_t num_buckets_new;
  if (is_increase == true) {
    /* num_buckets_new = CLHT_RATIO_DOUBLE * ht_old->num_buckets; */
    num_buckets_new = by * ht_old->num_buckets;
  } else {
#if CLHT_HELP_RESIZE == 1
    ht_old->is_helper = 0;
#endif
    num_buckets_new = ht_old->num_buckets / CLHT_RATIO_HALVE;
  }

  /* printf("// resizing: from %8zu to %8zu buckets\n", ht_old->num_buckets,
   * num_buckets_new); */

  clht_hashtable_t* ht_new = clht_hashtable_create(num_buckets_new);
  ht_new->version = ht_old->version + 1;

#if CLHT_HELP_RESIZE == 1
  ht_old->table_tmp = ht_new;

  int32_t b;
  for (b = 0; b < ht_old->num_buckets; b++) {
    bucket_t* bu_cur = ht_old->table + b;
    if (!bucket_cpy(bu_cur,
                    ht_new)) /* reached a point where the helper is handling */
    {
      break;
    }
  }

  if (is_increase && ht_old->is_helper != 1) /* there exist a helper */
  {
    while (ht_old->helper_done != 1) {
      _mm_pause();
    }
  }

#else

  int32_t b;
  for (b = 0; b < ht_old->num_buckets; b++) {
    bucket_t* bu_cur = ht_old->table + b;
    bucket_cpy(bu_cur, ht_new);
  }
#endif

#if defined(DEBUG)
  /* if (clht_size(ht_old) != clht_size(ht_new)) */
  /*   { */
  /*     printf("**clht_size(ht_old) = %zu != clht_size(ht_new) = %zu\n",
   * clht_size(ht_old), clht_size(ht_new)); */
  /*   } */
#endif

  ht_new->table_prev = ht_old;

  int ht_resize_again = 0;
  if (ht_new->num_expands >= ht_new->num_expands_threshold) {
    /* printf("--problem: have already %u expands\n", ht_new->num_expands); */
    ht_resize_again = 1;
    /* ht_new->num_expands_threshold = ht_new->num_expands + 1; */
  }

  SWAP_U64((uint64_t*)h, (uint64_t)ht_new);
  ht_old->table_new = ht_new;
  TRYLOCK_RLS(h->resize_lock);

#if CLHT_DO_GC == 1
  clht_gc_collect(h);
#else
  // clht_gc_release(ht_old);
  // TODO: need a epoch-based GC
  clht_hashtable_t *old_ht = ht_old;
  while (old_ht) {
    clht_hashtable_t* prev = old_ht->table_prev;
    for (uint64_t bin = 0; bin < old_ht->num_buckets; ++bin) {
      bucket_t* bucket = old_ht->table + bin;
      bucket = (bucket_t*)(bucket->next);
      while (bucket) {
        bucket_t* next_bucket = (bucket_t*)(bucket->next);
        free(bucket);
        bucket = next_bucket;
      }
    }
    free(old_ht->table);
    free(old_ht);
    old_ht = prev;
  }
  ht_new->table_prev = nullptr;
#endif

  if (ht_resize_again) {
    ht_status(h, 1, 0);
  }

  return 1;
}

size_t clht_size(clht_hashtable_t* hashtable) {
  uint64_t num_buckets = hashtable->num_buckets;
  volatile bucket_t* bucket = NULL;
  size_t size = 0;

  uint64_t bin;
  for (bin = 0; bin < num_buckets; bin++) {
    bucket = hashtable->table + bin;

    uint32_t j;
    do {
      for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
        if (bucket->key[j] > 0) {
          size++;
        }
      }

      bucket = bucket->next;
    } while (bucket != NULL);
  }
  return size;
}

size_t ht_status(clht_t* h, int resize_increase, int just_print) {
  if (TRYLOCK_ACQ(&h->status_lock) && !resize_increase) {
    return 0;
  }

  clht_hashtable_t* hashtable = h->ht;
  uint64_t num_buckets = hashtable->num_buckets;
  volatile bucket_t* bucket = NULL;
  size_t size = 0;
  int expands = 0;
  int expands_max = 0;

  uint64_t bin;
  for (bin = 0; bin < num_buckets; bin++) {
    bucket = hashtable->table + bin;

    int expands_cont = -1;
    expands--;
    uint32_t j;
    do {
      expands_cont++;
      expands++;
      for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
        if (bucket->key[j] > 0) {
          size++;
        }
      }

      bucket = bucket->next;
    } while (bucket != NULL);

    if (expands_cont > expands_max) {
      expands_max = expands_cont;
    }
  }

  double full_ratio =
      100.0 * size / ((hashtable->num_buckets) * ENTRIES_PER_BUCKET);

  if (just_print) {
    printf(
        "[STATUS-%02d] #bu: %7zu / #elems: %7zu / full%%: %8.4f%% / expands: "
        "%4d / max expands: %2d\n",
        99, hashtable->num_buckets, size, full_ratio, expands, expands_max);
  } else {
    if (full_ratio > 0 && full_ratio < CLHT_PERC_FULL_HALVE) {
      // printf(
      //     "[STATUS-%02d] #bu: %7zu / #elems: %7zu / full%%: %8.4f%% /
      //     expands: "
      //     "%4d / max expands: %2d\n",
      //     clht_gc_get_id(), hashtable->num_buckets, size, full_ratio,
      //     expands, expands_max);
      ht_resize_pes(h, 0, 33);
    } else if ((full_ratio > 0 && full_ratio > CLHT_PERC_FULL_DOUBLE) ||
               expands_max > CLHT_MAX_EXPANSIONS || resize_increase) {
      int inc_by = (full_ratio / CLHT_OCCUP_AFTER_RES);
      int inc_by_pow2 = pow2roundup(inc_by);

      // printf(
      //     "[STATUS-%02d] #bu: %7zu / #elems: %7zu / full%%: %8.4f%% /
      //     expands: "
      //     "%4d / max expands: %2d\n",
      //     clht_gc_get_id(), hashtable->num_buckets, size, full_ratio,
      //     expands, expands_max);
      if (inc_by_pow2 == 1) {
        inc_by_pow2 = 2;
      }
      ht_resize_pes(h, 1, inc_by_pow2);
    }
  }

  // if (!just_print) {
  //   clht_gc_collect(h);
  // }

  TRYLOCK_RLS(h->status_lock);
  return size;
}

size_t clht_size_mem(clht_hashtable_t* h) /* in bytes */
{
  if (h == NULL) {
    return 0;
  }

  size_t size_tot = sizeof(clht_hashtable_t**);
  size_tot += (h->num_buckets + h->num_expands) * sizeof(bucket_t);
  return size_tot;
}

size_t clht_size_mem_garbage(clht_hashtable_t* h) /* in bytes */
{
  if (h == NULL) {
    return 0;
  }

  size_t size_tot = 0;
  clht_hashtable_t* cur = h->table_prev;
  while (cur != NULL) {
    size_tot += clht_size_mem(cur);
    cur = cur->table_prev;
  }

  return size_tot;
}

void clht_print(clht_hashtable_t* hashtable) {
  uint64_t num_buckets = hashtable->num_buckets;
  volatile bucket_t* bucket;

  printf("Number of buckets: %zu\n", num_buckets);

  uint64_t bin;
  for (bin = 0; bin < num_buckets; bin++) {
    bucket = hashtable->table + bin;

    printf("[[%05zu]] ", bin);

    uint32_t j;
    do {
      for (j = 0; j < ENTRIES_PER_BUCKET; j++) {
        if (bucket->key[j]) {
          printf("(%-5llu/%p)-> ", (long long unsigned int)bucket->key[j],
                 (void*)bucket->val[j]);
        }
      }

      bucket = bucket->next;
      printf(" ** -> ");
    } while (bucket != NULL);
    printf("\n");
  }
  fflush(stdout);
}

void clht_destroy(clht_t *hashtable) {
  clht_hashtable_t *ht = hashtable->ht;
  while (ht) {
    clht_hashtable_t *prev = ht->table_prev;
    for (uint64_t bin = 0; bin < ht->num_buckets; bin++) {
      bucket_t* bucket = ht->table + bin;
      bucket = (bucket_t*)(bucket->next);
      while (bucket) {
        bucket_t* next_bucket = (bucket_t*)(bucket->next);
        free(bucket);
        bucket = next_bucket;
      }
    }
    free(ht->table);
    free(ht);
    ht = prev;
  }
  free(hashtable);
}

#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <sstream>
#include <string>
#include <set>

#include "CCEH/CCEH.h"
#include "FAST_FAIR/ff_btree.h"
#include "clht/clht_lb_res.h"
#include "murmur_hash2.h"
#include "sec_idx.h"
#include "db/dbformat.h"
#include "util/timer.h"

namespace leveldb {

class SecIdxFastFair : public SecIdx {
 public:
  SecIdxFastFair() {
    btree_ = new btree();
    if (leveldb::config::kUsingValidation) {
      cceh_ = new CCEH_NAMESPACE::CCEH("cceh_pool", 1024);
      // vol_cceh_ = new CCEH_NAMESPACE::CCEH("", 1024);
      vol_pri_key_idx_ = clht_create(512 * 1024);
    }
    if (leveldb::config::kParallelGetPDB) {
      for (int i = 0; i < num_parallel_threads; ++i) {
#ifdef USE_NAIVE_PARALLEL_GET
        pdb_func_[i] = std::thread(&SecIdx::SGetPDBFuncNaive, this, i);
#else
        pdb_func_[i] = std::thread(&SecIdx::SGetPDBFunc, this, i);
#endif
      }
    }
  }

  virtual ~SecIdxFastFair() {
    sget_share_.stop_flag = true;
    delete btree_;
    if (leveldb::config::kUsingValidation) {
      delete cceh_;
      // delete vol_cceh_;
      clht_destroy(vol_pri_key_idx_);
    }
    for (int i = 0; i < num_parallel_threads; ++i) {
      if (pdb_func_[i].joinable()) {
        pdb_func_[i].join();
      }
    }
  }

  virtual Status SInsert(const Slice &skey, const Slice &pkey,
                         SequenceNumber seq) {
    btree_->btree_insert(skey, pkey, seq, vol_pri_key_idx_);
    if (leveldb::config::kUsingValidation) {
      uint64_t pkey_hash = pkey.size() <= sizeof(uint64_t)
                               ? *(uint64_t *)pkey.data()
                               : MurmurHash64A(pkey.data(), pkey.size());
      bool new_item = cceh_->SetSeqCnt(pkey_hash, seq);
      if (!new_item) {
        // vol_cceh_->SetSeqCnt(pkey_hash, seq);
        clht_set_seqcnt(vol_pri_key_idx_, pkey_hash, seq);
      }
    }
    return Status::OK();
  }

  virtual Status SGet(const ReadOptions &options, const Slice &skey,
                      std::vector<KeyValuePair> *value_list, DB *pri_db) {
    int num_rec = options.num_records;
    PKeyHeader *ph = (PKeyHeader *)btree_->btree_search(skey);

    while (ph && (PKeyBlock *)ph != get_pkey_block_addr(ph) && num_rec) {
      int cnt = ph->count;
      PKeyEntry *pke = (PKeyEntry *)((char *)ph + sizeof(PKeyHeader));
      int deleted_cnt = 0;
      while (cnt-- && num_rec) {
        size_t sz = pke->size;
        if (!pke->deleted) {
          SequenceNumber seq = -1;
          SequenceNumber pke_seq = pke->seq;
          std::string pkey(pke->data, sz);
          // // debug
          // int pos = pkey.find(skey.data());
          // assert(pos + skey.size() == pkey.size());
          std::string pValue;
          bool check_skip = false;
          if (leveldb::config::kUsingValidation) {
            StopWatch<uint64_t> t((uint64_t &)options.time_validate);

            uint64_t pkey_hash = pkey.size() <= sizeof(uint64_t)
                                     ? *(uint64_t *)pkey.data()
                                     : MurmurHash64A(pkey.data(), pkey.size());
            bool equal = clht_check_seqcnt(vol_pri_key_idx_->ht, pkey_hash,
                                           pke_seq, true);
            if (equal) {
              seq = pke_seq;
            } else {
              check_skip = true;
              pke->deleted = true;
              ++deleted_cnt;
            }
          }

          if (!check_skip) {
            Status s;
            {
              StopWatch<uint64_t> t((uint64_t &)options.time_pdb);
              s = pri_db->Get(options, pkey, &pValue, &seq);
            }
            if (s.ok() && !s.IsNotFound()) {
              if (seq == pke_seq) {
                value_list->emplace_back(pkey, pValue);
                --num_rec;
              } else {
                pke->deleted = true;
                ++deleted_cnt;
              }
            }
          }
        }
        pke = (PKeyEntry *)((char *)pke + sizeof(PKeyEntry) + sz);
      }
      ph->deleted_count += deleted_cnt;
      ph = ph->get_prev();
    }

    return Status::OK();
  }

#ifdef USE_NAIVE_PARALLEL_GET

#else
// parallel sget
  virtual Status ParallelSGet(const ReadOptions &options, const Slice &skey,
                              std::vector<KeyValuePair> *value_list,
                              DB *pri_db) {
    int num_rec = options.num_records;

    PKeyHeader *ph = (PKeyHeader *)btree_->btree_search(skey);
    std::set<std::string> result_keys_found;

    std::vector<KeyValuePair> local_value_list[num_parallel_threads];
    for (int i = 0; i < num_parallel_threads; ++i) {
      sget_local_[i].options = options;
      sget_local_[i].options.ClearStats();
      local_value_list[i].reserve(num_rec);
      sget_local_[i].value_list = &local_value_list[i];
      sget_local_[i].finish_flag.store(false, std::memory_order_relaxed);
    }

    sget_share_.pkey_vec.resize(num_rec);
    sget_share_.pri_db = pri_db;
    sget_share_.end_flag.store(false, std::memory_order_relaxed);
    sget_share_.head.store(0, std::memory_order_relaxed);
    sget_share_.tail.store(0, std::memory_order_relaxed);
    sget_share_.th_id.resize(num_rec, -1);

    sget_share_.g_epoch.fetch_add(1, std::memory_order_release);

    int total_cnt = 0;
    while (ph && num_rec) {
      int cnt = ph->count;
      PKeyEntry *pke = (PKeyEntry *)((char *)ph + sizeof(PKeyHeader));
      int deleted_cnt = 0;
      while (cnt-- && num_rec) {
        size_t sz = pke->size;
        if (!pke->deleted) {
          std::string pkey(pke->data, sz);
          if (!result_keys_found.contains(pkey)) {
            SequenceNumber pke_seq = pke->seq;
            bool check_skip = false;
            if (leveldb::config::kUsingValidation) {
              StopWatch<uint64_t> t((uint64_t &)options.time_validate);

              uint64_t pkey_hash =
                  pkey.size() <= sizeof(uint64_t)
                      ? *(uint64_t *)pkey.data()
                      : MurmurHash64A(pkey.data(), pkey.size());
              bool equal = clht_check_seqcnt(vol_pri_key_idx_->ht, pkey_hash,
                                             pke_seq, true);
              if (!equal) {
                check_skip = true;
                pke->deleted = true;
                ++deleted_cnt;
              }
            }

            if (!check_skip) {
              sget_share_.pkey_vec[sget_share_.head] = PKeyItem(pke_seq, pkey);
              ++total_cnt;
              sget_share_.head.store(total_cnt, std::memory_order_release);
              --num_rec;
              result_keys_found.insert(pkey);
            }
          }
        }
        pke = (PKeyEntry *)((char *)pke + sizeof(PKeyEntry) + sz);
      }
      ph->deleted_count += deleted_cnt;
      ph = ph->get_prev();
    }
    sget_share_.end_flag.store(true, std::memory_order_release);

    int offset[num_parallel_threads];
    for (int i = 0; i < num_parallel_threads; ++i) {
      offset[i] = 0;
      while (!sget_local_[i].finish_flag.load(std::memory_order_relaxed))
        ;
      ((ReadOptions &)options).MergeStats(sget_local_[i].options);
    }

    for (int i = 0; i < total_cnt; ++i) {
      int tid = sget_share_.th_id[i];
      if (tid != -1) {
        value_list->emplace_back(local_value_list[tid][offset[tid]++]);
      }
    }

    return Status::OK();
  }

  void SGetPDBFunc(int tid) {
    SGetLocal &local = sget_local_[tid];
    int consume_epoch = 0;
    while (!sget_share_.stop_flag.load(std::memory_order_relaxed)) {
      while (consume_epoch ==
             sget_share_.g_epoch.load(std::memory_order_relaxed)) {
        if (sget_share_.stop_flag.load(std::memory_order_relaxed)) {
          return;
        }
      }

      DB *pri_db = sget_share_.pri_db;
      while (true) {
        int tail;
        {
          std::lock_guard<SpinLock> lk(sget_share_.spin_lock);
          bool end_flag = sget_share_.end_flag.load(std::memory_order_relaxed);
          tail = sget_share_.tail.load(std::memory_order_relaxed);
          if (tail < sget_share_.head) {
            sget_share_.tail.store(tail + 1, std::memory_order_relaxed);
          } else if (end_flag) {
            break;
          } else {
            continue;
          }
        }
        std::string pValue;
        std::string pkey = sget_share_.pkey_vec[tail].pkey;
        SequenceNumber cur_seq = sget_share_.pkey_vec[tail].seq;
        SequenceNumber seq = cur_seq;
        Status s;
        {
          StopWatch<uint64_t> t(local.options.time_pdb);
          s = pri_db->Get(local.options, pkey, &pValue, &seq);
        }
        if (s.ok() && !s.IsNotFound()) {
          if (seq == cur_seq) {
            local.value_list->emplace_back(pkey, pValue);
            sget_share_.th_id[tail] = tid;
          } else {
            sget_share_.th_id[tail] = -1;
          }
        } else {
          sget_share_.th_id[tail] = -1;
        }
      }

      ++consume_epoch;
      local.finish_flag.store(true, std::memory_order_relaxed);
    }
  }
#endif

  virtual Status SGetPKeyOnly(const ReadOptions &options, const Slice &skey,
                              std::vector<std::string> *pkey_list) {
    int num_rec = options.num_records;
    PKeyHeader *ph = (PKeyHeader *)btree_->btree_search(skey);

    while (ph && (PKeyBlock *)ph != get_pkey_block_addr(ph) && num_rec) {
      int cnt = ph->count;
      PKeyEntry *pke = (PKeyEntry *)((char *)ph + sizeof(PKeyHeader));
      int deleted_cnt = 0;
      while (cnt-- && num_rec) {
        size_t sz = pke->size;
        if (!pke->deleted) {
          std::string pkey(pke->data, sz);
          SequenceNumber pke_seq = pke->seq;

          bool check_skip = false;
          if (leveldb::config::kUsingValidation) {
            // StopWatch<uint64_t> t((uint64_t &)options.time_validate);

            uint64_t pkey_hash = pkey.size() <= sizeof(uint64_t)
                                     ? *(uint64_t *)pkey.data()
                                     : MurmurHash64A(pkey.data(), pkey.size());
            bool equal = clht_check_seqcnt(vol_pri_key_idx_->ht, pkey_hash,
                                           pke_seq, true);
            if (!equal) {
              check_skip = true;
              pke->deleted = true;
              ++deleted_cnt;
            }
          }

          if (!check_skip) {
            pkey_list->emplace_back(pkey);
            num_rec--;
          }
        }
        pke = (PKeyEntry *)((char *)pke + sizeof(PKeyEntry) + sz);
      }
      ph->deleted_count += deleted_cnt;
      ph = ph->get_prev();
    }
    return Status::OK();
  }

  virtual Status SScanPKeyOnly(const ReadOptions &options, const Slice &skey,
                               std::vector<std::string> *pkey_list) {
    int num_skey = options.num_records;
    // uint64_t ptr[num_skey];
    std::vector<uint64_t> ptr;
    ptr.clear();
    btree_->btree_search_range(skey, num_skey, ptr);
    for (int i = 0; i < ptr.size(); i++) {
      PKeyHeader *ph = (PKeyHeader *)ptr[i];
      int pkey_cnt = options.num_scan_pkey_cnt;
      while (ph && (PKeyBlock *)ph != get_pkey_block_addr(ph) && pkey_cnt) {
        int cur_cnt = ph->count;
        PKeyEntry *pke = (PKeyEntry *)((char *)ph + sizeof(PKeyHeader));
        int deleted_cnt = 0;
        while (cur_cnt-- && pkey_cnt) {
          size_t sz = pke->size;
          if (!pke->deleted) {
            std::string pkey(pke->data, sz);
            SequenceNumber pke_seq = pke->seq;

            bool check_skip = false;
            if (leveldb::config::kUsingValidation) {
              // StopWatch<uint64_t> t((uint64_t &)options.time_validate);
              uint64_t pkey_hash =
                  pkey.size() <= sizeof(uint64_t)
                      ? *(uint64_t *)pkey.data()
                      : MurmurHash64A(pkey.data(), pkey.size());
              bool equal = clht_check_seqcnt(vol_pri_key_idx_->ht, pkey_hash,
                                             pke_seq, true);
              if (!equal) {
                check_skip = true;
                // pke->deleted = true;
                ++deleted_cnt;
              }
            }

            if (!check_skip) {
              pkey_list->emplace_back(pkey);
              pkey_cnt--;
            }
          }
          pke = (PKeyEntry *)((char *)pke + sizeof(PKeyEntry) + sz);
        }
        // ph->deleted_count += deleted_cnt;
        ph = ph->get_prev();
      }
    }
    return Status::OK();
  }

  virtual Status RecoverTest() {
    clht_destroy(vol_pri_key_idx_);
    vol_pri_key_idx_ = nullptr;

    uint64_t recovery_time = 0;
    {
      StopWatch<uint64_t> sw(recovery_time);
      vol_pri_key_idx_ = clht_create(512 * 1024);
      CCEH_NAMESPACE::Segment *cur_seg = nullptr;
      for (unsigned i = 0; i < cceh_->dir->capacity; ++i) {
        if (cceh_->dir->_[i] != cur_seg) {
          cur_seg = cceh_->dir->_[i];

          for (unsigned k = 0; k < cur_seg->kNumSlot; ++k) {
            auto _key = cur_seg->_[k].key;
            if (_key != CCEH_NAMESPACE::INVALID &&
                _key != CCEH_NAMESPACE::SENTINEL) {
              SeqCnt entry_val;
              entry_val._data = cur_seg->_[k].value;
              if (entry_val.cnt_ > 1) {
                clht_set_seqcnt(vol_pri_key_idx_, _key, entry_val.seq_);
              }
            }
          }
        }
      }
    }

    printf("recovery time: %lu us\n", recovery_time / 1000);
    return Status::OK();
  }

 private:
  btree *btree_ = nullptr;

  CCEH_NAMESPACE::CCEH *cceh_ = nullptr;
  // CCEH_NAMESPACE::CCEH *vol_cceh_;
  clht *vol_pri_key_idx_ = nullptr;
};

} // namespace leveldb

#pragma once

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <string>
#include <set>
#include <sstream>

#include "sec_idx.h"
#include "murmur_hash2.h"
#include "util/timer.h"
#include "P-Masstree-log/masstree.h"
#include "CCEH/CCEH.h"
#include "clht/clht_lb_res.h"

namespace leveldb {

class SecIdxPMasstreeLog : public SecIdx {
 public:
  SecIdxPMasstreeLog() {
    mt_ = new masstree::masstree();
    mt_ti_ = mt_->getpThreadInfo();
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

  virtual ~SecIdxPMasstreeLog() {
    sget_share_.stop_flag = true;
    delete mt_;
    delete mt_ti_;
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
    mt_->put(skey, pkey, seq, *mt_ti_);
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
    PKeyEntry *pe = (PKeyEntry *)mt_->get(skey, *mt_ti_);

    while (pe && num_rec) {
      if (!pe->deleted) {
        SequenceNumber seq = -1;
        SequenceNumber pe_seq = pe->seq;
        std::string pkey(pe->data, pe->pkey_size);
        std::string value;
        bool check_skip = false;
        if (leveldb::config::kUsingValidation) {
          StopWatch<uint64_t> t((uint64_t &)options.time_validate);
          uint64_t pkey_hash = pkey.size() <= sizeof(uint64_t)
                                   ? *(uint64_t *)pkey.data()
                                   : MurmurHash64A(pkey.data(), pkey.size());
          bool equal =
              clht_check_seqcnt(vol_pri_key_idx_->ht, pkey_hash, pe_seq);
          if (equal) {
            seq = pe_seq;
          } else {
            check_skip = true;
            // pe->deleted = true;
          }
        }

        if (!check_skip) {
          Status s;
          {
            StopWatch<uint64_t> t((uint64_t &)options.time_pdb);
            s = pri_db->Get(options, pkey, &value, &seq);
          }
          if (s.ok() && !s.IsNotFound()) {
            if (seq == pe_seq) {
              value_list->emplace_back(pkey, value);
              --num_rec;
            } else {
              // pe->deleted = true;
            }
          }
        }
      }
      pe = (PKeyEntry *)(uint64_t)pe->prev;
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

    PKeyEntry *pe = (PKeyEntry *)mt_->get(skey, *mt_ti_);
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
    while (pe && num_rec) {
      if (!pe->deleted) {
        std::string pkey(pe->data, pe->pkey_size);
        SequenceNumber pe_seq = pe->seq;
        bool check_skip = false;
        if (leveldb::config::kUsingValidation) {
          StopWatch<uint64_t> t((uint64_t &)options.time_validate);

          uint64_t pkey_hash = pkey.size() <= sizeof(uint64_t)
                                   ? *(uint64_t *)pkey.data()
                                   : MurmurHash64A(pkey.data(), pkey.size());
          bool equal =
              clht_check_seqcnt(vol_pri_key_idx_->ht, pkey_hash, pe_seq);
          if (!equal) {
            check_skip = true;
            // pe->deleted = true;
          }
        }

        if (!check_skip) {
          sget_share_.pkey_vec[sget_share_.head] = PKeyItem(pe_seq, pkey);
          ++total_cnt;
          sget_share_.head.store(total_cnt, std::memory_order_release);
          --num_rec;
        }
      }
      pe = (PKeyEntry *)(uint64_t)pe->prev;
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
    PKeyEntry *pe = (PKeyEntry *)mt_->get(skey, *mt_ti_);
    while (pe && num_rec) {
      if (!pe->deleted) {
        std::string pkey(pe->data, pe->pkey_size);
        SequenceNumber pe_seq = pe->seq;
        bool check_skip = false;
        if (leveldb::config::kUsingValidation) {
          // StopWatch<uint64_t> t((uint64_t &)options.time_validate);
          uint64_t pkey_hash = pkey.size() <= sizeof(uint64_t)
                                   ? *(uint64_t *)pkey.data()
                                   : MurmurHash64A(pkey.data(), pkey.size());
          bool equal =
              clht_check_seqcnt(vol_pri_key_idx_->ht, pkey_hash, pe_seq);
          if (!equal) {
            check_skip = true;
            // pe->deleted = true;
          }
        }
        if (!check_skip) {
          pkey_list->emplace_back(pkey);
          num_rec--;
        }
      }
      pe = (PKeyEntry *)(uint64_t)pe->prev;
    }
    return Status::OK();
  }

  virtual Status SScanPKeyOnly(const ReadOptions &options, const Slice &skey,
                               std::vector<std::string> *pkey_list) {
    int num_skey = options.num_records;
    uint64_t ptr[num_skey];
    memset(ptr, 0, sizeof(uint64_t) * num_skey);
    int ret_cnt = mt_->scan(skey, num_skey, ptr, *mt_ti_);
    for (int i = 0; i < ret_cnt; i++) {
      PKeyEntry *pe = (PKeyEntry *)ptr[i];
      int pkey_cnt = options.num_scan_pkey_cnt;
      while (pe && pkey_cnt) {
        if (!pe->deleted) {
          std::string pkey(pe->data, pe->pkey_size);
          SequenceNumber pe_seq = pe->seq;
          bool check_skip = false;
          if (leveldb::config::kUsingValidation) {
            // StopWatch<uint64_t> t((uint64_t &)options.time_validate);
            uint64_t pkey_hash = pkey.size() <= sizeof(uint64_t)
                                     ? *(uint64_t *)pkey.data()
                                     : MurmurHash64A(pkey.data(), pkey.size());
            bool equal =
                clht_check_seqcnt(vol_pri_key_idx_->ht, pkey_hash, pe_seq);
            if (!equal) {
              check_skip = true;
              // pe->deleted = true;
            }
          }

          if (!check_skip) {
            pkey_list->emplace_back(pkey);
            pkey_cnt--;
          }
        }
        pe = (PKeyEntry *)(uint64_t)pe->prev;
      }
    }
    return Status::OK();
  }

 private:
  masstree::masstree *mt_ = nullptr;
  MASS::ThreadInfo *mt_ti_ = nullptr;

  CCEH_NAMESPACE::CCEH *cceh_ = nullptr;
  // CCEH_NAMESPACE::CCEH *vol_cceh_;
  clht *vol_pri_key_idx_ = nullptr;
};

} // namespace leveldb

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
#include "P-Masstree-composite/masstree.h"
#include "CCEH/CCEH.h"
#include "clht/clht_lb_res.h"
#include "db/dbformat.h"

namespace leveldb {

class SecIdxPMasstreeComposite : public SecIdx {
 public:
  SecIdxPMasstreeComposite() {
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

  virtual ~SecIdxPMasstreeComposite() {
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
    std::string sk = skey.ToString();
    sk.append(pkey.data(), pkey.size());
    mt_->put(sk, seq, *mt_ti_);
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
    std::string sk = skey.ToString();
    sk.append(8, 0);
    std::string sk_max = skey.ToString();
    sk_max.append(8, -1);
    std::vector<std::string> pkey_list;
    std::vector<uint64_t> seq_list;
    mt_->scan(sk, sk_max, num_rec, &pkey_list, &seq_list, *mt_ti_,
              (uint64_t &)options.time_validate, vol_pri_key_idx_);
    for (int i = 0; i < pkey_list.size(); ++i) {
      Status s;
      std::string value;
      SequenceNumber seq = seq_list[i];
      {
        StopWatch<uint64_t> t((uint64_t &)options.time_pdb);
        s = pri_db->Get(options, pkey_list[i], &value, &seq);
      }
      if (s.ok() && !s.IsNotFound()) {
        if (seq == seq_list[i]) {
          value_list->emplace_back(pkey_list[i], value);
        }
      }
    }
    return Status::OK();
  }

#ifdef USE_NAIVE_PARALLEL_GET
  virtual Status ParallelSGet(const ReadOptions &options, const Slice &skey,
                              std::vector<KeyValuePair> *value_list,
                              DB *pri_db) {
    int num_rec = options.num_records;
    std::string sk = skey.ToString();
    sk.append(8, 0);
    std::string sk_max = skey.ToString();
    sk_max.append(8, -1);
    std::vector<std::string> pkey_list;
    std::vector<uint64_t> seq_list;
    mt_->scan(sk, sk_max, num_rec, &pkey_list, &seq_list, *mt_ti_,
              (uint64_t &)options.time_validate, vol_pri_key_idx_);

    std::set<std::string> result_keys_found;

    std::vector<KeyValuePair> local_value_list[num_parallel_threads];
    for (int i = 0; i < num_parallel_threads; ++i) {
      sget_local_naive_[i].options = options;
      sget_local_naive_[i].options.ClearStats();
      sget_local_naive_[i].head = 0;
      sget_local_naive_[i].finish_flag.store(false, std::memory_order_relaxed);
      sget_local_naive_[i].value_list = &local_value_list[i];
      sget_local_naive_[i].pkey_vec.resize(num_rec);
    }

    sget_share_.pkey_vec.resize(num_rec);
    sget_share_.pri_db = pri_db;
    sget_share_.end_flag.store(false, std::memory_order_relaxed);
    sget_share_.g_epoch.fetch_add(1, std::memory_order_release);

    int cur_parallel_thread = 0;
    int total_cnt = 0;

    for (int i = 0; i < pkey_list.size(); i++) {
      if (!result_keys_found.contains(pkey_list[i])) {
        SequenceNumber seq = seq_list[i];
        SGetLocalNaive &cur_local = sget_local_naive_[cur_parallel_thread];
        cur_local.pkey_vec[cur_local.head] = PKeyItem(seq, pkey_list[i]);
        cur_local.head.fetch_add(1, std::memory_order_release);
        result_keys_found.insert(pkey_list[i]);
        ++total_cnt;
        cur_parallel_thread = (cur_parallel_thread + 1) % num_parallel_threads;
      }
    }

    sget_share_.end_flag.store(true, std::memory_order_release);

    for (int i = 0; i < num_parallel_threads; ++i) {
      while (!sget_local_naive_[i].finish_flag.load(std::memory_order_relaxed))
        ;
      ((ReadOptions &)options).MergeStats(sget_local_naive_[i].options);
    }

    for (int i = 0; i < total_cnt; ++i) {
      KeyValuePair &kvp =
          local_value_list[i % num_parallel_threads][i / num_parallel_threads];
      if (!kvp.key.empty()) {
        value_list->emplace_back(kvp);
      }
    }

    return Status::OK();
  }

  virtual void SGetPDBFuncNaive(int tid) {
    SGetLocalNaive &local = sget_local_naive_[tid];
    int consume_epoch = 0;
    while (!sget_share_.stop_flag.load(std::memory_order_relaxed)) {
      while (consume_epoch ==
             sget_share_.g_epoch.load(std::memory_order_relaxed)) {
        if (sget_share_.stop_flag.load(std::memory_order_relaxed)) {
          return;
        }
      }

      DB *pri_db = sget_share_.pri_db;
      int head, tail = 0;
      while (!sget_share_.end_flag.load(std::memory_order_acquire) ||
             tail < local.head.load(std::memory_order_relaxed)) {
        head = local.head.load(std::memory_order_relaxed);
        while (tail < head) {
          std::string pValue;
          std::string pkey = local.pkey_vec[tail].pkey;
          SequenceNumber cur_seq = local.pkey_vec[tail].seq;
          SequenceNumber seq = cur_seq;
          Status s;
          {
            StopWatch<uint64_t> t(local.options.time_pdb);
            s = pri_db->Get(local.options, pkey, &pValue, &seq);
          }
          if (s.ok() && !s.IsNotFound()) {
            if (seq == cur_seq) {
              local.value_list->emplace_back(pkey, pValue);
            } else {
              local.value_list->emplace_back("", "");
            }
          } else {
            local.value_list->emplace_back("", "");
          }
          ++tail;
        }
      }

      ++consume_epoch;
      local.finish_flag.store(true, std::memory_order_relaxed);
    }
  }

#else
// parallel sget
  virtual Status ParallelSGet(const ReadOptions &options, const Slice &skey,
                              std::vector<KeyValuePair> *value_list,
                              DB *pri_db) {
    int num_rec = options.num_records;
    std::string sk = skey.ToString();
    sk.append(8, 0);
    std::string sk_max = skey.ToString();
    sk_max.append(8, -1);
    std::vector<std::string> pkey_list;
    std::vector<uint64_t> seq_list;
    mt_->scan(sk, sk_max, num_rec, &pkey_list, &seq_list, *mt_ti_,
              (uint64_t &)options.time_validate, vol_pri_key_idx_);

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
    for (int i = 0; i < pkey_list.size(); ++i) {
      if (!result_keys_found.contains(pkey_list[i])) {
        sget_share_.pkey_vec[sget_share_.head] =
            PKeyItem(seq_list[i], pkey_list[i]);
        ++total_cnt;
        sget_share_.head.store(total_cnt, std::memory_order_release);
        result_keys_found.insert(pkey_list[i]);
      }
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
    std::string sk = skey.ToString();
    sk.append(8, 0);
    std::string sk_max = skey.ToString();
    sk_max.append(8, -1);
    std::vector<uint64_t> seq_list;
    mt_->scan(sk, sk_max, num_rec, pkey_list, &seq_list, *mt_ti_,
              (uint64_t &)options.time_validate, vol_pri_key_idx_);
    return Status::OK();
  }

  virtual Status SScanPKeyOnly(const ReadOptions &options, const Slice &skey,
                               std::vector<std::string> *pkey_list) {
    int num_rec = options.num_records;
    std::string sk = skey.ToString();
    for (int i = 0; i < num_rec; ++i) {
      std::string cur_sk = sk;
      cur_sk.append(8, 0);
      std::string sk_max(skey.size() + 8, -1);
      std::vector<std::string> pkeys;
      std::vector<uint64_t> seq_list;
      mt_->scan(sk, sk_max, options.num_scan_pkey_cnt, &pkeys, &seq_list,
                *mt_ti_, (uint64_t &)options.time_validate, vol_pri_key_idx_);
      if (pkeys.size() > 0) {
        sk = pkeys[0].substr(0, skey.size());
        sk.back() += 1;
      }
      pkey_list->insert(pkey_list->end(),
                        std::make_move_iterator(pkeys.begin()),
                        std::make_move_iterator(pkeys.end()));
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
  masstree::masstree *mt_ = nullptr;
  MASS::ThreadInfo *mt_ti_ = nullptr;

  CCEH_NAMESPACE::CCEH *cceh_ = nullptr;
  // CCEH_NAMESPACE::CCEH *vol_cceh_;
  clht *vol_pri_key_idx_ = nullptr;
};

} // namespace leveldb

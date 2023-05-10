#pragma once

#include "pebblesdb/db.h"
#include "pebblesdb/status.h"
// #include "db/dbformat.h"
#include "sec_idx_config.h"
#include "pmem_arena.h"
#include "seq_cnt.h"

#include <atomic>
#include <vector>
#include <string>
#include <thread>

namespace leveldb {

// #define USE_NAIVE_PARALLEL_GET

class SecIdx {
 public:
  SecIdx() = default;
  virtual ~SecIdx() {}

  virtual Status SInsert(const Slice &skey, const Slice &pkey,
                         SequenceNumber seq) = 0;
  virtual Status SGet(const ReadOptions &options, const Slice &skey,
                      std::vector<KeyValuePair> *value_list, DB *pri_db) = 0;
  virtual Status ParallelSGet(const ReadOptions &options, const Slice &skey,
                              std::vector<KeyValuePair> *value_list,
                              DB *pri_db) {
    ERROR_EXIT("not implemented.");
  }
#ifdef USE_NAIVE_PARALLEL_GET
  virtual void SGetPDBFuncNaive(int tid) { ERROR_EXIT("not implemented."); }
#else
  virtual void SGetPDBFunc(int tid) { ERROR_EXIT("not implemented."); }
#endif
  virtual Status SGetPKeyOnly(const ReadOptions &options, const Slice &skey,
                              std::vector<std::string> *pkey_list) {
    ERROR_EXIT("not implemented.");
  }
  virtual Status SScanPKeyOnly(const ReadOptions &options, const Slice &skey,
                               std::vector<std::string> *pkey_list) {
    ERROR_EXIT("not implemented.");
  }
  virtual Status RecoverTest() { ERROR_EXIT("not implemented."); }

 protected:
  static const int num_parallel_threads = leveldb::config::kNumParallelThreads;
  SGetShare sget_share_;
  SGetLocal sget_local_[num_parallel_threads];
  std::thread pdb_func_[num_parallel_threads];
#ifdef USE_NAIVE_PARALLEL_GET
  SGetLocalNaive sget_local_naive_[num_parallel_threads];
#endif
};

} // namespace leveldb

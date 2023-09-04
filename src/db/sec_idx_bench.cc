#define STRIP_FLAG_HELP 1    // this must go before the #include!
#include "gflags/gflags.h"
#include "pebblesdb/db.h"
#include "pebblesdb/slice.h"
#include "pebblesdb/filter_policy.h"
#include "pebblesdb/cache.h"
#include "db/sec_idx/sec_idx_config.h"
#include "db/sec_idx/debug.h"
#include "util/timer.h"

#include <fstream>
#include <iostream>
#include <unistd.h>
#include <sstream>
#include <cassert>
#include <algorithm>
#include <thread>
#include <atomic>

DEFINE_string(db, "/mnt/lsm_eval/pebblesdb", "path to database");
DEFINE_bool(use_existing_db, false, "use_existing_db");
DEFINE_string(topk, "10", "list of secondary query topk");
DEFINE_string(preload, "", "preload workloads");
DEFINE_string(trace, "", "running workload trace");
DEFINE_int64(log_point, 1000000, "interval of printing results");
DEFINE_int32(threads, 1, "number of threads");
DEFINE_bool(range_search, false, "range search");

char content[4096];
// uint64_t MAX_OP = 1000000;

struct SharedThreadState {
  std::atomic_int_fast32_t cur_count_{0};
  std::atomic_int_fast32_t gen_{0};

  void Barrier(int total_count) {
    int local_gen = gen_.load(std::memory_order_acquire);
    if (++cur_count_ == total_count) {
      // the last one
      gen_.fetch_add(1, std::memory_order_release);
      cur_count_.store(0, std::memory_order_relaxed);
    } else {
      // wait for barrier
      while (gen_.load(std::memory_order_consume) == local_gen)
        ;
    }
  }
};

struct alignas(64) Stats {
  uint64_t w = 0;   // write
  uint64_t rs = 0;  // read secondary key
  uint64_t rp = 0;  // read primary key
  uint64_t rscount = 0;
  double tp = 0;
  double durationW = 0;
  double durationRS = 0;
  double durationRP = 0;
  double durationPDB_RS = 0;
  double duration_validate = 0;
  double duration_pri_file_io = 0;
  uint64_t num_pri_probe = 0;  // number of read from pdb's memtable and sstable

  void clear() {
    w = 0;
    rs = 0;
    rp = 0;
    rscount = 0;
    tp = 0;
    durationW = 0;
    durationRS = 0;
    durationRP = 0;
    durationPDB_RS = 0;
    duration_validate = 0;
    duration_pri_file_io = 0;
    num_pri_probe = 0;
  }

  void merge(const Stats &other) {
    w += other.w;
    rs += other.rs;
    rp += other.rp;
    rscount += other.rscount;
    tp += other.tp;
    durationW += other.durationW;
    durationRS += other.durationRS;
    durationRP += other.durationRP;
    durationPDB_RS += other.durationPDB_RS;
    duration_validate += other.duration_validate;
    duration_pri_file_io += other.duration_pri_file_io;
    num_pri_probe += other.num_pri_probe;
  }

  void print_header() {
    std::cout
        << "No of Op (Millions), lat_op, lat_put, lat_get_pri, lat_get_sec, "
           "lat_pri_table, lat_validate, avg_sec_topK, num_pri_probe"
        << std::endl;
  }

  void print() {
    double total_time = durationW + durationRP + durationRS;
    uint64_t total_op = w + rp + rs;
    std::cout << total_op / 1e6 << ",\t";
    std::cout << std::fixed;
    std::cout.precision(3);
    std::cout << total_time / total_op / 1000 << ",\t" << durationW / w / 1000
              << ",\t" << durationRP / rp / 1000 << ",\t"
              << durationRS / rs / 1000 << ",\t" << durationPDB_RS / rs / 1000
              << ",\t"
              // << duration_pri_file_io / rs / 1000 << ",\t"
              << duration_validate / rs / 1000 << ",\t" << (double)rscount / rs
              << ",\t" << (double)num_pri_probe / rs << std::endl;
    std::cout << std::defaultfloat;
  }

  void print_final(std::string header) {
    double total_time = durationW + durationRP + durationRS;
    uint64_t total_op = w + rp + rs;
    std::cout << std::fixed;
    std::cout.precision(3);
    std::cout << header
              << " Final Result: Avg Latency: " << total_time / total_op / 1000
              << " us, Throughput: " << tp / 1000 << " kops/s" << std::endl;
    std::cout << std::defaultfloat;
  }
};

std::vector<std::string> split(const std::string &s, char delim) {
  std::vector<std::string> elems;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
  return elems;
}

void run_trace(leveldb::DB *db, std::string trace, int tid,
               SharedThreadState *shared_state, Stats *stats, int topk = 10) {
  std::ifstream ifile;
  if (FLAGS_threads == 1) {
    ifile.open(trace);
  } else {
    ifile.open(trace + "_" + std::to_string(tid));
  }
  std::vector<leveldb::KeyValuePair> svalues;
  std::vector<std::string> pkey_values;
  if (!ifile) {
    std::cerr << "Can't open input file " << trace << std::endl;
    return;
  }

  shared_state->Barrier(FLAGS_threads);

  std::string line;
  uint64_t i = 0;

  leveldb::ReadOptions roptions;
  roptions.num_records = topk;

  while (getline(ifile, line)) {
    i++;

    std::vector<std::string> x = split(line, '\t');
    leveldb::WriteOptions woptions;
    // struct timeval start, end;
    if (x[0] == "w" || x[0] == "up") {
      StopWatch<double> sw(stats->durationW);

      uint64_t pk, sk, val_sz;
      pk = std::stoul(x[1]);
      sk = __builtin_bswap64(std::stoul(x[2]));
      val_sz = std::stoul(x[3]);
      leveldb::Slice pkey((const char *)&pk, sizeof(uint64_t));
      leveldb::Slice skey((const char *)&sk, sizeof(uint64_t));
      memcpy(content, skey.data(), skey.size());
      leveldb::Status s =
          db->PutWithSec(woptions, pkey, skey, leveldb::Slice(content, val_sz));
      if (!s.ok()) {
        ERROR_EXIT("put failed");
      }
      stats->w++;
    } else if (x[0] == "rs" || x[0] == "ro") {
      roptions.sec_read = true;
      roptions.num_pri_probe = 0;
      roptions.time_pdb = 0;  // time to search pdb in secondary query
      roptions.time_pri_file_io = 0;
      roptions.time_validate = 0;
      bool index_only = false;

      {
        StopWatch<double> sw(stats->durationRS);
        uint64_t sk = __builtin_bswap64(std::stoul(x[1]));
        leveldb::Slice skey((const char *)&sk, sizeof(uint64_t));
        if (leveldb::config::kIndexOnlyQuery || x[0] == "ro") {
          if (FLAGS_range_search) {
            roptions.num_scan_pkey_cnt = 5;
            leveldb::Status s =
                db->SecScanPKeyOnly(roptions, skey, &pkey_values);
          } else {
            leveldb::Status s =
                db->SecGetPKeyOnly(roptions, skey, &pkey_values);
          }
          index_only = true;
        } else {
          leveldb::Status s = db->SecGet(roptions, skey, &svalues);
        }
      }

      stats->durationPDB_RS += roptions.time_pdb;
      stats->duration_pri_file_io += roptions.time_pri_file_io;
      stats->duration_validate += roptions.time_validate;
      stats->num_pri_probe += roptions.num_pri_probe;
      if (index_only) {
        int size = pkey_values.size();
        stats->rscount += size;
        if (size > 0) {
          pkey_values.clear();
        }
      } else {
        int size = svalues.size();
        stats->rscount += size;
        if (size > 0) {
          svalues.clear();
        }
      }
      stats->rs++;

    } else if (x[0] == "rp") {
      roptions.sec_read = false;
      uint64_t pk = std::stoul(x[1]);

      std::string pvalue;
      StopWatch<double> sw(stats->durationRP);
      leveldb::Status s = db->Get(
          roptions, leveldb::Slice((const char *)&pk, sizeof(pk)), &pvalue);
      stats->rp++;
    }

    if (i % FLAGS_log_point == 0) {
      if (FLAGS_threads == 1) {
        if (i / FLAGS_log_point == 1) {
          stats->print_header();
        }
        stats->print();
      } else if (tid == 0) {
        std::cout << "... finished " << i << " ops\r";
      }
    }
    // if (i >= MAX_OP) break;
  }

  double total_time = stats->durationW + stats->durationRP + stats->durationRS;
  stats->tp = (double)i * 1e9 / total_time;
  ifile.close();
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::cout << "DB: " << FLAGS_db << std::endl;

  memset(content, 'A', sizeof(content));

  leveldb::Options options;
  options.write_buffer_size = 64 << 20;   // 64MB memtable
  // 16 GB cache for ~100 GB DB
  options.block_cache = leveldb::NewLRUCache(16ul << 30);
  options.max_open_files = 50000;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);
  if (FLAGS_use_existing_db) {
    options.create_if_missing = false;
    options.allow_seek_compaction = false;
  } else {
    options.create_if_missing = true;
    options.error_if_exists = true;
    options.allow_seek_compaction = true;
  }
  options.using_s_index = true;

  leveldb::DB *db;
  leveldb::Status status = leveldb::DB::Open(options, FLAGS_db, &db);
  if (!status.ok()) {
    std::cout << "Open DB failed" << std::endl;
    return 0;
  }

  Stats stats[FLAGS_threads];
  std::thread ths[FLAGS_threads - 1];
  SharedThreadState shared_state;

  if (FLAGS_preload != "") {
    std::cout << "\nrunning " << FLAGS_preload << std::endl;
    stats[0].clear();
    for (int t = 1; t < FLAGS_threads; ++t) {
      stats[t].clear();
      ths[t - 1] = std::thread(run_trace, db, FLAGS_preload, t, &shared_state,
                               &stats[t], 10);
    }
    run_trace(db, FLAGS_preload, 0, &shared_state, &stats[0], 10);

    for (int t = 1; t < FLAGS_threads; ++t) {
      ths[t - 1].join();
      stats[0].merge(stats[t]);
    }

    std::cout << "\n";
    stats[0].print_header();
    stats[0].print();
    stats[0].print_final("Preload(PUT)");
  }

  if (FLAGS_trace != "") {
    std::vector<int> topk_set;
    std::stringstream ss(FLAGS_topk);
    std::string s_topk;
    while (std::getline(ss, s_topk, ',')) {
      topk_set.push_back(std::stoi(s_topk));
    }
    for (int i = 0; i < topk_set.size(); ++i) {
      std::cout << "\nrunning " << FLAGS_trace << " for topk " << topk_set[i];
      if (FLAGS_range_search) {
        std::cout << " range";
      }
      std::cout << std::endl;
      // run_trace(db, FLAGS_trace, topk_set[i]);

      stats[0].clear();
      for (int t = 1; t < FLAGS_threads; ++t) {
        stats[t].clear();
        ths[t - 1] = std::thread(run_trace, db, FLAGS_trace, t, &shared_state,
                                 &stats[t], topk_set[i]);
      }
      run_trace(db, FLAGS_trace, 0, &shared_state, &stats[0], topk_set[i]);

      for (int t = 1; t < FLAGS_threads; ++t) {
        ths[t - 1].join();
        stats[0].merge(stats[t]);
      }
      std::cout << "\n";
      stats[0].print_header();
      stats[0].print();
      stats[0].print_final("Trace_top" + std::to_string(topk_set[i]));
    }
  }

  delete db;
  delete options.filter_policy;
  delete options.block_cache;

  return 0;
}

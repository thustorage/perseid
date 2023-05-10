#pragma once

#include <fcntl.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <queue>
#include <vector>

#include "db/dbformat.h"
#include "pebblesdb/db.h"
#include "pebblesdb/status.h"
#include "persist.h"
#include "util/lock.h"
#include "debug.h"

using leveldb::Slice;

struct PKeyEntry {
  union {
    uint64_t _payload = 0;
    struct {
      uint64_t prev : 48;
      uint64_t pkey_size : 8;
      uint64_t deleted : 8;
    };
  };
  uint64_t seq = 0;
  char data[0];
};

static_assert(sizeof(PKeyEntry) == 16);

// static constexpr size_t SEGMENT_SIZE = 4ul << 20;

class PKeyLog {
 public:
  PKeyLog(size_t size) {
    std::string pkey_log_pool = std::string(PMEM_DIR) + "pkey_log";
    int log_fd = open(pkey_log_pool.c_str(), O_CREAT | O_RDWR, 0644);
    if (log_fd < 0) {
      ERROR_EXIT("create pkey_log_pool failed.");
    }
    if (fallocate(log_fd, 0, 0, size) != 0) {
      ERROR_EXIT("fallocate file failed");
    }
    start_ = (char *)mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED,
                          log_fd, 0);
    close(log_fd);
    if (start_ == nullptr || start_ == MAP_FAILED) {
      ERROR_EXIT("pkey_log_pool mmap failed");
    }
    end_ = start_ + size;
    tail_ = start_;
  }

  ~PKeyLog() {
    // DEBUG_LOG("PKeyLog used %.3lf MiB",
    //           (double)(tail_ - start_) / (1024 * 1024));
    munmap(start_, end_ - start_);
  }

  // bool HasEnoughSpace(size_t sz) {
  //   return tail_ + sz <= end_;
  // }

  char *Append(const Slice &pkey, uint64_t seq, void *prev = nullptr) {
    size_t entry_size = sizeof(PKeyEntry) + pkey.size();

    char *cur_tail = tail_;
    while (true) {
      char *new_tail = cur_tail + entry_size;
      if (new_tail > end_) {
        return nullptr;
      }
      if (__atomic_compare_exchange_n(&tail_, &cur_tail, new_tail, true,
                                      __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        break;
      }
    }

    PKeyEntry *pke = (PKeyEntry *)cur_tail;
    pke->prev = (uint64_t)prev;
    pke->pkey_size = pkey.size();
    pke->deleted = false;
    pke->seq = seq;
    memcpy(pke->data, pkey.data(), pkey.size());
    clwb_fence(cur_tail, entry_size);
    return cur_tail;
  }

  PKeyLog(const PKeyLog &) = delete;
  PKeyLog &operator=(const PKeyLog &) = delete;

 private:
  char *start_;
  char *end_;
  char *volatile tail_;
};

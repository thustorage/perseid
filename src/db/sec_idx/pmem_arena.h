#pragma once

#include <libpmemobj.h>
#include <string>
#include <atomic>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstdio>
#include <unordered_map>
#include <queue>
#include <mutex>
#include "sec_idx_common.h"
#include "sec_idx_config.h"
#include "util/lock.h"
#include "debug.h"

using leveldb::SpinLock;

// static constexpr size_t IDX_POOL_SIZE = 4ul << 30;

// static constexpr uint64_t IDX_CHUNK_SIZE = 64UL << 20;

// extern thread_local char *cur_addr;
// extern thread_local char *end_addr;

class PMemAllocator {
 public:
  std::string idx_pool_path_;
  size_t pool_size_;
  PMemAllocator() = default;
  virtual ~PMemAllocator() = default;
  virtual void *Alloc(size_t size, size_t align = CACHE_LINE_SIZE) = 0;
  virtual void Free(void *ptr, size_t size) = 0;
  virtual void Initialize(std::string pool_name, size_t pool_size) = 0;
  // virtual size_t ToPoolOffset(const char *addr) = 0;
  // virtual void *ToDirectPointer(size_t offset) = 0;
  virtual char *GetStartAddr() = 0;
  virtual bool check_addr_valid(void *addr) = 0;

  // DISALLOW_COPY_AND_ASSIGN(PMemAllocator);
};


class PMDKAllocator : public PMemAllocator {
 public:
  PMDKAllocator(std::string pool_name, size_t pool_size) {
    Initialize(pool_name, pool_size);
  }

  virtual ~PMDKAllocator() {
    if (pop) {
      pmemobj_close(pop);
    }
  }

  virtual void *Alloc(size_t size, size_t align = CACHE_LINE_SIZE) override {
    PMEMoid oid;
    int ret = pmemobj_alloc(pop, &oid, size, 0, nullptr, nullptr);
    if (ret != 0) {
      ERROR_EXIT("PMem allocate failed.");
    }
    return pmemobj_direct(oid);
  }

  virtual void Free(void *ptr, size_t size) override {
    // PMEMoid oid = pmemobj_oid(ptr);
    // pmemobj_free(&oid);
  }

  virtual void Initialize(std::string pool_name, size_t pool_size) override {
    Destroy();

    idx_pool_path_ = std::string(PMEM_DIR) + pool_name;
    pool_size_ = pool_size;
    pop = pmemobj_create(idx_pool_path_.c_str(), "IDX", pool_size_, 0664);
    if (pop == nullptr) {
      ERROR_EXIT("Create PMEMobjpool failed.");
    }
  }

  // virtual size_t ToPoolOffset(const char *addr) override {
  //   return (size_t)addr - (size_t)pop;
  // }

  // virtual void *ToDirectPointer(size_t offset) override {
  //   return (void *)((size_t)pop + offset);
  // }

  virtual char *GetStartAddr() override { return (char *)pop; }

 private:
  void Destroy() {
    if (pop) {
      pmemobj_close(pop);
      pop = nullptr;
      remove(idx_pool_path_.c_str());
    }
  }

  PMEMobjpool *pop = nullptr;

  // DISALLOW_COPY_AND_ASSIGN(PMDKAllocator);
};


class MMAPAllocator : public PMemAllocator {
 public:
  MMAPAllocator(std::string pool_name, size_t pool_size) {
    Initialize(pool_name, pool_size);
  }

  virtual ~MMAPAllocator() {
    if (idx_start_addr) {
      // DEBUG_LOG(
      //     "allocator %s used %.3lf MiB", idx_pool_path_.c_str(),
      //     (double)(cur_alloc_addr.load() - idx_start_addr) / (1024 * 1024));
      munmap(idx_start_addr, pool_size_);
    }
  }

  virtual void *Alloc(size_t size, size_t align = CACHE_LINE_SIZE) override {
    std::lock_guard<SpinLock> lk(lock_);
    if (reserved_space_.find(size) != reserved_space_.end() &&
        !reserved_space_[size].empty()) {
      void *ret = reserved_space_[size].front();
      reserved_space_[size].pop();
      return ret;
    }

    char *ret = (char *)((uintptr_t)(cur_alloc_addr + align - 1) &
                         ~(uintptr_t)(align - 1));
    cur_alloc_addr.store(ret + size, std::memory_order_relaxed);
    if (ret >= idx_start_addr + pool_size_) {
      ERROR_EXIT("Pool %s: No enough space.", idx_pool_path_.c_str());
    }
    return ret;
  }

  virtual void Free(void *ptr, size_t size) override {
    std::lock_guard<SpinLock> lk(lock_);
    reserved_space_[size].push(ptr);
    // if (ptr == nullptr) {
    //   ERROR_EXIT("free nullptr");
    // }
  }

  virtual void Initialize(std::string pool_name, size_t pool_size) override {
    if (idx_start_addr) {
      munmap(idx_start_addr, pool_size_);
    }
    reserved_space_.clear();

    pool_size_ = pool_size;

#ifdef IDX_PERSISTENT
    if (pool_name == "") {
      idx_start_addr = (char *)mmap(NULL, pool_size_, PROT_READ | PROT_WRITE,
                                    MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    } else {
      idx_pool_path_ = std::string(PMEM_DIR) + pool_name;
      int idx_pool_fd = open(idx_pool_path_.c_str(), O_CREAT | O_RDWR, 0644);
      if (idx_pool_fd < 0) {
        ERROR_EXIT("open file failed");
      }
      if (fallocate(idx_pool_fd, 0, 0, pool_size_) != 0) {
        ERROR_EXIT("fallocate file failed");
      }
      idx_start_addr = (char *)mmap(NULL, pool_size_, PROT_READ | PROT_WRITE,
                                    MAP_SHARED, idx_pool_fd, 0);
      close(idx_pool_fd);
    }
#else
    // use mmap for DRAM allocate, we can free these space among benchmarks
    idx_start_addr = (char *)mmap(NULL, pool_size_, PROT_READ | PROT_WRITE,
                                  MAP_SHARED | MAP_ANONYMOUS, -1, 0);
#endif
    if (idx_start_addr == nullptr || idx_start_addr == MAP_FAILED) {
      ERROR_EXIT("idx_pool mmap failed");
    }
    // global_alloc_addr = idx_start_addr;
    cur_alloc_addr = idx_start_addr;
    // DEBUG_LOG("%s begin %p end %p", pool_name.c_str(), idx_start_addr,
    //           idx_start_addr + pool_size_);
  }

  // virtual size_t ToPoolOffset(const char *addr) override {
  //   return (size_t)addr - (size_t)idx_start_addr;
  // }

  // virtual void *ToDirectPointer(size_t offset) override {
  //   return (void *)((size_t)idx_start_addr + offset);
  // }

  virtual char *GetStartAddr() override { return idx_start_addr; }
  
  virtual bool check_addr_valid(void *addr) override {
    return addr >= idx_start_addr && addr < cur_alloc_addr;
  }

 private:
  char *idx_start_addr = nullptr;
  // std::atomic<char *> global_alloc_addr{nullptr};
  std::atomic<char *> cur_alloc_addr{nullptr};
  SpinLock lock_;
  std::unordered_map<size_t, std::queue<void *>> reserved_space_;

  // DISALLOW_COPY_AND_ASSIGN(MMAPAllocator);
};

#pragma once

#include <atomic>
#include <string>
#include <cassert>
#include <sys/time.h>

namespace leveldb {

#ifdef LOGGING
#define LOG(fmt, ...)                                                          \
  fprintf(stderr, "\033[1;31mLOG(<%s>:%d %s): \033[0m" fmt "\n", __FILE__,     \
          __LINE__, __func__, ##__VA_ARGS__)
#else
#define LOG(fmt, ...)
#endif

static inline uint64_t NowMicros() {
  static constexpr uint64_t kUsecondsPerSecond = 1000000;
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
}

class SpinLock {
 public:
  SpinLock() : mutex(false) {}
  SpinLock(std::string name) : mutex(false), name(name) {}

  bool try_lock() {
    bool expect = false;
    return mutex.compare_exchange_strong(
        expect, true, std::memory_order_release, std::memory_order_relaxed);
  }

  void lock() {
    uint64_t startOfContention = 0;
    bool expect = false;
    while (!mutex.compare_exchange_weak(expect, true, std::memory_order_release,
                                        std::memory_order_relaxed)) {
      expect = false;
      debugLongWaitAndDeadlock(&startOfContention);
    }
    if (startOfContention != 0) {
      contendedTime += NowMicros() - startOfContention;
      ++contendedAcquisitions;
    }
  }

  void unlock() { mutex.store(0, std::memory_order_release); }

  void report() {
    LOG("spinlock %s: contendedAcquisitions %lu contendedTime %lu us",
        name.c_str(), contendedAcquisitions, contendedTime);
  }

 private:
  std::atomic_bool mutex;
  std::string name;
  uint64_t contendedAcquisitions = 0;
  uint64_t contendedTime = 0;

  void debugLongWaitAndDeadlock(uint64_t *startOfContention) {
    if (*startOfContention == 0) {
      *startOfContention = NowMicros();
    } else {
      uint64_t now = NowMicros();
      if (now >= *startOfContention + 1000000) {
        LOG("%s SpinLock locked for one second; deadlock?", name.c_str());
      }
    }
  }
};

// read write lock
class ReadWriteLock {
  // the lowest bit is used for writer
 public:
  bool TryReadLock() {
    uint64_t old_val = lock_value.load(std::memory_order_acquire);
    while (true) {
      if (old_val & 1 || old_val > 1024) {
        break;
      }
      uint64_t new_val = old_val + 2;
      bool cas = lock_value.compare_exchange_weak(old_val, new_val,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire);
      if (cas) {
        return true;
      }
    }
    return false;
  }

  void ReadLock() {
    while (!TryReadLock())
      ;
  }

  void ReadUnlock() {
    uint64_t old_val = lock_value.load(std::memory_order_acquire);
    while (true) {
      if (old_val <= 1) {
        assert(old_val >= 2);
        return;
      }
      uint64_t new_val = old_val - 2;
      if (lock_value.compare_exchange_weak(old_val, new_val)) {
        break;
      }
    }
  }

  bool TryWriteLock() {
    uint64_t old_val = lock_value.load(std::memory_order_acquire);
    while (true) {
      if (old_val & 1) {
        return false;
      }
      uint64_t new_val = old_val | 1;
      bool cas = lock_value.compare_exchange_weak(old_val, new_val);
      if (cas) {
        break;
      }
    }
    // got write lock, waiting for readers
    while (lock_value.load(std::memory_order_acquire) != 1) {
      asm("nop");
    }
    return true;
  }

  void WriteLock() {
    while (!TryWriteLock())
      ;
  }

  void WriteUnlock() {
    assert(lock_value == 1);
    lock_value.store(0);
  }

 private:
  std::atomic_uint_fast64_t lock_value{0};
};

class ReadLockHelper {
 public:
  explicit ReadLockHelper(ReadWriteLock &rwlock) : rwlock_(rwlock) {
    rwlock_.ReadLock();
  }

  ~ReadLockHelper() { rwlock_.ReadUnlock(); }

 private:
  ReadWriteLock &rwlock_;
};

} // namespace leveldb

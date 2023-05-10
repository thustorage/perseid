#pragma once

#include <assert.h>
#include <immintrin.h>

#include "sec_idx_common.h"

static inline void mfence() { asm volatile("mfence" ::: "memory"); }

static inline void sfence() { _mm_sfence(); }

static force_inline void pmem_clwb(const void *addr) {
  asm volatile(".byte 0x66; xsaveopt %0" : "+m"(*(volatile char *)(addr)));
}

static force_inline void clwb_fence(const void *addr, size_t len) {
  uintptr_t uptr;
  for (uptr = (uintptr_t)addr & ~(FLUSH_ALIGN - 1);
       uptr < (uintptr_t)addr + len; uptr += FLUSH_ALIGN)
    pmem_clwb((char *)uptr);
  _mm_sfence();
}

static force_inline void clwb_nofence(const void *addr, size_t len) {
  uintptr_t uptr;
  for (uptr = (uintptr_t)addr & ~(FLUSH_ALIGN - 1);
       uptr < (uintptr_t)addr + len; uptr += FLUSH_ALIGN)
    pmem_clwb((char *)uptr);
}

static force_inline void idx_clwb_fence(const void *addr, size_t len) {
  clwb_fence(addr, len);
}

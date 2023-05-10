#pragma once

#include <stdint.h>

// for secondary index
struct SeqCnt {
  union {
    struct {
      uint64_t seq_ : 48;
      uint64_t cnt_ : 16;
    };
    uint64_t _data = 0;
  };
  SeqCnt() {}
  SeqCnt(uint64_t seq, uint64_t cnt) : seq_(seq), cnt_(cnt) {}
  void SetNewSeq(uint64_t seq) {
    seq_ = seq;
    ++cnt_;
  }
};


#include "CCEH.h"
#include <stdio.h>

using CCEH_NAMESPACE::CCEH;

int main() {
  CCEH *cceh = new CCEH();
  for (int i = 0; i < 4096; i++) {
    cceh->Insert(i, i);
  }
  uint64_t a = 0x1234;
  cceh->Insert(a, a);
  cceh->Insert(0x5678, 0x5678);
  printf("%lx %lx\n", cceh->Get(0x1234), cceh->Get(0x1111));
  delete cceh;
  return 0;
}

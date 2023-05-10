#include "clht_lb_res.h"

int main() {
  clht *ht = clht_create(1024);
  for (int i = 0; i < 1000; i++) {
    clht_put(ht, i, i);
  }
  for (int i = 0; i < 1000; i++) {
    if (clht_get(ht->ht, i) != i) {
      printf("error %d\n", i);
    }
  }
  return 0;
}

#include "ff_btree.h"

#include <stdio.h>

int main() {
  btree *bt = new btree();
  FILE *f = fopen("a.txt", "r");
  char skey[20];
  char pkey[40];
  uint64_t seq;
  while (fscanf(f, "%s %s %ld", skey, pkey, &seq) == 3) {
    bt->btree_insert(skey, std::string(pkey), seq);
    DEBUG_LOG("insert %s %s %ld", std::string(skey).c_str(), pkey, seq);
  }

  return 0;
}

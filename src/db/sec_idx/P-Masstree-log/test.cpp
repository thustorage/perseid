#include <iostream>
#include <chrono>
#include <random>

using namespace std;

#include "masstree.h"

int main() {
  masstree::masstree *tree = new masstree::masstree();
  auto info = tree->getThreadInfo();
  char s1[] = "123456789";
  tree->put(s1, 123, info);
  char s2[] = "123456780";
  tree->put(s2, 456, info);
  printf("%ld\n", (int64_t)tree->get(s1, info));
  printf("%ld\n", (int64_t)tree->get(s2, info));
}


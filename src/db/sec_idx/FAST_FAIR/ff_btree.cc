#include "ff_btree.h"

/*
 * class btree
 */

btree::btree() {
  pmem_allocator_ = new MMAPAllocator("fastfair_pool", 4ul << 30);
  pkey_block_allocator_ = new MMAPAllocator("fastfair_pkb_pool", 8ul << 30);

  void *p = pmem_allocator_->Alloc(sizeof(page));
  root = (char *)new (p) page();
  height = 1;
}

btree::~btree() {
  if (pmem_allocator_) {
    delete pmem_allocator_;
  }
  if (pkey_block_allocator_) {
    delete pkey_block_allocator_;
  }
}

void btree::setNewRoot(char *new_root) {
  this->root = (char *)new_root;
  idx_clwb_fence((char *)&(this->root), sizeof(char *));
  ++height;
}

char *btree::btree_search(entry_key_t key) {
  page *p = (page *)root;

  while (p->hdr.leftmost_ptr != NULL) {
    p = (page *)p->linear_search(key);
  }

  page *t;
  while ((t = (page *)p->linear_search(key)) == p->hdr.sibling_ptr) {
    p = t;
    if (!p) {
      break;
    }
  }

  if (!t) {
    // printf("NOT FOUND %lu, t = %x\n", key, t);
    return NULL;
  }

  return (char *)t;
}

// // insert the key in the leaf node
// void btree::btree_insert(entry_key_t key, InsertHelper &ins_helper) {
//   page *p = (page *)root;

//   while (p->hdr.leftmost_ptr != NULL) {
//     p = (page *)p->linear_search(key);
//   }
  
//   char *value = ins_helper.new_val;
//   if (!p->store(this, NULL, key, value, true, true, nullptr, &ins_helper)) {
//     btree_insert(key, ins_helper);
//   }
// }

void btree::btree_insert(entry_key_t key, const Slice &value,
                         SequenceNumber seq, clht *ht) {
  page *p = (page *)root;

  while (p->hdr.leftmost_ptr != NULL) {
    p = (page *)p->linear_search(key);
  }

  if (!p->store(this, key, value, seq, ht, true, true, nullptr)) {
    btree_insert(key, value, seq, ht);
  }
}

// store the key into the node at the given level
void btree::btree_insert_internal(char *left, entry_key_t key, char *right,
                                  uint32_t level) {
  if (level > ((page *)root)->hdr.level)
    return;

  page *p = (page *)this->root;

  while (p->hdr.level > level)
    p = (page *)p->linear_search(key);

  if (!p->store(this, NULL, key, right, true, true)) {
    btree_insert_internal(left, key, right, level);
  }
}

void btree::btree_delete(entry_key_t key) {
  page *p = (page *)root;

  while (p->hdr.leftmost_ptr != NULL) {
    p = (page *)p->linear_search(key);
  }

  page *t;
  while ((t = (page *)p->linear_search(key)) == p->hdr.sibling_ptr) {
    p = t;
    if (!p)
      break;
  }

  if (p) {
    if (!p->remove(this, key)) {
      btree_delete(key);
    }
  } else {
    // printf("not found the key to delete %lu\n", key);
  }
}

void btree::btree_delete_internal(entry_key_t key, char *ptr, uint32_t level,
                                  entry_key_t *deleted_key,
                                  bool *is_leftmost_node, page **left_sibling) {
  if (level > ((page *)this->root)->hdr.level)
    return;

  page *p = (page *)this->root;

  while (p->hdr.level > level) {
    p = (page *)p->linear_search(key);
  }

  p->hdr.mtx->lock();

  if ((char *)p->hdr.leftmost_ptr == ptr) {
    *is_leftmost_node = true;
    p->hdr.mtx->unlock();
    return;
  }

  *is_leftmost_node = false;

  for (int i = 0; p->records[i].ptr != NULL; ++i) {
    if (p->records[i].ptr == ptr) {
      if (i == 0) {
        if ((char *)p->hdr.leftmost_ptr != p->records[i].ptr) {
          *deleted_key = p->records[i].key;
          *left_sibling = p->hdr.leftmost_ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      } else {
        if (p->records[i - 1].ptr != p->records[i].ptr) {
          *deleted_key = p->records[i].key;
          *left_sibling = (page *)p->records[i - 1].ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      }
    }
  }

  p->hdr.mtx->unlock();
}

// Function to search keys from "min" to "max"
void btree::btree_search_range(entry_key_t min, entry_key_t max,
                               unsigned long *buf) {
  page *p = (page *)root;

  while (p) {
    if (p->hdr.leftmost_ptr != NULL) {
      // The current page is internal
      p = (page *)p->linear_search(min);
    } else {
      // Found a leaf
      p->linear_search_range(min, max, buf);

      break;
    }
  }
}

void btree::btree_search_range(entry_key_t min, int cnt,
                               std::vector<uint64_t> &vec) {
  page *p = (page *)root;

  while (p) {
    if (p->hdr.leftmost_ptr != NULL) {
      // The current page is internal
      p = (page *)p->linear_search(min);
    } else {
      // Found a leaf
      p->linear_search_range(min, cnt, vec);

      break;
    }
  }
}

// void btree::printAll() {
//   pthread_mutex_lock(&print_mtx);
//   int total_keys = 0;
//   page *leftmost = (page *)root;
//   printf("root: %p\n", root);
//   do {
//     page *sibling = leftmost;
//     while (sibling) {
//       if (sibling->hdr.level == 0) {
//         total_keys += sibling->hdr.last_index + 1;
//       }
//       sibling->print();
//       sibling = sibling->hdr.sibling_ptr;
//     }
//     printf("-----------------------------------------\n");
//     leftmost = leftmost->hdr.leftmost_ptr;
//   } while (leftmost);

//   printf("total number of keys: %d\n", total_keys);
//   pthread_mutex_unlock(&print_mtx);
// }

/***********************************************
 *                                             *
 * -__ /\\     ,,         /\\                  *
 *   ||  \\  ' ||   _    ||                    *
 *  /||__|| \\ ||  < \, =||=                   *
 *  \||__|| || ||  /-||  ||                    *
 *   ||  |, || || (( ||  ||                    *
 * _-||-_/  \\ \\  \/\\  \\,                   *
 *   ||                                        *
 *                                             *
 *   Pilaf Infiniband DHT                      *
 *   (c) 2012-2013 Christopher Mitchell et al. *
 *   New York University, Courant Institute    *
 *   Networking and Wide-Area Systems Group    *
 *   All rights reserved.                      *
 *                                             *
 *   dht.h: Underlying single-server k/v       *
 *          class for the distributed HT.      *
 *          Uses d-ary Cuckoo hashing.         *
 *                                             *
 ***********************************************/

#ifndef DHT_H
#define DHT_H

#include <stddef.h>
#include <vector>
#include <assert.h>
#include <stdio.h>
#include <cstdint>
#include "table_types.h"
#include "mem/mem5.h"

// Contains CRC and hash functions
#include "integrity.h"

#define BUCKET_NOT_FOUND SIZE_MAX

#define INIT_EXTENTS_SIZE (1<<20)	//1 MB
#define INIT_KV_CAPACITY 16

#define EXTENTS_MARGIN 1.25			//25% spare space in extents

#define	CUCKOO_D 3
#define MAX_INSERT_CYCLES 32

#define _write_barrier() __asm__ volatile ( "sfence" )

#define EXTENTSIZE(kl,vl) (size_t)(EXTENTS_MARGIN*((kl)+(vl)))
inline bool binarycmp(const char * p1, size_t p1_len, const char *p2, size_t p2_len) {
  if (p1_len != p2_len) return false;

  for (int i = 0; i < p1_len; i++) {
    if (p1[i] != p2[i])
      return false;
  }
  return true;
}


template<class K, class V>
class DHT {

public:
  // The bucket thing
    struct __attribute__ ((__packed__)) dht_data {
      K key;
      V value;

      size_t key_len;
      size_t val_len;
      uint64_t crc;
      unsigned int ext_capacity;		//extents

      bool in_use:1;
      int  hash:7;
    };
  struct __attribute__ ((__packed__)) dht_block {
    struct dht_data d;
    uint64_t guard;
  };

  // DHT
  std::vector<dht_block> buckets_;

  // DHT Extents
  void* memregion;					//used for extents
  size_t ext_size_;

  // DHT shard attributes
  size_t size_;			//current capacity
  size_t entries_;		//filled  capacity

private:
  friend class IB_DHT;

  // When loading gets beyond this percentage, resize
  static const double kLoadFactor = 0.75;

  Integrity64 crc;

  struct dht_block blankrow;

  // DHT shard extents attributes
  PoolMalloc pool;
  // entries should be the same as entries_

  // Callback for before table is resized
  int (*pre_resize_hook) (size_t, std::vector<dht_block>*, void*);
  void* pre_resize_context;

  // Callback for after table is resized
  int (*post_resize_hook)(size_t, std::vector<dht_block>*, void*);
  void* post_resize_context;

  // Callback for before extents are resized
  int (*pre_resize_extents_hook) (size_t, std::vector<dht_block>*, void*);
  void* pre_resize_extents_context;

  // Callback for after extents are resized
  int (*post_resize_extents_hook)(size_t, std::vector<dht_block>*, void*);
  void* post_resize_extents_context;

public:
  DHT() {
    size_ = 0;
    entries_ = 0;
    buckets_.clear();

    // DHT shard itself
    pre_resize_hook = NULL;
    pre_resize_context = NULL;
    post_resize_hook = NULL;
    post_resize_context = NULL;

    // DHT shard extents
    pre_resize_extents_hook = NULL;
    pre_resize_extents_context = NULL;
    post_resize_extents_hook = NULL;
    post_resize_extents_context = NULL;

    // Start table with some extents capacity
    memregion = (void*)malloc(INIT_EXTENTS_SIZE);
    if (NULL == memregion) {
      fprintf(stderr,"Failed to allocate memregion for extents");
      exit(-1);
    }
    ext_size_ = INIT_EXTENTS_SIZE;
    pool.memsys5Init(memregion,ext_size_,3);	//minimum 1<<3 = 8 bytes per k-v

    blankrow.d.in_use = 0;
    blankrow.d.hash = 0;

    blankrow.d.key = NULL;
    blankrow.d.value = NULL;
    blankrow.d.key_len = 0;
    blankrow.d.val_len = 0;
    blankrow.d.ext_capacity = 0;
    blankrow.d.crc = 0;

    fix_guard(&blankrow);

    // Start table with some k-v capacity
    resize(INIT_KV_CAPACITY);

  }

  /**
   * Returns the number of slots this table shard contains
   */
  size_t buckets() {
    return size_;
  }

  /**
   * Pretends to resizes the DHT to
   * trigger any resize hooks, but does
   * not actually change the size.
   */
  void resize(void) {

    // Pre-resize hook
    if (pre_resize_hook) pre_resize_hook(size_, &buckets_, pre_resize_context);

    // Post-resize hook
    if (post_resize_hook) post_resize_hook(size_, &buckets_, post_resize_context);

  }

  /**
   * Resizes the DHT itself
   */
  void resize(size_t capacity) {
    // Sanity-checking
    assert(capacity >= size_);
    if (capacity == size_)
      return;

    printf("Resizing DHT from %lu to %lu entries.\n",size_,capacity);

    // Back up the bucket for restoration
    std::vector<dht_block> old_b = buckets_;

    // Pre-resize hook
    if (pre_resize_hook) pre_resize_hook(size_, &buckets_, pre_resize_context);

    // Perform the resize, protect and clear, propagate new size
    buckets_.resize(capacity);
    for(size_t i = 0; i < capacity; i++) {
      memcpy(&buckets_[i],&blankrow,sizeof(struct dht_block));
    }

    // Copy in new contents and save new size;
    size_t oldsize = size_;
    size_ = capacity;
    entries_ = 0;
    for(size_t i = 0; i < oldsize; i++) {
      if (old_b[i].d.in_use) {
        insert_internal(old_b[i].d.key, old_b[i].d.key_len, old_b[i].d.value, old_b[i].d.val_len, old_b[i].d.crc, true, old_b[i].d.ext_capacity);
      }
    }
    old_b.clear();

    // Post-resize hook
    if (post_resize_hook) post_resize_hook(size_, &buckets_, post_resize_context);

  }

  /**
   * Resizes the extents of the DHT, when necessary. This
   * operation is not synchronous with DHT resizing.
   */
  void resize_extents(size_t capacity) {

    printf("Resizing DHT extents to %zu bytes.\n",capacity);

    // Create a new memory pool and initialize
    // it with a new malloc'd chunk of heap.
    PoolMalloc newpool;
    void* newmemregion = (void*)malloc(capacity);
    if (NULL == newmemregion) {
      fprintf(stderr,"Failed to allocate memregion for extents");
      exit(-1);
    }
    ext_size_ = capacity;
    newpool.memsys5Init(newmemregion,ext_size_,3);	//minimum 1<<3 = 8 bytes per k-v

    // Pre-resize extents hook
    if (pre_resize_extents_hook)
      pre_resize_extents_hook(size_, &buckets_, pre_resize_extents_context);

    // Move all extents into new memory area
    for(size_t i = 0; i < size_; i++) {
      if (buckets_[i].d.in_use) {

        void* newspace = newpool.memsys5Malloc(buckets_[i].d.ext_capacity);
        if (newspace == NULL) {
          fprintf(stderr,"Fatal bug: ran out of memory during extents resize");
          *((char*)0) = 0;
          exit(-1);
        }

        memcpy(newspace,buckets_[i].d.key,buckets_[i].d.ext_capacity);
        buckets_[i].d.key[0]++; //invalid old crc
        buckets_[i].d.key = (K)newspace;
        buckets_[i].d.value = (V)((char*)newspace+buckets_[i].d.key_len);
        fix_guard(&buckets_[i]);
        // No point in pool.memsys5Free() since we're going to throw out
        // the entire memory region

      }
    }

    // Delete old pool and free its memory
    pool.memsys5Shutdown();
    free(memregion);

    // Set member variables for new region/pool
    memregion = newmemregion;
    pool = newpool;

    // Post-resize extents hook
    if (post_resize_extents_hook)
      post_resize_extents_hook(size_, &buckets_, post_resize_extents_context);

  }

  /**
   * Reserves extents for a k,v pair
   */
  size_t reserve_extents(const K& key, size_t key_len, V value, size_t val_len, dht_block* block) {
    size_t extsize = EXTENTSIZE(key_len,val_len);
    block->d.key = (K)pool.memsys5Malloc(extsize);

    while (block->d.key == NULL) {	//out of memory
      block->d.in_use = 0;

      if (ext_size_ > (1024L*1024L*1024L)) {
        resize_extents(1.25*ext_size_);
      } else {
        resize_extents(2*ext_size_);
      }

      block->d.key = (K)pool.memsys5Malloc(extsize);
      block->d.in_use = 1;
    }

    block->d.ext_capacity = extsize;
	  block->d.value = block->d.key+key_len;
    return extsize;
  }

  /**
   * An insert triggered by resize. For key/vals
   * that are strings, this should not re-reserve
   * any extents.
   */
  void insert_internal(const K& key, size_t key_len, V value, size_t val_len, uint64_t crc_, bool have_crc, size_t capacity) {
    insert(key, key_len, value, val_len, crc_, have_crc, true, capacity);
  }

  /**
   * Insert a k,v pair into the table. The
   * extra nocopy argument is if this insert is
   * coming from a resize operation and the
   * strings should not be re-reserved/copied.
   */
  void insert(const K& key, size_t key_len, V value, size_t val_len, uint64_t crc_ = 0, bool have_crc = false, bool nocopy = false, size_t capacity = 0) {
    size_t hv[CUCKOO_D];

    for(int i=0; i < CUCKOO_D; i++) {
      size_t b = hv[i] = bucket_idx(key,key_len,i);

      if (!buckets_[b].d.in_use) {
		    buckets_[b].d.hash = i;
        set(b,key,key_len,value,val_len,crc_,have_crc,nocopy,capacity);
        entries_++;
        return;
      } else if (binarycmp(buckets_[b].d.key,buckets_[b].d.key_len,key,key_len)) {
        update(key,key_len,value,val_len,crc_,have_crc);
        return;
      }

    }

    // Jinyang's suggestion to prevent a walking lock
    // situation. Walk forward until we find a blank
    // spot, then go backwards. Each entry in the traceback
    // array is the hash index of the entry where the *new*
    // key is virtually getting placed into.
    std::vector<uint64_t> traceback;
    std::vector<uint8_t> hashback;

    traceback.clear(); hashback.clear();

    K tempkey = buckets_[hv[0]].d.key;
    size_t tempkey_len = buckets_[hv[0]].d.key_len;

    size_t start = hv[0];
    int hash_ixp = buckets_[hv[0]].d.hash;
    int hash_ixq = (hash_ixp + 1) % CUCKOO_D;
    hv[hash_ixp] = hv[0];	 //for the traceback push

    size_t cycles = 0;
    int foundslot = -1;

    do {
      for(int i = hash_ixq; i != hash_ixp; i=(i+1)%CUCKOO_D) {
        hv[i] = bucket_idx(tempkey,tempkey_len,i);
        if (!buckets_[hv[i]].d.in_use) {
          foundslot = i;
          break;
        }
      }

      // this needs to happen whether we found a slot or not
      traceback.push_back(hv[hash_ixp]);
      int pushslot = (foundslot == -1)?hash_ixq:foundslot;
      hashback.push_back(pushslot);

      if (-1 != foundslot)
        break;

      // What if a key collides with itself?
      if (binarycmp(buckets_[hv[pushslot]].d.key,buckets_[hv[pushslot]].d.key_len,tempkey,tempkey_len)) {
        cycles = MAX_INSERT_CYCLES;
      }

      tempkey = buckets_[hv[hash_ixq]].d.key;
      tempkey_len = buckets_[hv[hash_ixq]].d.key_len;
      hash_ixp = buckets_[hv[hash_ixq]].d.hash;
      hv[hash_ixp] = hv[hash_ixq];	 //for the traceback push
      hash_ixq = (hash_ixp + 1) % CUCKOO_D;

      cycles++;

    } while(foundslot == -1 && cycles < MAX_INSERT_CYCLES && start != hash_ixq);

    // Time to resize or insert
    if (foundslot == -1) {
      resize(1+2*size_);
      insert(key,key_len,value,val_len,crc_,have_crc,nocopy,capacity);
      return;
    }

    // found spot, must walk backwards shifting pieces around
    // move items until we get back to where we started
    // Pull the tempkey to this spot and work backwards
    size_t target = hv[foundslot];
	  for (int i=traceback.size()-1; i >= 0; i--) {
      size_t current = traceback[i];
      memcpy(&(buckets_[target]),&(buckets_[current]),sizeof(struct dht_block));
      buckets_[target].d.hash = hashback[i];
      fix_guard(&buckets_[target]);
      target = current;
    }

	  buckets_[start].d.hash = 0;
    set(start,key,key_len,value,val_len,crc_,have_crc,nocopy,capacity);
    entries_++;

  }


  void set(size_t b, const K& key, size_t key_len, V value, size_t val_len, uint64_t crc_ = 0, bool have_crc = false, bool nocopy = false, size_t capacity = 0) {

	// caller should set buckets_[b].d.hash

    buckets_[b].d.in_use = 1;
    if (!nocopy) {
      // Normal user-triggered insertion.
      reserve_extents(key,key_len,value,val_len,&(buckets_[b]));

      memcpy(buckets_[b].d.key,key,key_len);
      memcpy(buckets_[b].d.value,value,val_len);
    } else {
      // Internally-triggered insertion from resize.
      buckets_[b].d.key = key;
      buckets_[b].d.value = value;

      if (capacity)
        buckets_[b].d.ext_capacity = capacity;
    }

    buckets_[b].d.key_len = key_len;
    buckets_[b].d.val_len = val_len;

    if (have_crc)
      buckets_[b].d.crc = crc_;
    else
      buckets_[b].d.crc = crc.crc(buckets_[b].d.key,2+key_len+val_len);

    fix_guard(&buckets_[b]);
    _write_barrier();   // Must make sure it is in mem before replying to client (13-01-25)

  }

  /**
   * Updates an existing key, value in the table
   * According to the current design, clients should
   * do this remotely if possible. The server should
   * only be the one doing it if extents need to be
   * enlarged.
   */
  void update(const K& key, size_t key_len, V value, size_t val_len, uint64_t crc_ = 0, bool have_crc = false) {
    size_t index;

    for(int i=0; i < CUCKOO_D; i++) {
      index = bucket_idx(key,key_len,i);
      if (buckets_[index].d.in_use) {
        if (binarycmp(buckets_[index].d.key,buckets_[index].d.key_len,key,key_len)) {
          if (buckets_[index].d.ext_capacity >= key_len+val_len) {

            // Fits in current extents
            memcpy(buckets_[index].d.value,value,val_len);

          } else {

            // Needs new extents
            buckets_[index].d.key[0]++; //mess up the CRC
            pool.memsys5Free(buckets_[index].d.key);
            reserve_extents(key,key_len,value,val_len,&(buckets_[index]));

            memcpy(buckets_[index].d.key,key,key_len);
            memcpy(buckets_[index].d.value,value,val_len);
          }

          buckets_[index].d.key_len = key_len;
          buckets_[index].d.val_len = val_len;

          if (have_crc)
            buckets_[index].d.crc = crc_;
          else
            buckets_[index].d.crc = crc.crc(buckets_[index].d.key,key_len+val_len);

          fix_guard(&buckets_[index]);
          return;
        } // end key matches
      } // end in_use
    } // end for

    insert(key,key_len,value,val_len,crc_,have_crc);
    return;
  }

  /**
   * Removes a key-value pair, if it exists in the table. For a key with extents,
   * invalidates and frees the extents as well.
   */
  void remove(const K& key,size_t key_len) {
    size_t index;

    for(int i=0; i < CUCKOO_D; i++) {
      index = bucket_idx(key,key_len,i);
      if (buckets_[index].d.in_use) {
        if (binarycmp(buckets_[index].d.key,buckets_[index].d.key_len,key,key_len)) {
          buckets_[index].d.key[0]++; //make the CRC wrong
          pool.memsys5Free(buckets_[index].d.key);
          buckets_[index].d.in_use = 0;
          fix_guard(&buckets_[index]);
          entries_--;
          return;
        } //end key check
      } //end in_use
    } //end for
  }

  /**
   * Updates the CRC attached to a row for self-validation
   */
  inline void fix_guard(struct dht_block* block) {
    block->guard = crc.crc((char*)&(block->d),sizeof(struct dht_data));
  }

  /**
   * Returns 1 for existing keys, 0 otherwise.
   */
  bool contains(const K& key, size_t key_len) {
    size_t index;
    for(int i=0; i < CUCKOO_D; i++) {
      index = bucket_idx(key,key_len,i);
      if (buckets_[index].d.in_use) {
        if (binarycmp(buckets_[index].d.key,buckets_[index].d.key_len,key,key_len))
          return true;
      } //end in_use
    } //end for
    return false;
  }

  /**
   * Returns a bucket index for a given key.
   * Disregards whether the key is actually there.
   */
  inline size_t bucket_idx(const K& key, size_t key_len, int hash = 0) {		//starting position from hash
    return crc.hashN(key,key_len,hash) % size_;
  }

  /**
   * Set callbacks for resizing the DHT (not extents)
   */
  void set_resize_hooks(int(*pre_hook)(size_t, std::vector<dht_block>*, void*), void* pre_context,
                        int(*post_hook)(size_t, std::vector<dht_block>*, void*), void* post_context) {
    pre_resize_hook = pre_hook;
    pre_resize_context  = pre_context;
    post_resize_hook = post_hook;
    post_resize_context = post_context;
  }

  /**
   * Set callbacks for resizing the DHT (not extents)
   */
  void set_resize_extents_hooks(int(*pre_hook)(size_t, std::vector<dht_block>*, void*), void* pre_context,
                                int(*post_hook)(size_t, std::vector<dht_block>*, void*), void* post_context) {
    pre_resize_extents_hook = pre_hook;
    pre_resize_extents_context  = pre_context;
    post_resize_extents_hook = post_hook;
    post_resize_extents_context = post_context;
  }

  void dump_table(void) {
    printf("Slot     \tHx\tKey\n");
    for(size_t i=0; i<size_; i++) {
      if (buckets_[i].d.in_use) {
        printf("%9zu\t%2d\t%s\n",i,buckets_[i].d.hash,buckets_[i].d.key);
      } else {
        printf("%9zu\t [EMPTY] -----------------------\n",i);
      }
    }
  }

}; // end class DHT

enum post_get_state {
  POST_GET_FOUND = 0,        // success
  POST_GET_LOCKED = -1,      // locked, need to re-read
  POST_GET_MISSING = -2,     // slot is not in_use
  POST_GET_FAILURE = -3,     // IB problem
  POST_GET_COLLISION = -4,   // slot is filled, but with wrong key
  POST_CONTAINS_FAILURE = -5 // contains failed for some reason
};

#define POST_PUT_FAILURE POST_GET_FAILURE

template<class K, class V>
class DHTClient {
private:
  std::vector<struct DHT<K,V>::dht_block>* dht_;
  size_t dht_size_;

  Integrity64 crc;

public:
  DHTClient(void* dht, size_t dht_size) {
    dht_ = (std::vector<struct DHT<K,V>::dht_block>*)dht;
    dht_size_ = dht_size;
  }

  int server_for_key(int server_count, K key, size_t key_len) {
    return crc.hashN(key,key_len,0) % server_count;
  }

  uint64_t check_crc(void* mem, size_t len) {
    return crc.crc((char*)mem,len);
  }

  void* pre_get(K key, size_t key_len, int hash_idx) {
    size_t offset = crc.hashN(key,key_len,hash_idx) % dht_size_;
    void* mem_off = (void*)(((char*)dht_) + ((offset) * sizeof(struct DHT<K,V>::dht_block)));
    return mem_off;
  }

  inline int post_get(struct DHT<K,V>::dht_block* block, K key, size_t key_len, V& value, bool skip_crc = false) {
    int rval = post_contains(block, key, key_len, skip_crc);
    return rval;
  }

  inline int post_contains(struct DHT<K,V>::dht_block* block, K key, size_t key_len, bool skip_crc = false) {

    // 2012-10-02: Guards changed to increasing numbers
    if (!skip_crc && block->guard != crc.crc((char*)&(block->d),sizeof(struct DHT<K,V>::dht_data))) {
      return POST_GET_LOCKED;
    }

    if (!(block->d.in_use)) {
      return POST_GET_MISSING;
    }

    return POST_GET_FOUND;
  }

  inline int post_get_extents(struct DHT<K,V>::dht_block* block, K key, size_t key_len, bool skip_crc = false /*, V* value*/) {
    int rval = post_contains_extents(block, key, key_len, skip_crc);
/*
	if (rval == POST_GET_FOUND) {
      *value = block->d.value;
    } else  {
      *value = NULL;
    }
*/
    return rval;
  }

  inline int post_contains_extents(struct DHT<K,V>::dht_block* block, K key, size_t key_len, bool skip_crc = false) {
    // This assumes post_get() was already called to check guards
    if (!skip_crc && block->d.crc != crc.crc(block->d.key,
        block->d.key_len+block->d.val_len))
    {
      return POST_GET_LOCKED;
    }
    if (!binarycmp(key,key_len,block->d.key,block->d.key_len)) {
      return POST_GET_COLLISION;
    }
    return POST_GET_FOUND;
  }

};

#endif // DHT_H


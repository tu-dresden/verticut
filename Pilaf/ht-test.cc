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
 *   ht-test.cc: Driver for benchmarking the   *
 *               raw dht hashtable imple-      *
 *               mentation for inserts, gets,  *
 *               and updates.                  *
 ***********************************************/

#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <boost/random.hpp>
#include <boost/random/normal_distribution.hpp>

#include "dht.h"
#include "time/get_clock.h"

#define uint64_t unsigned long long int

const int STRESS_COUNT = 100000L;
const int REPS = 2L;

void usage(const char *argv0);

// Testing variables
int role;

int main(int argc, char **argv) {

  if (argc < 3)
    usage(argv[0]);

  char test='a';

  size_t key_len = atoi(argv[1]);
  size_t val_len = atoi(argv[2]);

  if (argc >= 4)
      test = argv[3][0];

  printf("Generating %d k-v pairs of workload\n",STRESS_COUNT);
  std::vector<char*> keys;
  std::vector<char*> vals;

  boost::mt19937 rnga, rngb;
  boost::normal_distribution<> nd_key(key_len, key_len/2.f);
  boost::normal_distribution<> nd_val(val_len, val_len/2.f);

  boost::variate_generator<boost::mt19937&,
                           boost::normal_distribution<> > key_nor(rnga, nd_key);
  boost::variate_generator<boost::mt19937&,
                           boost::normal_distribution<> > val_nor(rngb, nd_val);

  for(int i=0; i<STRESS_COUNT; i++) {
    size_t klen = 0, vlen = 0;

    do {
      double klen_;
      klen_ = key_nor();
      if (klen_ <= 0)
        continue;
      klen = (size_t)klen_;
    } while (klen == 0);

    do {
      double vlen_;
      vlen_ = val_nor();
      if (vlen_ <= 0)
        continue;
      vlen = (size_t)vlen_;
    } while (vlen == 0);

    const char* charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    char* key = (char*)malloc(sizeof(char)*klen);
    char* val = (char*)malloc(sizeof(char)*vlen);

    for (int i=0; i<klen; i++) {
      key[i] = charset[rand() % (26+26+10)];
    }
    for (int i=0; i<vlen; i++) {
      val[i] = charset[rand() % (26+26+10)];
    }
    key[klen-1] = val[vlen-1] = '\0';

    keys.push_back(key);
    vals.push_back(val);
  }

  // Test-specific vars
  long long totaltime;
  unsigned int iters;
  double mintime;
  double maxtime;

  // Create new DHT
  DHT<KEY_TYPE,VAL_TYPE>* dht = new DHT<KEY_TYPE,VAL_TYPE>();

  // Sanity testing
  {
    std::vector<char*>::iterator itk = keys.begin();
    std::vector<char*>::iterator itv = vals.begin();

    for(; itk != keys.end(); itk++, itv++) {
      dht->insert(*itk,strlen(*itk),*itv,strlen(*itv));
      if (false == dht->contains(*itk,strlen(*itk))) {
        fprintf(stderr,"FAIL: Key '%s' not found after insert\n",*itk);
        exit(-1);
      }
      dht->remove(*itk,strlen(*itk));
      if (true == dht->contains(*itk,strlen(*itk))) {
        fprintf(stderr,"FAIL: Key '%s' found after remove\n",*itk);
        exit(-1);
      }
    }

    itk = keys.begin();
    itv = vals.begin();

    for(; itk != keys.end(); itk++, itv++) {
      dht->insert(*itk,strlen(*itk),*itv,strlen(*itv));
      if (false == dht->contains(*itk,strlen(*itk))) {
        fprintf(stderr,"FAIL: Key '%s' not found after insert\n",*itk);
        exit(-1);
      }
    }

    itk = keys.begin();
    itv = vals.begin();

    for(; itk != keys.end(); itk++, itv++) {
      dht->remove(*itk,strlen(*itk));
      if (true == dht->contains(*itk,strlen(*itk))) {
        fprintf(stderr,"FAIL: Key '%s' found after remove\n",*itk);
        exit(-1);
      }
    }

    fprintf(stderr,"Sanity check on insert()/contains()/remove() passed.\n");
    delete dht;
    dht = new DHT<KEY_TYPE,VAL_TYPE>();
  }

  // Test inserts
  {
    cycles_t stick, etick;
    cycles_t sstick, eetick;
    totaltime = 0;
    mintime = 9e99;
    maxtime = 0;

    std::vector<char*>::iterator itk = keys.begin();
    std::vector<char*>::iterator itv = vals.begin();

    sstick = get_cycles();
    for(; itk != keys.end(); itk++, itv++) {
      stick = get_cycles();
      dht->insert(*itk,strlen(*itk),*itv,strlen(*itv));
      etick = get_cycles();

      totaltime += (etick-stick);
      if ((etick-stick) > maxtime) maxtime = (etick-stick);
      if ((etick-stick) < mintime) mintime = (etick-stick);
    }
    eetick = get_cycles();

    // print results
    printf("------INSERT TEST-(NO PRE-SIZE)--------------------------\n");
    printf("min time [ticks]\tavg time [ticks]\tmax time [ticks]\n");

    printf("%8.3f\t%8.3f\t%8.3f\n",mintime,(double)totaltime/(double)(STRESS_COUNT),maxtime);
    //printf("Throughput: between %f and %f\n",
    //       ((double)STRESS_COUNT)*(double)CLOCKS_PER_SEC/((double)(eetick-sstick)),
    //       ((double)STRESS_COUNT)*(double)CLOCKS_PER_SEC/((double)totaltime));

    size_t size = dht->size_;
    size_t ext_size = dht->ext_size_;

    delete dht;
    dht = new DHT<KEY_TYPE,VAL_TYPE>();
    dht->resize(size);
    dht->resize_extents(ext_size);

    totaltime = 0;
    mintime = 9e99;
    maxtime = 0;

    itk = keys.begin();
    itv = vals.begin();

    sstick = get_cycles();
    for(; itk != keys.end(); itk++, itv++) {
      stick = get_cycles();
      dht->insert(*itk,strlen(*itk),*itv,strlen(*itv));
      etick = get_cycles();

      totaltime += (etick-stick);
      if ((etick-stick) > maxtime) maxtime = (etick-stick);
      if ((etick-stick) < mintime) mintime = (etick-stick);
    }
    eetick = get_cycles();

    // print results
    printf("------INSERT TEST-(PRE-SIZE)----------------------------\n");
    printf("min time [ticks]\tavg time [ticks]\tmax time [ticks]\n");

    printf("%8.3f\t%8.3f\t%8.3f\n",mintime,(double)totaltime/(double)(STRESS_COUNT),maxtime);
    //printf("Throughput: between %f and %f\n",
    //       ((double)STRESS_COUNT)*(double)CLOCKS_PER_SEC/((double)(eetick-sstick)),
    //       ((double)STRESS_COUNT)*(double)CLOCKS_PER_SEC/((double)totaltime));
   printf("-- Test passed --\n");
  }

  // Test updates
  {
    cycles_t stick, etick;
    cycles_t sstick, eetick;
    for(int j=0; j<REPS; j++) {
      totaltime = 0;
      mintime = 9e99;
      maxtime = 0;

      std::vector<char*>::iterator itk = keys.begin();
      std::vector<char*>::iterator itv = vals.begin();

      sstick = get_cycles();
      for(; itk != keys.end(); itk++, itv++) {
        stick = get_cycles();
        dht->insert(*itk,strlen(*itk),*itv,strlen(*itv));
        etick = get_cycles();

        totaltime += (etick-stick);
        if ((etick-stick) > maxtime) maxtime = (etick-stick);
        if ((etick-stick) < mintime) mintime = (etick-stick);
      }
      eetick = get_cycles();

      // print results
      if (j == 0) {
        printf("------UPDATE TEST----------------------------------------\n");
        printf("min time [us]\tavg time [us]\tmax time [us]\n");
      }

      printf("%8.3f\t%8.3f\t%8.3f\n",mintime,(double)totaltime/(double)(STRESS_COUNT),maxtime);
      //printf("Throughput: between %f and %f\n",
      //       ((double)STRESS_COUNT)*(double)CLOCKS_PER_SEC/((double)(eetick-sstick)),
      //       ((double)STRESS_COUNT)*(double)CLOCKS_PER_SEC/((double)totaltime));
    }
    printf("-- Test passed --\n");
  }

  // Test gets
  {
    cycles_t stick, etick;
    cycles_t sstick, eetick;
    int result;
    DHTClient<KEY_TYPE,VAL_TYPE>* dhtclient = new DHTClient<KEY_TYPE, VAL_TYPE>((void*)&(dht->buckets_[0]),dht->size_);

    for(int j=0; j<REPS; j++) {
      totaltime = 0;
      mintime = 9e99;
      maxtime = 0;

      int maxcollide = 0;
      size_t totalreads = 0;

      std::vector<char*>::iterator itk = keys.begin();
      std::vector<char*>::iterator itv = vals.begin();

      sstick = get_cycles();
      for(; itk != keys.end(); itk++, itv++) {

        int collision_offset = 0;
        stick = get_cycles();
re_server_read:
        void* addr = dhtclient->pre_get(*itk,strlen(*itk),collision_offset);
		totalreads++;
        DHT<KEY_TYPE,VAL_TYPE>::dht_block* dhtb = (DHT<KEY_TYPE,VAL_TYPE>::dht_block*)addr;

        // Figure out if this is the right thing, or what.
        char* valloc;
        result = dhtclient->post_get(dhtb,*itk,strlen(*itk),valloc);
        if (result == POST_GET_FOUND) {
          if (POST_GET_COLLISION == dhtclient->post_get_extents(dhtb,*itk,false)) {
            collision_offset++;
            if (collision_offset < CUCKOO_D)
              goto re_server_read;
            fprintf(stderr,"Fatal: (Collided) missing key '%s' during get test\n",*itk);
            exit(-1);
          }
          valloc = dhtb->d.value;
        } else {
          fprintf(stderr,"Fatal: Missing key '%s' during get test\n",*itk);
          exit(-1);
        }
        etick = get_cycles();


		if (maxcollide < collision_offset) maxcollide = collision_offset+1;

        totaltime += (etick-stick);
        if ((etick-stick) > maxtime) maxtime = (etick-stick);
        if ((etick-stick) < mintime) mintime = (etick-stick);
      }
      eetick = get_cycles();

      // print results
      if (j == 0) {
        printf("------GET TEST-------------------------------------------\n");
        printf("min time [us]\tavg time [us]\tmax time [us]\n");
      }

      printf("%8.3f\t%8.3f\t%8.3f\n",mintime,(double)totaltime/(double)(STRESS_COUNT),maxtime);
      printf("Avg reads = %3.2f, max for 1 k-v = %d\n",(double)totalreads/(double)STRESS_COUNT,maxcollide);
      //printf("Throughput: between %f and %f\n",
      //       ((double)STRESS_COUNT)*(double)CLOCKS_PER_SEC/((double)(eetick-sstick)),
      //       ((double)STRESS_COUNT)*(double)CLOCKS_PER_SEC/((double)totaltime));

    }
    printf("-- Test passed --\n");
  }
  // Tear down the client and exit
  exit(0);

}

void usage(const char *argv0) {
  fprintf(stderr, "usage: %s <mean_key_len> <mean_val_len> [a|i|u|g]\n",argv0);
  fprintf(stderr,"  [a|i|u|g]: (a)ll, (i)nsert, (u)pdate, (g)et\n");
  exit(1);
}


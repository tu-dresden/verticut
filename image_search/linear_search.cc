// Copyright (C) 2013, Peking University
// Author: Qi Chen (chenqi871025@gmail.com)
//
// Description:

#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>
#include <stdlib.h>
#include <signal.h>
#include <queue>
#include <bitset>
#include "image_search.pb.h"
#include "image_tools.h"
#include "args_config.h"
#include "memcached_proxy.h"
#include "pilaf_proxy.h"
#include <iostream>
using namespace std;
using namespace google;

int s_bits;
std::string search_code;
BaseProxy<protobuf::Message, protobuf::Message> *proxy_clt;

struct MAX {
public:
  int dist;
  uint32_t image_id;
};

bool operator<(const MAX &a,const MAX &b)
{
  return a.dist<b.dist;
}

void search_K_nearest_neighbors(int k) {
  // get nearest K images
  ID image_id;
  BinaryCode code;
  std::priority_queue<MAX> qmax;
  for (uint32_t i = 0; i < image_total; i++) {
    image_id.set_id(i);
    proxy_clt->get(image_id, code);
    MAX item;
    item.image_id = i;
    item.dist = compute_hamming_dist(code.code(), search_code);

    if (qmax.size() < k) {
      qmax.push(item);
    } else if (qmax.top().dist > item.dist) {
      qmax.pop();
      qmax.push(item);
    }
  }

  while (!qmax.empty()) {
    MAX item = qmax.top();
    qmax.pop();
    printf("Find image with id=%d and hamming_dist=%d\n", item.image_id, item.dist);
  }
}

int main (int argc, char *argv[]) {
  
  configure(argc, argv);

  /*
  uint32_t a[4] = {0};
  uint32_t b[4] = {0};
  
  a[0] = 1;
  b[0] = 0;
  b[1] = 1;
  a[2] = (1 << 30) + (1 << 29) + (1 << 28) + (1 << 27);

  BinaryCode code, code2;
  code.set_code(a, 16);
  code2.set_code(b, 16);

  cout<<code.code().substr(12, 16).length()<<endl;

  int dist1 = compute_hamming_dist(code.code().substr(0, 4), code2.code().substr(0, 4));
  
  //cout<<code.code()<<" ! "<<code2.code()<<endl;

  //cout<<dist1<<endl;
  int dist2 = compute_hamming_dist(code.code().substr(4, 4), code2.code().substr(4, 4));
  int dist3 = compute_hamming_dist(code.code().substr(8, 4), code2.code().substr(8, 4));
  int dist4 = compute_hamming_dist(code.code().substr(12, 4), code2.code().substr(12, 4));
  int dist = compute_hamming_dist(code.code(), code2.code());

  cout<<dist1<<" "<<dist2<<" "<<dist3<<" "<<dist4<<" total : "<<dist<<endl;
  */

  struct timeval start_time, end_time;  
  s_bits = binary_bits / n_tables;
  int search_times = 1;

  if(strcmp(server, "memcached") == 0)
    proxy_clt = new MemcachedProxy<protobuf::Message, protobuf::Message>;
  else
    proxy_clt = new PilafProxy<protobuf::Message, protobuf::Message>;

  proxy_clt->init(config_path);

  ID image_id;
  BinaryCode code;
  srand(34);
  
  gettimeofday(&start_time, NULL);
  for (int i = 0; i < search_times; i++) {
    image_id.set_id(rand() % image_total);
    proxy_clt->get(image_id, code);
    search_code = code.code();
    printf("search_image_id:%d code:%s\n", image_id.id(), binaryToString(search_code.c_str(), search_code.length()).c_str());
    search_K_nearest_neighbors(knn);
  }
  gettimeofday(&end_time, NULL);

  long long totaltime = (long long) (end_time.tv_sec - start_time.tv_sec) * 1000000
                          + (end_time.tv_usec - start_time.tv_usec);

  printf("Search finish! time cost:%fs", (double)totaltime);
  proxy_clt->close();
  delete proxy_clt;
  return 0;
}


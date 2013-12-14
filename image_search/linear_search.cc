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
#include "redis_proxy.h"
#include <iostream>
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

  s_bits = binary_bits / n_tables;
  int search_times = 1;

  if(strcmp(server, "memcached") == 0)
    proxy_clt = new MemcachedProxy<protobuf::Message, protobuf::Message>;
  else if(strcmp(server, "pilaf") == 0)
    proxy_clt = new PilafProxy<protobuf::Message, protobuf::Message>;
  else
    proxy_clt = new RedisProxy<protobuf::Message, protobuf::Message>;

  proxy_clt->init(config_path);

  srand(34);

  Image_List img_list;
  std::string code = "1234567812345678";
  HashIndex index;
  index.set_index(1000);
  index.set_table_id(1);

  for(int i = 0; i < 250000; ++i){
    ID_Code_Pair *pair = img_list.add_images();
    pair->set_id(i);
    pair->set_code(code.c_str(), code.size());
  }
  
  std::string str;
  img_list.SerializeToString(&str);
  Image_List tmp;
  tmp.ParseFromString(str);
  
  std::cout<<(str.size() / 1024.0 / 1024)<<std::endl;
  proxy_clt->put(index, img_list);

  /*for (int i = 0; i < search_times; i++) {
    image_id.set_id(rand() % image_total);
    proxy_clt->get(image_id, code);
    search_code = code.code();
    printf("search_image_id:%d code:%s\n", image_id.id(), binaryToString(search_code.c_str(), search_code.length()).c_str());
    search_K_nearest_neighbors(knn);
  }
  */

  proxy_clt->close();
  delete proxy_clt;
  return 0;
}

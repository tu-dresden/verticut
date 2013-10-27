// Copyright (C) 2013, Peking University
// Author: Qi Chen (chenqi871025@gmail.com)
//
// Description:

#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>
#include <stdlib.h>

#include "../Pilaf/table_types.h"
#include "../Pilaf/store-server.h"
#include "../Pilaf/store-client.h"
#include "../Pilaf/ibman.h"
#include "../Pilaf/dht.h"
#include "../Pilaf/config.h"
#include <signal.h>
#include <getopt.h>
#include <pthread.h>
#include <queue>
#include <bitset>
#include "image_search.pb.h"
#include "../Pilaf/image_tools.h"

#define IMAGE_COUNT 1000000
uint32_t image_count;
int table_count;
int substr_len;
int binary_bits;
int s_bits;
char* config_path;
std::bitset<IMAGE_COUNT> image_map;
read_modes read_mode;

// parameters used in multiple threads
int r;
std::string search_code;

struct MAX {
public:
  int dist;
  uint32_t image_id;
};

bool operator<(const MAX &a,const MAX &b)
{
  return a.dist<b.dist;
}

void enumerate_entry(uint32_t curr, int len, int rr, Client* c, HashIndex& idx) {

  if (rr == 0 || len == s_bits) {
    //printf("try entry:%d\n", curr);
    ImageList img_list;
    idx.set_index(curr);
    int rval = c->get_ext(idx, img_list);

    if (rval == POST_GET_FOUND) {
      for(int i = 0; i < img_list.images_size(); i++) {
        image_map.set(img_list.images(i));
      }
    }
  } else {
    enumerate_entry(curr^(1<<len), len+1, rr-1, c, idx);
    enumerate_entry(curr, len+1, rr, c, idx);
  }
}

void enumerate_image(int table_id, uint32_t search_index, Client* c) {
  ID image_id;
  BinaryCode code;

  int start_pos = table_id * substr_len;
  for (uint32_t i = 0; i < image_count; i++) {
      image_id.set_id(i);
      c->get_ext(image_id, code);

      std::string tmp_str = code.code().substr(start_pos, substr_len);
      uint32_t index = binaryToInt(tmp_str.c_str(), substr_len);

      int dist = __builtin_popcount(search_index ^ index);
      if (dist <= r) image_map.set(i);
  }
}

void *search_R_neighbors(void *arg) {
  int table_id = *((int *)arg);

  printf("begin search in table:%d\n", table_id);

  Client* c = new Client;
  if (c->setup())
    die("Failed to set up client");

  ConfigReader config(config_path);
  while(!config.get_end()) {
    struct server_info* this_server = config.get_next();
    if (c->add_server(this_server->host->c_str(),this_server->port->c_str()))
      die("Failed to add server");
  }

  c->set_read_mode(read_mode);
  c->ready();

  int start_pos = table_id * substr_len;
  std::string search_str = search_code.substr(start_pos, substr_len);
  uint32_t search_index = binaryToInt(search_str.c_str(), substr_len);

  // enum the index of table entry which has at most r bits different from the search index
  if ((1<<r) < image_count) {
    HashIndex idx;
    idx.set_table_id(table_id);
    enumerate_entry(search_index, 0, r, c, idx);
  } else {
    enumerate_image(table_id, search_index, c);
  }

  printf("end search in table:%d\n", table_id);
  c->teardown();
  delete(c);
}

void search_K_nearest_neighbors(int k, Client* c) {
  int* table_ids = new int [table_count];
  pthread_t* threads = new pthread_t [table_count];

  ID image_id;
  BinaryCode code;
  std::priority_queue<MAX> qmax;

  r = 0;
  while (qmax.size() < k) {
    qmax = std::priority_queue<MAX>();
    image_map.reset();

    for (int i = 0; i < table_count; i++) {
      table_ids[i] = i;
      pthread_create(&threads[i], NULL, search_R_neighbors, &table_ids[i]);
      pthread_join(threads[i], NULL);
    }

    //for (int i = 0; i < table_count; i++)
    //  pthread_join(threads[i], NULL);

    // get nearest K images
    ID image_id;
    BinaryCode code;
    for (uint32_t i = 0; i < image_count; i++) {
      if (image_map[i] == 1) {
        image_id.set_id(i);
        c->get_ext(image_id, code);

        MAX item;
        item.image_id = i;
        item.dist = compute_hamming_dist(code.code(), search_code);

        if (item.dist > r * table_count) continue;

        if (qmax.size() < k) {
          qmax.push(item);
        } else if (qmax.top().dist > item.dist) {
          qmax.pop();
          qmax.push(item);
        }
      }
    }

    printf("Find %lu nearest images with r=%d\n", qmax.size(), r);
    r++;
  }

  delete table_ids;
  delete threads;

  while (!qmax.empty()) {
    MAX item = qmax.top();
    qmax.pop();
    printf("Find image with id=%d and hamming_dist=%d\n", item.image_id, item.dist);
  }
}

void usage() {
  printf("./search_image <config_path> <image_count> <binary_bits> <substring_bits> <search_times> <K>\n");
}

int main (int argc, char *argv[]) {

  struct timeval start_time, end_time;
  read_mode = READ_MODE_RDMA;
  config_path = "dht-test.cnf";
  image_count = 10;

  binary_bits = 128;
  s_bits = 32;

  int search_times = 1;
  int k = 3;

  if (argc == 7) {
    config_path = argv[1];
    image_count = atoi(argv[2]);
    binary_bits = atoi(argv[3]);
    s_bits = atoi(argv[4]);
    search_times = atoi(argv[5]);
    k = atoi(argv[6]);
  } else if (argc == 2 && strcmp(argv[1], "--help") == 0) {
    usage();
    exit(0);
  } else if (argc == 3) {
    image_count = atoi(argv[1]);
    read_mode = (read_modes)atoi(argv[2]);
  }

  printf("Run with config_path=%s image_count=%d binary_bits=%d substring_bits=%d search_times=%d k=%d read_mode=%d\n",
         config_path, image_count, binary_bits, s_bits, search_times, k, read_mode);

  table_count = binary_bits / s_bits;
  substr_len = s_bits / 8 / sizeof(char);

  // generate image code for search
  Client* c = new Client;
  if (c->setup())
    die("Failed to set up client");

  ConfigReader config(config_path);
  while(!config.get_end()) {
    struct server_info* this_server = config.get_next();
    if (c->add_server(this_server->host->c_str(),this_server->port->c_str()))
      die("Failed to add server");
  }

  c->set_read_mode(read_mode);
  c->ready();

  ID image_id;
  BinaryCode code;
  srand(34);
  //srand(time(NULL));

  gettimeofday(&start_time, NULL);
  for (int i = 0; i < search_times; i++) {
    image_id.set_id(rand() % image_count);
    c->get_ext(image_id, code);
    search_code = code.code();
    printf("search_image_id:%d code:%s\n", image_id.id(), binaryToString(search_code.c_str(), search_code.length()).c_str());
    search_K_nearest_neighbors(k, c);
  }
  gettimeofday(&end_time, NULL);

  long long totaltime = (long long) (end_time.tv_sec - start_time.tv_sec) * 1000000
                          + (end_time.tv_usec - start_time.tv_usec);

  printf("Search finish! time cost:%fs", (double)totaltime);

  c->teardown();
  delete(c);
  return 0;
}


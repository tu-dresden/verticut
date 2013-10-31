// Copyright (C) 2013, Peking University
// Author: Qi Chen (chenqi871025@gmail.com)
//
// Description:

#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>
#include <stdlib.h>

#include "table_types.h"
#include "store-server.h"
#include "store-client.h"
#include "ibman.h"
#include "dht.h"
#include "config.h"
#include <signal.h>
#include <queue>
#include <bitset>
#include "image_search.pb.h"
#include "image_tools.h"

uint32_t image_count;
int binary_bits;
int s_bits;
char* config_path;
std::string search_code;
read_modes read_mode;

struct MAX {
public:
  int dist;
  uint32_t image_id;
};

bool operator<(const MAX &a,const MAX &b)
{
  return a.dist<b.dist;
}

void search_K_nearest_neighbors(int k, Client* c) {
  // get nearest K images
  ID image_id;
  BinaryCode code;
  std::priority_queue<MAX> qmax;
  for (uint32_t i = 0; i < image_count; i++) {
    image_id.set_id(i);
    c->get_ext(image_id, code);

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

  printf("Run with config_path=%s image_count=%d binary_bits=%d substring_bits=%d search_times=%d k=%d read_mode:%d\n",
         config_path, image_count, binary_bits, s_bits, search_times, k, read_mode);

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


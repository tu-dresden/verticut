// Copyright (C) 2013, Peking University
// Author: Qi Chen (chenqi871025@gmail.com)
//
// Description:

#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>

#include <boost/random.hpp>
#include <boost/random/normal_distribution.hpp>

#include "../Pilaf/table_types.h"
#include "../Pilaf/store-server.h"
#include "../Pilaf/store-client.h"
#include "../Pilaf/ibman.h"
#include "../Pilaf/dht.h"
#include "../Pilaf/config.h"
#include <signal.h>
#include <getopt.h>
#include <pthread.h>
#include "image_search.pb.h"
#include "../Pilaf/image_tools.h"

uint32_t image_count = 0;
int binary_bits = 128;
int s_bits = 32;

int substr_len;
char * config_path;
read_modes read_mode;

void load_binarycode(char * fname, char * config_path) {
  FILE* fh;
  if (NULL == (fh = fopen(fname,"r"))) {
    return;
  }

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

  while(!feof(fh)) {
    char code[17] = {'\0'};
    int read_bytes = fread((void *)code, binary_bits/8, 1, fh);
    if (read_bytes == 0) break;

    ID image_id;
    image_id.set_id(image_count);

    BinaryCode bcode;
    bcode.set_code(code, binary_bits/8);
    printf("id:%d code_length:%lu\n", image_count, bcode.code().size());
    c->put_ext(image_id, bcode);

    image_count++;
  }

  c->teardown();
  delete c;
  fclose(fh);
}

void *build_hash_tables(void *arg) {
  int table_id = *((int *)arg);

  printf("build hash table %d\n", table_id);

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
  HashIndex idx;
  ImageList img_list;

  idx.set_table_id(table_id);
  int start_pos = table_id * substr_len;

  for (uint32_t i = 0; i < image_count; i++) {
    image_id.set_id(i);
    c->get_ext(image_id, code);

    std::string tmp_str = code.code().substr(start_pos, substr_len);
    idx.set_index(binaryToInt(tmp_str.c_str(), substr_len));
    printf("table id%d, image id:%d, index:%d\n", table_id, i, idx.index());

    int rval = c->get_ext(idx, img_list);
    if (rval == POST_GET_FOUND) {
      img_list.add_images(i);
      c->put_ext(idx, img_list);
    } else {
      img_list.clear_images();
      img_list.add_images(i);
      c->put_ext(idx, img_list);
    }
  }

  c->teardown();
  delete(c);
}

void usage() {
  printf("./build_hash_tables <binarycode_path> <config_path> <binary_bits> <substring_bits>\n");
}

void dump_hashtables(int table_count) {
  FILE* fh;
  if (NULL == (fh = fopen("dump_hashtables.tmp","w"))) {
    return;
  }

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

  HashIndex idx;
  ImageList img_list;
  for (uint32_t table_id = 0; table_id < table_count; table_id++) {
    printf("dump hash table %d\n", table_id);
    fprintf(fh, "hashtable %d:\n", table_id);

    idx.set_table_id(table_id);
    int start_pos = table_id * substr_len;

    for (uint32_t i = 0; i < image_count; i++) {
      image_id.set_id(i);
      int rval = c->get_ext(image_id, code);

      std::string tmp_str = code.code().substr(start_pos, substr_len);
      idx.set_index(binaryToInt(tmp_str.c_str(), substr_len));
      c->get_ext(idx, img_list);

      uint32_t index = idx.index();
      fprintf(fh, "entry:%s img_list:", binaryToString((char*)(&index),sizeof(uint32_t)).c_str());
      for (int j = 0; j < img_list.images_size(); j++)
        fprintf(fh, "%d ", img_list.images(j));

      fprintf(fh, "\n");
    }
    fprintf(fh, "\n");
  }

  fclose(fh);
  c->teardown();
  delete(c);
}

void dump_binarycode() {

  printf("Dump binarycode to file dump_binarycode.tmp!\n");

  FILE* fh;
  if (NULL == (fh = fopen("dump_binarycode.tmp","w"))) {
    return;
  }

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

  for (uint32_t i = 0; i < image_count; i++) {
    image_id.set_id(i);
    c->get_ext(image_id, code);
    fprintf(fh, "%d %s\n", i, binaryToString(code.code().c_str(), code.code().length()).c_str());
  }

  fclose(fh);
  c->teardown();
  delete(c);
}

int main (int argc, char *argv[]) {

  struct timeval start_time, end_time;
  char * binarycode_path = "lsh.code";
  config_path = "dht-test.cnf";
  read_mode = READ_MODE_RDMA;

  if (argc == 5) {
    binarycode_path = argv[1];
    config_path = argv[2];

    binary_bits = atoi(argv[3]);
    s_bits = atoi(argv[4]);
  } else if (argc == 2 && strcmp(argv[1], "--help") == 0) {
    usage();
    exit(0);
  }

  printf("Run with binary_path=%s config_path=%s binary_bits=%d substring_bits=%d\n", binarycode_path, config_path, binary_bits, s_bits);

  // load binary code into memory
  gettimeofday(&start_time, NULL);
  load_binarycode(binarycode_path, config_path);
  gettimeofday(&end_time, NULL);

  long long totaltime = (long long) (end_time.tv_sec - start_time.tv_sec) * 1000000
                          + (end_time.tv_usec - start_time.tv_usec);

  printf("Load binary code finish! image count:%d time cost:%llus\n", image_count, totaltime);

  //dump_binarycode();
  //exit(0);

  // build multiple index hash table
  gettimeofday(&start_time, NULL);

  int m = binary_bits / s_bits;
  substr_len = s_bits / 8 / sizeof(char);

  int* table_ids = new int [m];
  pthread_t* threads = new pthread_t [m];
  for (int i = 0; i < m; i++) {
    table_ids[i] = i;
    pthread_create(&threads[i], NULL, build_hash_tables, &table_ids[i]);
    pthread_join(threads[i], NULL);
  }

  //for (int i = 0; i < m; i++)
  //  pthread_join(threads[i], NULL);

  gettimeofday(&end_time, NULL);
  totaltime = (long long) (end_time.tv_sec - start_time.tv_sec) * 1000000
                          + (end_time.tv_usec - start_time.tv_usec);
  printf("build hash tables finish! time cost:%llus\n", totaltime);

  //dump_hashtables(m);

  delete table_ids;
  delete threads;
  return 0;
}

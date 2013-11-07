// Copyright (C) 2013, Peking University
// Author: Qi Chen (chenqi871025@gmail.com)
//
// Description:

#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>
#include <getopt.h>
#include <pthread.h>
#include "image_search.pb.h"
#include "image_tools.h"
#include "memcached_proxy.h"
#include "redis_proxy.h"
#include "pilaf_proxy.h"
#include "args_config.h"
#include "mpi_coordinator.h"
#include "image_search_constants.h"

using namespace google;
int substr_len;

static BaseProxy<protobuf::Message, protobuf::Message> *proxy_clt;
static mpi_coordinator *coord;

void load_binarycode(const char * fname) {
  FILE* fh;
  int ret;

  if (NULL == (fh = fopen(fname,"r"))) {
    return;
  }

  while(!feof(fh)) {
    char code[17] = {'\0'};
    int read_bytes = fread((void *)code, binary_bits/8, 1, fh);
    if (read_bytes == 0) break;

    ID image_id;
    image_id.set_id(image_total);

    BinaryCode bcode;
    bcode.set_code(code, binary_bits/8);
    
    if(image_total % REPORT_SIZE == 0)
      printf("id:%d code_length:%lu\n", image_total, bcode.code().size());
    
    if(proxy_clt->put(image_id, bcode) != PROXY_PUT_DONE){
      printf("put fails\n");
      exit(0);
    }
    
    image_total++;
  }

  fclose(fh);
}

void build_hash_tables() {
  int table_id = coord->get_rank();
  printf("build hash table %d\n", table_id);

  ID image_id;
  BinaryCode code;
  HashIndex idx;
  ImageList img_list;

  idx.set_table_id(table_id);
  int start_pos = table_id * substr_len;

  for (uint32_t i = 0; i < image_total; i++) {
    image_id.set_id(i);
    
    assert(proxy_clt->get(image_id, code) == PROXY_FOUND);

    std::string tmp_str = code.code().substr(start_pos, substr_len);
    idx.set_index(binaryToInt(tmp_str.c_str(), substr_len));
      
    if(i % REPORT_SIZE == 0)
      printf("rank : %d, table id : %d, image id:%d, index:%d\n", coord->get_rank(), table_id, i, idx.index());

    int rval = proxy_clt->get(idx, img_list);

    if (rval == PROXY_FOUND) {
      img_list.add_images(i);
      assert(proxy_clt->put(idx, img_list) == PROXY_PUT_DONE);
    } else {
      img_list.clear_images();
      img_list.add_images(i);
      assert(proxy_clt->put(idx, img_list) == PROXY_PUT_DONE);
    }
  }
}

int main (int argc, char *argv[]) {
  
  mpi_coordinator::init(argc, argv);  
  configure(argc, argv);
  image_total = 0;

  coord = new mpi_coordinator();
  
  if(strcmp(server, "memcached") == 0)
    proxy_clt = new MemcachedProxy<protobuf::Message, protobuf::Message>;
  else if(strcmp(server, "pilaf") == 0)
    proxy_clt = new PilafProxy<protobuf::Message, protobuf::Message>;
  else  
    proxy_clt = new RedisProxy<protobuf::Message, protobuf::Message>;
  
  proxy_clt->init(config_path);

  if(coord->is_master())
    load_binarycode(binary_file);
  
  substr_len = binary_bits / n_tables / 8;
  coord->synchronize();
  coord->bcast(&image_total);

  build_hash_tables();
  
  proxy_clt->close();
  mpi_coordinator::finalize();

  delete proxy_clt;
  delete coord;
  return 0;
}

/*
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

    for (uint32_t i = 0; i < image_total; i++) {
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

  for (uint32_t i = 0; i < image_total; i++) {
    image_id.set_id(i);
    c->get_ext(image_id, code);
    fprintf(fh, "%d %s\n", i, binaryToString(code.code().c_str(), code.code().length()).c_str());
  }

  fclose(fh);
  c->teardown();
  delete(c);
}
*/

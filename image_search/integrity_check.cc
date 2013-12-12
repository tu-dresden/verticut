// Copyright (C) 2013, Peking University
// Author: Qi Chen (chenqi871025@gmail.com)
//
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

bool check_is_in(Image_List &img_list, uint32_t id, const char* code){
  
  for(int i = 0; i < img_list.images_size(); ++i){
    ID_Code_Pair pair = img_list.images(i); 
    
    if(pair.id() == id && strcmp(pair.code().c_str(), code) == 0)
      return true;
  }

  return false;
}

void run_check(const char * fname) {
  int table_id = coord->get_rank();
  FILE* fh;
  int ret;
  
  if (NULL == (fh = fopen(fname,"r"))) {
    fprintf(stderr, "Can't open file %s.", fname);
    return;
  }

  Image_List img_list;
  HashIndex idx;
  idx.set_table_id(table_id);
  int start_pos = table_id * substr_len;

  while(!feof(fh)) {
    char code[17] = {'\0'};
    int read_bytes = fread((void *)code, binary_bits/8, 1, fh);
    if (read_bytes == 0) break;
  
    uint32_t index = binaryToInt(code + start_pos, substr_len);
    idx.set_index(index);       
    
    int rval = proxy_clt->get(idx, img_list);
    assert(rval == PROXY_FOUND && check_is_in(img_list, image_total, code));

    if(image_total % REPORT_SIZE == 0)
      printf("rank : %d, table id : %d, image id:%d, index:%d\n", coord->get_rank(), table_id, image_total, idx.index());

    image_total++;
  }

  fclose(fh);
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
  substr_len = binary_bits / n_tables / 8;

  run_check(binary_file);
  
  proxy_clt->close();
  mpi_coordinator::finalize();

  delete proxy_clt;
  delete coord;
  return 0;
}

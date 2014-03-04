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

void load_binarycode(const char * fname) {
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
    
    if(image_total % REPORT_SIZE == 0)
      printf("rank : %d, table id : %d, image id:%d, index:%d\n", coord->get_rank(), table_id, image_total, idx.index());

    if(rval == PROXY_FOUND){
      ID_Code_Pair *pair = img_list.add_images();
      pair->set_id(image_total);
      pair->set_code(code, binary_bits/8);
      assert(proxy_clt->put(idx, img_list) == PROXY_PUT_DONE);
    }else{
      img_list.clear_images();
      ID_Code_Pair *pair = img_list.add_images();
      pair->set_id(image_total);
      pair->set_code(code, binary_bits/8);
      assert(proxy_clt->put(idx, img_list) == PROXY_PUT_DONE); 
    } 
    
    if(image_total == 120000000)
      break;

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

  load_binarycode(binary_file);
  
  proxy_clt->close();
  mpi_coordinator::finalize();

  delete proxy_clt;
  delete coord;
  return 0;
}

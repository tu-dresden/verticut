//Check the if the hash table is appropriatly build.
#include "memcached_proxy.h"
#include "pilaf_proxy.h"
#include "redis_proxy.h"
#include <iostream>
#include "mpi_coordinator.h"
#include "image_search.pb.h"
#include "args_config.h"
#include <string.h>
#include <stdlib.h>
#include "image_search_constants.h"

using namespace std;
using namespace google;

mpi_coordinator *coord;
BaseProxy<protobuf::Message, protobuf::Message> *proxy_clt;

bool check_idx(const ImageList &img_list, uint32_t id){
  for(int i = 0; i < img_list.images_size(); i++)
    if(img_list.images(i) == id)
      return true;

  return false;
}

void run_checking(){
  ID image_id;
  BinaryCode code;
  HashIndex idx;
  ImageList img_list;
  int substr_len = binary_bits / n_tables / 8;
  int ret;

  uint32_t range = image_total / coord->get_size();
  uint32_t start_pos, stop_pos;
  start_pos = coord->get_rank() * range;
  
  if(coord->get_rank() == coord->get_size() - 1)
    stop_pos = image_total;
  else
    stop_pos = start_pos + range;
  
  for(uint32_t i = start_pos; i < stop_pos; ++i){
    image_id.set_id(i);
    if(proxy_clt->get(image_id, code) != PROXY_FOUND){
      std::cerr<<"Failed to get image id : "<<i<<" from main table."<<std::endl;
      DIE
    }
    
    for(int t = 0; t < n_tables; ++t){
      idx.set_table_id(t);
      int start = t * substr_len;
      int search_index = binaryToInt(code.code().substr(start, substr_len).c_str(), substr_len);
      idx.set_index(search_index);
      img_list.clear_images();

      ret = proxy_clt->get(idx, img_list);
      if(ret == PROXY_NOT_FOUND){
        std::cerr<<"Can't find image id : "<<i<<"'s "<<t<<"th part in table "<<t<<std::endl;
        DIE
      }
      if(check_idx(img_list, i) == false){
        std::cerr<<"The "<<t<<"th part of image "<<i\
          <<" is not in the image list of sub-hash table."<<std::endl;
        DIE
      }
    }

    if(i % 10000 == 0)
      std::cout<<"Rank : "<<coord->get_rank()<<" finished "<<i - start_pos<<"/"<<stop_pos - start_pos<<\
        " part."<<std::endl;
  } 
}


int main(int argc, char* argv[]){
  configure(argc, argv);
  mpi_coordinator::init(argc, argv);
  
  if(strcmp(server, "memcached") == 0)
    proxy_clt = new MemcachedProxy<protobuf::Message, protobuf::Message>;
  else if(strcmp(server, "pilaf") == 0)
    proxy_clt = new PilafProxy<protobuf::Message, protobuf::Message>;
  else
    proxy_clt = new RedisProxy<protobuf::Message, protobuf::Message>;

  proxy_clt->init(config_path);
  coord = new mpi_coordinator; 

  run_checking();
  
  if(coord->is_master())
    std::cout<<"Finish integerity checking."<<std::endl;

  mpi_coordinator::finalize();
  proxy_clt->close();

  delete proxy_clt;
  delete coord;

  return 0;
}

#include "image_search_client.h"

image_search_client::image_search_client(std::string ip, uint16_t port){
  clt_ = new msgpack::rpc::client(ip, port);
}

image_search_client::~image_search_client(){
  if(clt_){
    delete clt_;
    clt_ = 0;
  }
}

std::string image_search_client::ping(const std::string &content){
  return clt_->call("ping", content).get<std::string>();
}

std::list<std::pair<uint32_t, uint32_t> > image_search_client::search_image_by_id(uint32_t id, 
    int kdd, 
    bool approximate){
  return clt_->call("search_image_by_id", id, kdd, approximate).get<std::list<std::pair<uint32_t, uint32_t> > >();
}

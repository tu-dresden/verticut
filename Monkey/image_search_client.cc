#include "image_search_client.h"

image_search_client::image_search_client(std::string ip, uint16_t port){
  ip_ = ip;
  port_ = port;
  clt_ = new msgpack::rpc::client(ip, port);
  pool_ = new msgpack::rpc::session_pool();
  pool_->start(20);
}

image_search_client::~image_search_client(){
  if(clt_){
    delete clt_;
    delete pool_;
    clt_ = 0;
  }
}

std::string image_search_client::ping(const std::string &content){
  return clt_->call("ping", content).get<std::string>();
}


std::list<std::pair<uint32_t, uint32_t> > image_search_client::search_image_by_id(uint32_t id, 
    int knn, 
    bool approximate){
  
  msgpack::rpc::session s = pool_->get_session(ip_, port_);
  s.set_timeout(120 * 4);
  return s.call("search_image_by_id", 
                      id, 
                      knn, 
                      approximate).get<std::list<std::pair<uint32_t, uint32_t> > >();
}

/* Key-value store proxy client for Pilaf 
 * Author : Yisheng Liao & Chenqi*/

#ifndef PILAF_PROXY
#define PILAF_PROXY
#include "base_proxy.h"
#include <string>
#include "store-client.h"
#include "config.h"
#define MAX_BUF_LEN 10000000

template<class K, class V>
class PilafProxy:public BaseProxy<K, V>{
  private:
    //Diable copy constructor.
    PilafProxy(const PilafProxy& p);
  
  protected:
    //We use this buffer to receive the data from server.
    //This is OK since Pilaf is not thread-safe itself.
    //So we won't get data from different threads.
    char buffer_[MAX_BUF_LEN];
    Client *clt_;

  public:
    PilafProxy();    
    int put(const K& key, const V& value);
    int get(const K& key, V& value);
    int init(const char* filename);
    int contain(const K& key);
    void close(); 
};

template<class K, class V>
PilafProxy<K, V>::PilafProxy(){
  clt_ = 0;
}

template<class K, class V>
int PilafProxy<K, V>::put(const K& key, const V& value){
  std::string k_str, v_str;
  key.SerializeToString(&k_str);
  value.SerializeToString(&v_str);
  
  int ret = clt_->put_with_size(k_str.c_str(), v_str.c_str(), k_str.size(), v_str.size());

  return (ret == 0)? PROXY_PUT_DONE : PROXY_PUT_FAIL;
}

template<class K, class V>
int PilafProxy<K, V>::get(const K& key, V& value){
  std::string k_str, v_str;
  key.SerializeToString(&k_str);
  size_t val_len;

  int ret = clt_->get_with_size(k_str.c_str(), buffer_, k_str.size(), val_len); 
  if(ret == POST_GET_FOUND){
    value.ParseFromString(std::string(buffer_, val_len));
    return PROXY_FOUND;
  }  
  return PROXY_NOT_FOUND;
}

template<class K, class V>
int PilafProxy<K, V>::init(const char* filename){
  clt_ = new Client();
  if(clt_->setup())
    return -1;

  ConfigReader config(filename);
  while(!config.get_end()) {
    struct server_info* this_server = config.get_next();
    if (clt_->add_server(this_server->host->c_str(),this_server->port->c_str()))
      return -1;
  }

  clt_->set_read_mode((read_modes)0);
  clt_->ready();
  return 0;
}

template<class K, class V>
int PilafProxy<K, V>::contain(const K& key){
  //Not implemented yet.
  return 0;
}

template<class K, class V>
void PilafProxy<K, V>::close(){
  if(clt_){
    clt_->teardown();
    delete clt_;
    clt_ = 0;
  } 
}
#endif

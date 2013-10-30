#ifndef PILAF_PROXY
#define PILAF_PROXY
#include "base_proxy.h"
#include <iostream>
#include <string>
#include "../Pilaf/table_types.h"
#include "../Pilaf/store-server.h"
#include "../Pilaf/store-client.h"
#include "../Pilaf/ibman.h"
#include "../Pilaf/dht.h"
#include "../Pilaf/config.h"
#define MAX_BUF_LEN 10000000
using namespace std;

template<class K, class V>
class PilafProxy:public BaseProxy<K, V>{
  protected:
    char m_buffer[MAX_BUF_LEN];
    Client *m_clt;

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
  m_clt = 0;
}

template<class K, class V>
int PilafProxy<K, V>::put(const K& key, const V& value){
  std::string k_str, v_str;
  key.SerializeToString(&k_str);
  value.SerializeToString(&v_str);
  
  int ret = m_clt->put_with_size(k_str.c_str(), v_str.c_str(), k_str.size(), v_str.size());

  return (ret == 0)? PROXY_PUT_DONE : PROXY_PUT_FAIL;
}

template<class K, class V>
int PilafProxy<K, V>::get(const K& key, V& value){
  std::string k_str, v_str;
  key.SerializeToString(&k_str);
  size_t val_len;

  int ret = m_clt->get_with_size(k_str.c_str(), m_buffer, k_str.size(), val_len); 
  if(ret == POST_GET_FOUND){
    value.ParseFromString(std::string(m_buffer, val_len));
    return PROXY_FOUND;
  }  
  return PROXY_NOT_FOUND;
}

template<class K, class V>
int PilafProxy<K, V>::init(const char* filename){
  m_clt = new Client();
  if(m_clt->setup())
    return -1;

  ConfigReader config(filename);
  while(!config.get_end()) {
    struct server_info* this_server = config.get_next();
    if (m_clt->add_server(this_server->host->c_str(),this_server->port->c_str()))
      return -1;
  }

  m_clt->set_read_mode((read_modes)0);
  m_clt->ready();
  return 0;
}

template<class K, class V>
int PilafProxy<K, V>::contain(const K& key){
  
  return 0;
}

template<class K, class V>
void PilafProxy<K, V>::close(){
  if(m_clt){
    m_clt->teardown();
    delete m_clt;
    m_clt = 0;
  } 
}
#endif

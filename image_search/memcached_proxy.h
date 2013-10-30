#ifndef MEMCACHED_PROXY
#define MEMCACHED_PROXY
#include "base_proxy.h"
#include <libmemcached/memcached.h>
#include <string.h>
#include <fstream>
#include <iostream>
#include <sstream>
using namespace std;

template<class K, class V>
class MemcachedProxy:public BaseProxy<K, V>{
  protected:
    memcached_st* m_clt;

  public:
    MemcachedProxy();    
    int put(const K& key, const V& value);
    int get(const K& key, V& value);
    int init(const char* filename);
    int contain(const K& key);
    void close();
};

template<class K, class V>
MemcachedProxy<K, V>::MemcachedProxy(){
  m_clt = 0;
}

template<class K, class V>
int MemcachedProxy<K, V>::put(const K& key, const V& value){ 
  std::string k_str, v_str;
  key.SerializeToString(&k_str);
  value.SerializeToString(&v_str);
   
  memcached_return_t ret = memcached_set(m_clt, k_str.c_str(), k_str.size(), v_str.c_str(), v_str.size(), 0, 0); 

  return (ret == MEMCACHED_SUCCESS)? PROXY_PUT_DONE : PROXY_PUT_FAIL;
}

template<class K, class V>
int MemcachedProxy<K, V>::get(const K& key, V& value){
  std::string k_str;
  key.SerializeToString(&k_str);
  size_t val_len;
  memcached_return_t ret;
  char *buffer;
   
  buffer = memcached_get(m_clt, k_str.c_str(), k_str.size(), &val_len, 0, &ret);
  
  if(buffer != 0){
    value.ParseFromString(std::string(buffer, val_len));
    free(buffer);
    return PROXY_FOUND;
  }
  
  return PROXY_NOT_FOUND;
}

template<class K, class V>
int MemcachedProxy<K, V>::init(const char* filename){
  std::ifstream fin(filename);
  if(!fin.is_open())
    return -1;
  
  char addr[1024];
  int port = 0;
  std::string ip;
  memcached_return_t ret;
  m_clt = memcached_create(0);

  while(fin.getline(addr, 1024)){
    port = MEMCACHED_DEFAULT_PORT;
    istringstream sin(addr);
    sin>>ip>>port;
    ret = memcached_server_add(m_clt, ip.c_str(), port);
    if(ret != MEMCACHED_SUCCESS)
      return -1;
  }
  
  return 0;
}

template<class K, class V>
int MemcachedProxy<K, V>::contain(const K& key){  
  //Not implemented yet.
  assert(false); 
  return 0;
}

template<class K, class V>
void MemcachedProxy<K, V>::close(){
  if(m_clt)
    memcached_free(m_clt);
}
#endif

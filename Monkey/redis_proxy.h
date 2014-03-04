/* A redis proxy client
 * Author: Yisheng Liao & Chenqi */
#ifndef REDIS_PROXY
#define REDIS_PROXY
#include "base_proxy.h"
#include "redisclient.h"
#include <string.h>
#include <fstream>
#include <sstream>
#include <iostream>

using namespace std;
#define REDIS_DEFAULT_PORT 6379
#define MISSING_VALUE "**nonexistent-key**"

template<class K, class V>
class RedisProxy:public BaseProxy<K, V>{
  private:
    //Disable copy constructor.
    RedisProxy(const RedisProxy &m);
  
  protected:
    redis::client *clt_;

  public:
    RedisProxy();    
    int put(const K& key, const V& value);
    int get(const K& key, V& value);
    int init(const char* filename);
    int contain(const K& key);
    void close();
};

template<class K, class V>
RedisProxy<K, V>::RedisProxy(){
  clt_ = 0;
}

template<class K, class V>
int RedisProxy<K, V>::put(const K& key, const V& value){ 
  std::string k_str, v_str;
  key.SerializeToString(&k_str);
  value.SerializeToString(&v_str);
  
  clt_->set(k_str, v_str);

  return PROXY_PUT_DONE;
}

template<class K, class V>
int RedisProxy<K, V>::get(const K& key, V& value){
  std::string k_str;
  std::string v_str;
  key.SerializeToString(&k_str);
  size_t val_len;
    
  v_str = clt_->get(k_str);
  
  if(v_str == MISSING_VALUE)
    return PROXY_NOT_FOUND;

  value.ParseFromString(v_str);

  return PROXY_FOUND;
}

template<class K, class V>
int RedisProxy<K, V>::init(const char* filename){
  std::ifstream fin(filename);
  if(!fin.is_open())
    return -1;
  
  char addr[1024];
  int port = 0;
  std::string ip;
  redis::connection_data con;
  std::vector<redis::connection_data> redis_servers;

  while(fin.getline(addr, 1024)){
    port = REDIS_DEFAULT_PORT;
    std::istringstream sin(addr);
    sin>>ip>>port;
    
    con.host = ip;
    con.port = port;
    con.dbindex = 14; //Not sure what it means, but it works.
    redis_servers.push_back(con);
  }
  
  clt_ = new redis::client(redis_servers.begin(), redis_servers.end());

  return 0;
}

template<class K, class V>
int RedisProxy<K, V>::contain(const K& key){  
  //Not implemented yet.
  return 0;
}

template<class K, class V>
void RedisProxy<K, V>::close(){
  if(clt_ != 0){
    delete clt_;
    clt_ = 0;
  } 
}
#endif

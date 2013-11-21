/* Key-value store proxy client for local map 
 * Author : Yisheng Liao & Chenqi*/

#ifndef MAP_PROXY
#define MAP_PROXY
#include "base_proxy.h"
#include <string>
#include "store-client.h"
#include "config.h"
#include <map>

template<class K, class V>
class MapProxy:public BaseProxy<K, V>{
  private:
    //Diable copy constructor.
    MapProxy(const MapProxy& p);
  
  protected:
    std::map<std::string, std::string> map_;

  public:
    MapProxy();    
    int put(const K& key, const V& value);
    int get(const K& key, V& value);
    int init(const char* filename);
    int contain(const K& key);
    void close(); 
};

template<class K, class V>
MapProxy<K, V>::MapProxy(){
}

template<class K, class V>
int MapProxy<K, V>::put(const K& key, const V& value){
  std::string k_str, v_str;
  key.SerializeToString(&k_str);
  value.SerializeToString(&v_str);
  
  map_[k_str] = v_str;

  return PROXY_PUT_DONE;
}

template<class K, class V>
int MapProxy<K, V>::get(const K& key, V& value){
  std::string k_str, v_str;
  key.SerializeToString(&k_str);
  
  if(map_.find(k_str) != map_.end()){
    std::string v_str = map_[k_str];
    value.ParseFromString(v_str);
    return PROXY_FOUND;
  }

  return PROXY_NOT_FOUND;
}

template<class K, class V>
int MapProxy<K, V>::init(const char* filename){
  //Do nothing
  return 0;
}

template<class K, class V>
int MapProxy<K, V>::contain(const K& key){
  //Not implemented yet.
  return 0;
}

template<class K, class V>
void MapProxy<K, V>::close(){
  map_.clear();
}
#endif

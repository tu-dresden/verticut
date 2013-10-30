#ifndef MEMCACHED_PROXY
#define MEMCACHED_PROXY
#include "base_proxy.h"
#include<iostream>
using namespace std;

template<class K, class V>
class MemcachedProxy:public BaseProxy<K, V>{
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

}

template<class K, class V>
int MemcachedProxy<K, V>::put(const K& key, const V& value){
  cout<<"Memcached put"<<endl;
  return 0;
}

template<class K, class V>
int MemcachedProxy<K, V>::get(const K& key, V& value){
  cout<<"Memcached get"<<endl; 
  return 0;
}

template<class K, class V>
int MemcachedProxy<K, V>::init(const char* filename){

  return 0;
}

template<class K, class V>
int MemcachedProxy<K, V>::contain(const K& key){

  return 0;
}

template<class K, class V>
void MemcachedProxy<K, V>::close(){

}
#endif

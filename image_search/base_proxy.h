#ifndef CLIENT_BASE_PROXY
#define CLIENT_BASE_PROXY

#define PROXY_FOUND 0
#define PROXY_NOT_FOUND 1
#define PROXY_PUT_DONE 0
#define PROXY_PUT_FAIL 1

template<class K, class V>
class BaseProxy{
  public:
    virtual int get(const K& key, V& value) = 0;

    virtual int put(const K& key, const V& value) = 0;

    virtual int contain(const K& key) = 0;
    
    virtual int init(const char* filename) = 0;
  
    virtual void close() = 0;
};

#endif

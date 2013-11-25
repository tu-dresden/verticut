#ifndef IMAGE_SEARCH_CLIENT_H
#define IMAGE_SEARCH_CLIENT_H
//Image client, which is just a simple wrapper on rpc client
#include <msgpack/rpc/client.h>
#include <string>
#include <iostream>
#include <stdint.h>
#include <list>

class image_search_client{
  protected:
    msgpack::rpc::client* clt_;

  public:
    image_search_client(std::string ip, uint16_t port);
    ~image_search_client();
    std::string ping(const std::string &);
    std::list<std::pair<uint32_t, uint32_t> > search_image_by_id(uint32_t id, int kdd, bool approximate = false);
};

#endif

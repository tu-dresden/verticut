#ifndef IMAGE_SEARCH_SERVER_H
#define IMAGE_SEARCH_SERVER_H
#include <msgpack/rpc/server.h>
#include <iostream>
#include <string>
#include <list>
#include <vector>
#include "image_search_constants.h"

#define SEARCH_COMMAND "ssh %s \"cd ~/workplace/image_search/image_search/; ./run_distributed_search.py\"\
  -i 100000000 -q %d -k %d %s"

class image_search_server : public msgpack::rpc::server::base{
  protected:
    std::vector<std::string> workers_;
 
    void ping(msgpack::rpc::request req, std::string& s);
    void search_image_by_id(msgpack::rpc::request req, uint32_t id, uint32_t knn, bool approximate);
    std::list<std::pair<uint32_t, uint32_t> > wait_and_parse(FILE* f);

  public:
    void dispatch(msgpack::rpc::request req);
    void init_workers(const std::string& config_file = DEFAULT_WORKERS_CONFIG);

};

#endif

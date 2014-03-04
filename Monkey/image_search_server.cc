#include "image_search_server.h"
#include <fstream>
#include <assert.h>
#include <unistd.h>

#define BUFFER_SIZE 4096

//Read workers' address from configure file.
void image_search_server::init_workers(const std::string& config_file){
  srand(getpid());
  std::ifstream fin(config_file.c_str());
  std::string hostname;
  assert(fin.is_open());
  
  while(fin>>hostname && !fin.eof()){
    workers_.push_back(hostname);
  }
  
  fin.close();
}

void image_search_server::dispatch(msgpack::rpc::request req)
try{
  std::string method;
  req.method().convert(&method);
  
  if(method == "ping"){
    msgpack::type::tuple<std::string> params;
    req.params().convert(&params);
    ping(req, params.get<0>());
  }
  else if(method == "search_image_by_id"){
    msgpack::type::tuple<uint32_t, uint32_t, bool> params;
    req.params().convert(&params);
    search_image_by_id(req, params.get<0>(), params.get<1>(), params.get<2>()); 
  }
  else{
    req.error(msgpack::rpc::NO_METHOD_ERROR);
  }
}
catch (msgpack::type_error& e){
  req.error(msgpack::rpc::ARGUMENT_ERROR);
  return;
}
catch (std::exception &e){
  req.error(std::string(e.what()));
  return;
}


//ping rpc call
void image_search_server::ping(msgpack::rpc::request req, std::string& s){
  req.result(s);
}

//Randomly pick a worker machine and launch searching process on that machine, return until the
//searching is finished.
void image_search_server::search_image_by_id(msgpack::rpc::request req, 
    uint32_t id, 
    uint32_t knn, 
    bool approximate){
  
  char command[4096];
  int idx = rand() % workers_.size(); //randomly pick a worker.
  
  if(approximate)
    sprintf(command, SEARCH_COMMAND, workers_[idx].c_str(), id, knn, "-a");
  else
    sprintf(command, SEARCH_COMMAND, workers_[idx].c_str(), id, knn, "");
  
  std::cout<<command<<std::endl;
  
  FILE* f = popen(command, "r");
  
  if(f == 0){
    req.error(std::string("can't launch searching process."));
    return;
  }
  
  req.result(wait_and_parse(f));
  fclose(f);
  printf("finish query for %u\n", id);
}

//Wait the child process finish and parse the output of the child process.
std::list<std::pair<uint32_t, uint32_t> > image_search_server::wait_and_parse(FILE* f){
  char buffer[BUFFER_SIZE]; 
  std::list<std::pair<uint32_t, uint32_t> > result;
  std::pair<uint32_t, uint32_t> info;
  uint32_t id;
  uint32_t dist;

  while(fgets(buffer, sizeof(buffer), f) != 0){
    if(sscanf(buffer, "%u : %u", &id, &dist) == 2){
      info.first = id;
      info.second = dist;
      result.push_back(info);
    }
  }
  
  return result;
}

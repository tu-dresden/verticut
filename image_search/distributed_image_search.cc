// Copyright (C) 2013, Peking University & New York University
// Author: Qi Chen (chenqi871025@gmail.com) & Yisheng Liao(eason.liao@nyu.edu)
#include <iostream>
#include "mpi_coordinator.h"
#include <string.h>
#include "image_search.pb.h"
#include <vector>
#include <algorithm>
#include "pilaf_proxy.h"
#include "memcached_proxy.h"
#include <map>
#include <iostream>

using namespace std;
using namespace google;

static mpi_coordinator* coord;
static BaseProxy<protobuf::Message, protobuf::Message>* proxy_clt;
static uint32_t image_count;
static int table_count;
static int binary_bits;
static int n_local_bits;
static read_modes read_mode;
static int k;
static int n_local_bytes;
static char* config_path;
static std::map<int, bool> knn_found;

struct MAX {
public:
  int dist;
  uint32_t image_id;
};

bool operator<(const MAX &a,const MAX &b)
{
  return a.dist<b.dist;
}

void cleanup();
void setup(int argc, char* argv[]);

//Enumerate all the entries.
void enumerate_entry(uint32_t curr, int len, int rr, HashIndex& idx, int& count, 
    std::vector<int> &kn_candidates){ 
  if (rr == 0) {
    count++;
    ImageList img_list;
    idx.set_index(curr);
    int rval = proxy_clt->get(idx, img_list);
    
    if (rval == PROXY_FOUND) {
      for(int i = 0; i < img_list.images_size(); i++){
        if(img_list.images(i) < image_count){
          kn_candidates.push_back(img_list.images(i));
        }
      }
    }
  }else {
    enumerate_entry(curr^(1<<len), len+1, rr-1, idx, count, kn_candidates);
    if(n_local_bits - len > rr)
      enumerate_entry(curr, len+1, rr, idx, count, kn_candidates);
  }
}

//Enumerate all the images.
void enumerate_image(int table_id, uint32_t search_index, int r, std::vector<int> &kn_candidates) {
  ID image_id;
  BinaryCode code;

  int start_pos = table_id * n_local_bytes;
  for (uint32_t i = 0; i < image_count; i++) {
      image_id.set_id(i);
      proxy_clt->get(image_id, code);

      std::string tmp_str = code.code().substr(start_pos, n_local_bytes);
      uint32_t index = binaryToInt(tmp_str.c_str(), n_local_bytes);

      int dist = __builtin_popcount(search_index ^ index);
      if (dist == r) kn_candidates.push_back(i);
  }
}

int search_R_neighbors(int r, uint32_t search_index, 
    std::vector<int>& kn_candidates){ 
  int count = 0; 
  // enumerate the index of table entry which has at most r bits different 
  // from the search index
  if (1) {
    HashIndex idx;
    idx.set_table_id(coord->get_rank());
    enumerate_entry(search_index, 0, r, idx, count, kn_candidates);
  }else //OK, if until now we still can't find KNN, just do linear search.
    enumerate_image(coord->get_rank(), search_index, r, kn_candidates);
}

//Find the K nearest neighbors
int search_K_nearest_neighbors(int k, std::string query_code){
  ID image_id;
  BinaryCode code;
  std::priority_queue<MAX> qmax;
  int radius = 0; //Current searching radius.
  std::vector<int> kn_candidates; //KNN candidates for current searching radius.

  int start_pos = coord->get_rank() * n_local_bytes; 
  std::string local_query_code = query_code.substr(start_pos, n_local_bytes);
  uint32_t search_index = binaryToInt(local_query_code.c_str(), n_local_bytes);  
  int is_stop = 0;

  HashIndex idx;
  ImageList img_list;
  idx.set_table_id(coord->get_rank());
  idx.set_index(search_index); 

  while(!is_stop && radius <= n_local_bits){ 
    //Clear kn_candidates
    kn_candidates.clear();
    kn_candidates.reserve(8192);
    search_R_neighbors(radius, search_index, kn_candidates);
    
    vector<int> gathered_vector = coord->gather_vectors(kn_candidates);

    if(coord->is_master()){
      sort(gathered_vector.begin(), gathered_vector.end()); 
      //Eliminate duplicates.
      vector<int>::iterator iter = unique(gathered_vector.begin(), gathered_vector.end());
      gathered_vector.resize(distance(gathered_vector.begin(), iter));
      
      BinaryCode code;
      ID image_id;

      for(uint32_t i = 0; i < gathered_vector.size(); ++i){
        int id = gathered_vector[i];
        
        if(knn_found.find(id) != knn_found.end())
          continue;

        image_id.set_id(id);
        if(proxy_clt->get(image_id, code) != PROXY_FOUND)
          mpi_coordinator::die("No corresponding image found.\n");
        
        MAX item;
        item.image_id = id;
        item.dist = compute_hamming_dist(code.code(), query_code);
        
        knn_found[id] = 1;

        if (qmax.size() < k) {
          qmax.push(item);
        }else if (qmax.top().dist > item.dist) {
          qmax.pop();
          qmax.push(item);
        }
      }
    }

    radius += 1; 
    //If the mininum distance next epoch we may find is less than the max one of 
    //what we've found, then stop.
    if(coord->is_master() && qmax.size() == k && qmax.top().dist < radius * 4)
      is_stop = 1;

    coord->bcast(&is_stop);
  }

  if(coord->is_master())
    while (!qmax.empty()) {
      MAX item = qmax.top();
      qmax.pop();
      printf("Find image with id=%d and hamming_dist=%d\n", item.image_id, item.dist);
    }
}

void run(){   
  ID image_id;
  BinaryCode code;
  table_count = binary_bits / n_local_bits;
  n_local_bytes = n_local_bits / 8;
  
  //The number of mpi workers must equal to number of tables.
  if(table_count != coord->get_size())
    mpi_coordinator::die("The number of table must equals to the number of mpi processes.");
  
  srand(34);
  int query_image;
    
  //Just random pick one image and search its neighbors.
  if(coord->is_master())
    query_image = rand() % image_count;
 
  //Broadcast to all the workers.
  coord->bcast(&query_image);
  
  image_id.set_id(query_image);
  if(proxy_clt->get(image_id, code) != PROXY_FOUND)
    mpi_coordinator::die("Can't find match\n");
  
  std::string query_code = code.code();
  search_K_nearest_neighbors(k, query_code);
}


int main(int argc, char* argv[]){  
  setup(argc, argv); 
  run(); 
  cleanup();
  return 0;
}

//Clean up code.
void cleanup(){
  if(coord != 0){
    delete coord;
    coord = 0;
  }
  if(proxy_clt != 0){
    proxy_clt->close();
    delete proxy_clt;
    proxy_clt = 0;
  }
  mpi_coordinator::finalize(); 
}

//Set up code. The arguments should be passed by bootstrap script(run_distributed_search.py) 
void setup(int argc, char* argv[]){
  if(argc != 8)
    mpi_coordinator::die("Incorrect number of arguments!");
  
  config_path = argv[1];
  image_count = atoi(argv[2]);
  binary_bits = atoi(argv[3]);
  n_local_bits = atoi(argv[4]);
  k = atoi(argv[5]);
  read_mode = (read_modes)atoi(argv[7]);

  mpi_coordinator::init(argc, argv);
  coord = new mpi_coordinator;
  
  if(strcmp(argv[6], "pilaf") == 0)
    proxy_clt = new PilafProxy<protobuf::Message, protobuf::Message>;
  else if(strcmp(argv[6], "memcached") == 0)
    proxy_clt = new MemcachedProxy<protobuf::Message, protobuf::Message>;
  else
    mpi_coordinator::die("Unrecognized server type.");

  proxy_clt->init(config_path);
} 

#include <iostream>
#include "mpi_coordinator.h"
#include <string.h>
#include "image_search.pb.h"
#include <vector>
#include <algorithm>
#include "pilaf_proxy.h"
#include "memcached_proxy.h"

using namespace std;
using namespace google;

static mpi_coordinator* coord;
static BaseProxy<protobuf::Message, protobuf::Message>* proxy_clt;
static uint32_t image_count;
static int table_count;
static int binary_bits;
static int s_bits;
static read_modes read_mode;
static int k;
static int substr_len;
static char* config_path;

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

void enumerate_entry(uint32_t curr, int len, int rr, HashIndex& idx, int& count, 
    std::vector<int> &kn_candidates){ 
  if (rr == 0) {
    count++;
    ImageList img_list;
    idx.set_index(curr);
    int rval = proxy_clt->get(idx, img_list);
    
    if (rval == PROXY_FOUND) {
      for(int i = 0; i < img_list.images_size(); i++){
        kn_candidates.push_back(img_list.images(i));
      }
    }
  }else {
    enumerate_entry(curr^(1<<len), len+1, rr-1, idx, count, kn_candidates);
    if(s_bits - len > rr)
      enumerate_entry(curr, len+1, rr, idx, count, kn_candidates);
  }
}

void enumerate_image(int table_id, uint32_t search_index, int r, std::vector<int> &kn_candidates) {
  ID image_id;
  BinaryCode code;

  int start_pos = table_id * substr_len;
  for (uint32_t i = 0; i < image_count; i++) {
      image_id.set_id(i);
      proxy_clt->get(image_id, code);

      std::string tmp_str = code.code().substr(start_pos, substr_len);
      uint32_t index = binaryToInt(tmp_str.c_str(), substr_len);

      int dist = __builtin_popcount(search_index ^ index);
      if (dist == r) kn_candidates.push_back(i);
  }
}

int search_R_neighbors(int r, uint32_t search_index, std::vector<int>& kn_candidates){ 
  int count = 0; 
  // enum the index of table entry which has at most r bits different from the search index
  if ((1<<r) < image_count) {
    HashIndex idx;
    idx.set_table_id(coord->get_rank());
    enumerate_entry(search_index, 0, r, idx, count, kn_candidates);
  }else
    enumerate_image(coord->get_rank(), search_index, r, kn_candidates);
}

//Find the K nearest neighbors
int search_K_nearest_neighbors(int k, std::string query_code){
  ID image_id;
  BinaryCode code;
  int total_find = 0;
  std::priority_queue<MAX> qmax;
  int radius = 0;
  std::vector<int> kn_candidates;

  int start_pos = coord->get_rank() * substr_len; 
  std::string local_query_code = query_code.substr(start_pos, substr_len);
  uint32_t search_index = binaryToInt(local_query_code.c_str(), substr_len);  
    
  HashIndex idx;
  ImageList img_list;
  idx.set_table_id(coord->get_rank());
  idx.set_index(search_index); 

  while(total_find < k && radius < s_bits){ 
    //Clear kn_candidates
    kn_candidates.clear();
    kn_candidates.reserve(4096);
    search_R_neighbors(radius, search_index, kn_candidates);
    
    vector<int> gathered_vector = coord->gather_vectors(kn_candidates);

    if(coord->is_master()){
      sort(gathered_vector.begin(), gathered_vector.end()); 
      vector<int>::iterator iter = unique(gathered_vector.begin(), gathered_vector.end());
      gathered_vector.resize(distance(gathered_vector.begin(), iter));
      
      BinaryCode code;
      ID image_id;

      for(uint32_t i = 0; i < gathered_vector.size(); ++i){
        image_id.set_id(gathered_vector[i]);
        if(proxy_clt->get(image_id, code) != PROXY_FOUND)
          mpi_coordinator::die("No corresponding image found.\n");
        
        MAX item;
        item.image_id = gathered_vector[i];
        item.dist = compute_hamming_dist(code.code(), query_code);
        
        if(item.dist > radius * table_count || item.image_id >= image_count) continue;
         
        if (qmax.size() < k) {
          qmax.push(item);
        }else if (qmax.top().dist > item.dist) {
          qmax.pop();
          qmax.push(item);
        }
      }
      total_find = qmax.size();
    }
    coord->bcast(&total_find);
    radius += 1;
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
  //printf("Run with config_path=%s image_count=%d binary_bits=%d substring_bits=%d k=%d\ 
  //    read_mode=%d\n", config_path, image_count, binary_bits, s_bits, k, read_mode); 
  table_count = binary_bits / s_bits;
  substr_len = s_bits / 8 / sizeof(char);
  
  if(table_count != coord->get_size())
    mpi_coordinator::die("The number of table must equals to the number of mpi processes.");
  
  srand(34);
  int query_image;
    
  if(coord->is_master())
    query_image = rand() % image_count;
  
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

void setup(int argc, char* argv[]){
  if(argc != 7)
    mpi_coordinator::die("Incorrect number of arguments!");
  
  config_path = argv[1];
  image_count = atoi(argv[2]);
  binary_bits = atoi(argv[3]);
  s_bits = atoi(argv[4]);
  k = atoi(argv[5]);
  read_mode = (read_modes)atoi(argv[6]);

  mpi_coordinator::init(argc, argv);
  coord = new mpi_coordinator;
  
  proxy_clt = new MemcachedProxy<protobuf::Message, protobuf::Message>;
  proxy_clt->init(config_path);
} 

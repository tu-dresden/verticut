#include <iostream>
#include "mpi_coordinator.h"
#include <bitset>
#include <string.h>
#include "image_search.pb.h"
#include "img_bitmap.h"
#include "../Pilaf/table_types.h"
#include "../Pilaf/store-server.h"
#include "../Pilaf/store-client.h"
#include "../Pilaf/ibman.h"
#include "../Pilaf/dht.h"
#include "../Pilaf/config.h"
#include <vector>
using namespace std;

static mpi_coordinator* coord;
static Client* clt;
static uint32_t image_count;
static int table_count;
static int binary_bits;
static int s_bits;
static int radius;
static std::string search_code;
static read_modes read_mode;
static int* bitmap;
static int* bitmap_back;
static int bitmap_count;
static int k = 3;
static int substr_len;
static char* config_path = "dht-test.cnf";

void cleanup();
void setup(int argc, char* argv[]);

uint8_t check_bitmap(int* bmp, int idx){
  int w_idx = idx / 32;
  int b_idx = idx % 32;
  return bmp[w_idx] & (1 << b_idx);
}


void enumerate_entry(uint32_t curr, int len, int rr, HashIndex& idx, int& count) { 
  if (rr == 0 || len == s_bits) {
    count++;
    ImageList img_list;
    idx.set_index(curr);
    int rval = clt->get_ext(idx, img_list);

    if (rval == POST_GET_FOUND) {
      for(int i = 0; i < img_list.images_size(); i++) {
        //image_map.set(img_list.images(i));
      }
    }
  } else {
    enumerate_entry(curr^(1<<len), len+1, rr-1, idx, count);
    enumerate_entry(curr, len+1, rr, idx, count);
  }
}

int search_R_neighbors(int r, int search_index){ 
  int count = 0; 
  // enum the index of table entry which has at most r bits different from the search index
  if ((1<<r) < image_count) {
    HashIndex idx;
    idx.set_table_id(coord->get_rank());
    enumerate_entry(search_index, 0, r, idx, count);
    cout<<count<<endl;
  }

  printf("end search in table:%d\n", coord->get_rank());
}

int search_K_nearest_neighbors(int k, std::string query_code){
  ID image_id;
  BinaryCode code;
  int total_find = 0;
  radius = 0;
  
  int start_pos = coord->get_rank() * substr_len;
  std::string local_query_code = query_code.substr(start_pos, substr_len);
  uint32_t search_index = binaryToInt(local_query_code.c_str(), substr_len); 

  while(total_find < k && radius < s_bits){ 
    search_R_neighbors(radius, search_index);

    if(coord->is_master())
      total_find += 1;
    
    coord->bcast(&total_find);
    radius += 1;
  } 
}


void run(){   
  ID image_id;
  BinaryCode code;
   
  printf("Run with config_path=%s image_count=%d binary_bits=%d substring_bits=%d k=%d\ 
      read_mode=%d\n", config_path, image_count, binary_bits, s_bits, k, read_mode);
  
  table_count = binary_bits / s_bits;
  substr_len = s_bits / 8 / sizeof(char);
  
  if(table_count != coord->get_size())
    mpi_coordinator::die("The number of table must equals to the number of mpi processes.");

  srand(getpid());
  int query_image;
    
  if(coord->is_master())
    query_image = rand() % image_count;
  
  coord->bcast(&query_image);
  
  image_id.set_id(query_image);
  
  //if(coord->is_master())
  //  cout<<"query image : "<<query_image<<endl;

  if(clt->get_ext(image_id, code) != POST_GET_FOUND)
    mpi_coordinator::die("Can't find match\n");
  
  std::string query_code = code.code();
  search_K_nearest_neighbors(k, query_code);
}


int main(int argc, char* argv[]){  
  setup(argc, argv); 
  
  vector<int> vec;
  
  for(int i = 0; i < coord->get_rank(); ++i)
    vec.push_back(coord->get_rank());
  
  int *pdata = vec.data();

  //for(int i = 0; i < coord->get_rank() + 1; ++i)
  //  cout<<pdata[i]<<endl;
  
  int data[100];
  int size_array[4] = {1, 2, 3, 4};
  data[0] = 1;
  data[1] = 10;
  data[2] = 11;
    
  vector<int> res = coord->gather_vectors(vec);
  
  if(coord->is_master()){
    cout<<res.size()<<endl;
    
    for(int i = 0; i < res.size(); ++i)
      cout<<res[i]<<" ";

    cout<<endl;
  }
  //run();
  cleanup(); 
  
  return 0;
}

void usage() {
  printf("./search_image <config_path> <image_count> <binary_bits>\ 
      <substring_bits> <search_times> <K>\n");
}

void cleanup(){
  if(bitmap != 0)
    delete bitmap;

  if(clt != 0){
    clt->teardown();
    delete clt;
    clt = 0;
  }
  if(coord != 0){
    delete coord;
    coord = 0;
  }
  
  mpi_coordinator::finalize(); 
}

void setup(int argc, char* argv[]){
  int search_times = 1;

  image_count = 1024;
  binary_bits = 128;
  s_bits = 32;
  radius = 0;
  
  if (argc == 7) {
    config_path = argv[1];
    image_count = atoi(argv[2]);
    binary_bits = atoi(argv[3]);
    s_bits = atoi(argv[4]);
    search_times = atoi(argv[5]);
    k = atoi(argv[6]);
  } else if (argc == 2 && strcmp(argv[1], "--help") == 0) {
    usage();
    exit(0);
  } else if (argc == 3) {
    image_count = atoi(argv[1]);
    read_mode = (read_modes)atoi(argv[2]);
  }

  mpi_coordinator::init(argc, argv);
  coord = new mpi_coordinator;
  clt = new Client;
  if (clt->setup())
    die("Failed to set up client");
  
  ConfigReader config(config_path);
  while(!config.get_end()) {
    struct server_info* this_server = config.get_next();
    if (clt->add_server(this_server->host->c_str(),this_server->port->c_str()))
      die("Failed to add server");
  }
  
  clt->set_read_mode(read_mode);
  clt->ready();
  
  bitmap_count = (image_count + 31) / 32;
  bitmap = new int[bitmap_count];

  if(coord->is_master())
    bitmap_back = new int[bitmap_count];
}

#include <iostream>
#include "mpi_coordinator.h"
#include <bitset>
#include <string.h>
#include "../Pilaf/table_types.h"
#include "../Pilaf/store-server.h"
#include "../Pilaf/store-client.h"
#include "../Pilaf/ibman.h"
#include "../Pilaf/dht.h"
#include "../Pilaf/config.h"
using namespace std;

static mpi_coordinator* coord;
static Client* clt;
static uint32_t image_count;
static int table_count;
static int binary_bits;
static int s_bits;
static int r;
static std::string search_code;
static read_modes read_mode;
static int* bitmap;
static int k;

void cleanup();
void setup(int argc, char* argv[]);


int main(int argc, char* argv[]){  
  
  setup(argc, argv);

  coord->synchronize();
    
  cleanup(); 
  
  return 0;
}

void usage() {
  printf("./search_image <config_path> <image_count> <binary_bits> <substring_bits> <search_times> <K>\n");
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
  char* config_path = "dht-test.cnf";
  int search_times = 1;

  image_count = 1024;
  binary_bits = 128;
  s_bits = 32;
  r = 0;
  
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
  bitmap = new int[(image_count + 31) / 32];
}

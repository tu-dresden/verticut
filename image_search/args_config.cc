#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "args_config.h"
#include "image_search_constants.h"

static struct option long_options[] = {
  {"server",          required_argument,  0,  's'},
  {"config_path",     required_argument,  0,  'c'},
  {"binary_bits",     required_argument,  0,  'b'},
  {"read_mode",       required_argument,  0,  'r'},
  {"ntables",         required_argument,  0,  'n'},
  {"binary_file",     required_argument,  0,  'f'},
  {"help",            no_argument,        0,  'h'},
  {0,                 0,                  0,  0}
};

const char* memcached_config = MEMCACHED_CONFIG;
const char* pilaf_config = PILAF_CONFIG;
const char* config_path = 0;
const char* server = DEFAULT_SERVER;
const char* binary_file = BINARY_CODE_FILE;
int binary_bits = N_BINARY_BITS;
int n_tables = DEFAULT_N_TABLES;
int read_mode  = 0;
int image_total = DEFAULT_IMAGE_TOTAL;
int knn = DEFAULT_KNN;


void usage(){
  printf("Usage : \n");
  printf("--server -s : What kind of key-value server you want to connect.[memcached | pilaf]\n");
  printf("--config_path -c : The path of the file you store server address information.\n");
  printf("--binary_bits -b : How many bits of each binary code.\n");
  printf("--ntables -n : How many sub-tables we use.\n");
  printf("--binary_file -f : The path of the binary file. \n");
  printf("-i : The number of images the server has.\n");
  printf("-k : Find k nearest neighbors.\n");
  printf("-r : The read mode. 0 means RDMA_READ, 1 means verb message read. Only works when use Pilaf proxy.\n");
  printf("--help -h : help information.\n");
  exit(-1);
}

void configure(int argc, char* argv[]){
  int opt_index = 0;
  int opt;
  
  while((opt = getopt_long(argc, argv, "c:b:r:n:s:i:k:f:h", long_options, &opt_index)) != -1){
    switch(opt){
      case 0:
        fprintf(stderr, "get_opt bug?\n");
        break;

      case 'n':
        n_tables = atoi(optarg);
        break;

      case 's':
        server = optarg;
        break;

      case 'b':
        binary_bits = atoi(optarg);
        break;

      case 'c':
        config_path = optarg;
        break;

      case 'r':
        read_mode = atoi(optarg);
        break;
      
      case 'i':
        image_total = atoi(optarg);
        break;
      
      case 'k':
        knn = atoi(optarg);
        break;
      
      case 'f':
        binary_file = optarg;
        break;
      
      case 'h':
        usage();
        break;

      case '?':
        usage();
        break;

      default:
        fprintf(stderr, "Invalid arguments (%c)\n", opt);
        usage();
    }
  }

  if(strcmp(server, "pilaf") != 0 && strcmp(server, "memcached") != 0){
    fprintf(stderr, "Unknown server type.\n");
    usage();
  }

  if(strcmp(server, "pilaf") == 0 && config_path == 0)
    config_path = pilaf_config;
  else if(config_path == 0)
    config_path = memcached_config;
}


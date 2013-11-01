#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "args_config.h"

static struct option long_options[] = {
  {"server",          required_argument,  0,  's'},
  {"config_path",     required_argument,  0,  'c'},
  {"binary_bits",     required_argument,  0,  'b'},
  {"read_mode",       required_argument,  0,  'r'},
  {"ntables",         required_argument,  0,  'n'},
  {0,                 0,                  0,  0}
};

const char* memcached_config = "../config/memcached.cnf";
const char* pilaf_config = "../config/pilaf.cnf";
const char* config_path = 0;
const char* server = "pilaf";
int binary_bits = 128;
int n_tables = 4;
int read_mode  = 0;
int image_total = 1000000;
int knn = 10;

void usage(){
  printf("usage : \n");
  exit(-1);
}

void configure(int argc, char* argv[]){
  int opt_index = 0;
  int opt;
  
  while((opt = getopt_long(argc, argv, "c:b:r:n:s:i:k:", long_options, &opt_index)) != -1){
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


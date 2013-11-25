#include <iostream>
#include "image_search_server.h"
#include <stdint.h>
#include <string>
#include <getopt.h>
#include <stdlib.h>
#include "image_search_constants.h"
#include <signal.h>

static uint16_t port;
static std::string ip = "0.0.0.0";
static std::string config_path = DEFAULT_WORKERS_CONFIG;
static uint16_t n_threads = 10;
static image_search_server *server;

static struct option long_options[] = {
  {"config_path",   required_argument,  0,  'c'},
  {"port",          required_argument,  0,  'p'},
  {"ip",            required_argument,  0,  'i'},
  {"nthreads",      required_argument,  0,  'n'}
};

void usage(){
  printf("Usage : \n"); 
  printf("--config_path -c : The configure file which tells where are the available workers.\n");
  printf("--port -p : The port number the server listens to.\n");
  printf("--ip -i : The ip address the server listens to.\n");
  printf("--n_threads -n : The maximum number of requests the server can process simultaneously.\n");
  exit(-1);
}

void parse_args(int argc, char *argv[]){
  int opt_index = 0;
  int opt;

  while((opt = getopt_long(argc, argv, "c:p:i:", long_options, &opt_index)) != -1){
    switch(opt){
      case 0:
        fprintf(stderr, "get_opt but?\n");
        break;

      case 'c':
        config_path = optarg;
        break;

      case 'p':
        port = atoi(optarg);
        break;
        
      case 'i':
        ip = optarg;
        break;
      
      case 'n':
        n_threads = atoi(optarg);
        break;

      case '?':
        usage();
        break;

      default:
          fprintf(stderr, "Invald arguments (%c)\n", opt);
          usage();
    }
  }
}

void sig_handler(int sig){
  if(sig == SIGINT){
    if(server){
      server->instance.close();
      delete server;
    }
    exit(1);
  }
}


int main(int argc, char* argv[]){
  
  signal(SIGINT, sig_handler);
  parse_args(argc, argv);
  
  server = new image_search_server;
  server->init_workers(config_path);
  server->instance.listen(ip, port);
  
  std::cout<<"Server is running with "<<n_threads<<" threads..."<<std::endl;
  server->instance.run(n_threads);

  return 0;
}

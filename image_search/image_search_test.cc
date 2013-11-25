#include "image_search_client.h"
#include <iostream>
#include <list>
#include <getopt.h>
#include <string>
#include <stdint.h>
#include "image_search_constants.h"
#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>

static int TEST_NUM = 20;
static std::string ip = "127.0.0.1";
static uint16_t port = DEFAULT_SERVER_PORT;
static bool throughput_test = false;
static bool con_throughput_test = false;
static bool approximate = false;
static image_search_client *clt;
static uint32_t knn = DEFAULT_KNN;

static struct option long_options[] = {
  {"throughput",    required_argument,  0,  't'},
  {"c_throughput",  required_argument,  0,  'c'},
  {"ip",            required_argument,  0,  'i'},
  {"port",          required_argument,  0,  'p'},
  {"approximate",   required_argument,  0,  'a'},
  {"knn",           required_argument,  0,  'k'},
  {"n_test",        required_argument,  0,  'n'}
};

void usage(){
  printf("Usage :\n");
  exit(-1);
}

void parse_args(int argc, char *argv[]){
  int opt_index = 0;
  int opt;

  while((opt = getopt_long(argc, argv, "tci:p:ak:n:", long_options, &opt_index)) != -1){
    switch(opt){
      case 0:
        fprintf(stderr, "get_opt but?\n");
        break;

      case 'c':
        con_throughput_test = true;
        break;

      case 'p':
        port = atoi(optarg);
        break;
       
      case 'n':
        TEST_NUM = atoi(optarg);
        break;

      case 'i':
        ip = optarg;
        break;
      
      case 't':
        throughput_test = true;
        break;
      
      case 'a':
        approximate = true;
        break;
      
      case 'k':
        knn = atoi(optarg);
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

void* query_thread(void* param){
  image_search_client c(ip, port);
  uint32_t id = rand() % 100000000;
  std::list<std::pair<uint32_t, uint32_t> > res = c.search_image_by_id(id, knn, approximate);

  return NULL;
}


int main(int argc, char *argv[]){

  parse_args(argc, argv);
  
  clt = new image_search_client(ip, port);
  struct timeval start_time, end_time;

  srand(getpid());

  if(throughput_test){
    //test sequential thoroughput 
    std::cout<<"Testing sequential throughput..."<<std::endl;
    gettimeofday(&start_time, NULL);

    for(int i = 0; i < TEST_NUM; ++i){
      uint32_t id = rand() % 100000000;
      std::list<std::pair<uint32_t, uint32_t> > res = clt->search_image_by_id(id, knn, approximate);
      std::cerr<<'\r';
      std::cerr<<i<<"/"<<TEST_NUM;
    }
    std::cerr<<std::endl;
    
    gettimeofday(&end_time, NULL);
    double secs = (double)(end_time.tv_sec - start_time.tv_sec);
    std::cout<<secs<<" secs passed."<<std::endl;
    std::cout<<"Throughput : "<<(TEST_NUM / secs)<<std::endl;
  }

  if(con_throughput_test){
    //test concurrent thoroughput 
    std::cout<<"Testing concurrent throughput..."<<std::endl; 
    gettimeofday(&start_time, NULL);
    
    pthread_t *tids = (pthread_t*)malloc(sizeof(pthread_t) * TEST_NUM);

    for(int i = 0; i < TEST_NUM; ++i)
      pthread_create(&tids[i], 0, query_thread, 0);

    for(int i = 0; i < TEST_NUM; ++i)
      pthread_join(tids[i], NULL);

    gettimeofday(&end_time, NULL);
    double secs = (double)(end_time.tv_sec - start_time.tv_sec);
    std::cout<<secs<<" secs passed."<<std::endl;
    std::cout<<"Throughput : "<<(TEST_NUM / secs)<<std::endl;
    free((void*)tids);
  }
  
  delete clt;
  return 0;
}

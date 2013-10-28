//Author : Yisheng
#include <iostream>
#include "../Pilaf/store-client.h"
#include <pthread.h>
#include <unistd.h>

using namespace std;

Client clt;

void setup(const char *ip, const char* port){
  clt.setup();
  clt.add_server(ip, port);
  clt.set_read_mode(READ_MODE_RDMA);
  clt.ready();
}

void test_get(){
  const int TEST_LEN = 100;
  char test_buf[TEST_LEN][32];
  for(int i = 0; i < TEST_LEN; ++i){
    sprintf(test_buf[i], "%d", i);
    clt.put(test_buf[i], test_buf[i]);
  }
  
  char buffer[32];
  char* res = buffer;
  size_t size;

  for(int i = 0; i <  TEST_LEN; ++i){
    clt.get_with_size(test_buf[i], res, size);
    res[size] = '\0';
    assert(strcmp(res, test_buf[i]) == 0);
  }

  cout<<"get test pass..."<<endl;
}


void* thread_get(void* len){
  long TEST_LEN = (long)len;
    
  char buffer[32];
  char* res = buffer;
  size_t size;
  char sz[32];

  for(int i = 0; i <  TEST_LEN; ++i){
    sprintf(sz, "%d", i);
    clt.get_with_size(sz, res, size);
    res[size] = '\0';
    assert(strcmp(res, sz) == 0);
  }

  return 0;
}

void test_get_multithreads(){
  const int TEST_LEN = 100;
  char test_buf[TEST_LEN][32];
  for(int i = 0; i < TEST_LEN; ++i){
    sprintf(test_buf[i], "%d", i);
    clt.put(test_buf[i], test_buf[i]);
  }
  
  const int THREAD_NUM = 4;
  pthread_t tids[THREAD_NUM];

  for(int t = 0; t < THREAD_NUM; ++t)
    pthread_create(&tids[t], 0, thread_get, (void*)TEST_LEN);
  
  for(int t = 0; t < THREAD_NUM; ++t)
    pthread_join(tids[t], 0);
  
  cout<<"get multithreads pass..."<<endl;
}


int main(int argc, char* argv[]){
  
  if(argc == 3)
    setup(argv[1], argv[2]);
  else
    setup("192.168.5.14", "36001");

  test_get();
  //test_get_multithreads(); 

  cout<<"test done."<<endl;

  return 0;
}

//Author : Yisheng
#include<iostream>
#include "mpi.h"
#include "../Pilaf/store-client.h"
using namespace std;

const int MASTER = 0;
const int TEST_LEN = 100;
int task_id;
int n_tasks;
Client clt;

void setup(const char *ip, const char* port){
  clt.setup();
  clt.add_server(ip, port);
  clt.set_read_mode(READ_MODE_RDMA);
  clt.ready();
}

void build_table(){
  char test_buf[TEST_LEN][32];
  for(int i = 0; i < TEST_LEN; ++i){
    sprintf(test_buf[i], "%d", i);
    clt.put(test_buf[i], test_buf[i]);
  }
}

void test_get(){
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
}


int main(int argc, char* argv[]){
  MPI_Init(&argc, &argv);  
  MPI_Comm_size(MPI_COMM_WORLD, &n_tasks);
  MPI_Comm_rank(MPI_COMM_WORLD, &task_id);
   
  if(argc == 3)
    setup(argv[1], argv[2]);
  else
    setup("192.168.5.14", "36001");

  if(task_id == MASTER)
    build_table();
  
  MPI_Barrier(MPI_COMM_WORLD);
  test_get();
  
  cout<<"test done."<<endl;

  MPI_Finalize();
  
  return 0;
}

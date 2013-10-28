#include "mpi_coordinator.h"
#include <iostream>

mpi_coordinator::mpi_coordinator(MPI_Comm comm):MASTER(0){
  m_comm = comm;
  MPI_Comm_size(m_comm, &m_size);
  MPI_Comm_rank(m_comm, &m_rank);
}

void mpi_coordinator::finalize(){
  MPI_Finalize();
}

void mpi_coordinator::init(int argc, char *argv[]){
  MPI_Init(&argc, &argv);  
}

void mpi_coordinator::bitwise_or(int *send_bmp, int* recv_bmp, int count){ 
  MPI_Reduce(send_bmp, recv_bmp, count, MPI_INT, MPI_BOR, MASTER, MPI_COMM_WORLD); 
}

void mpi_coordinator::die(const std::string& str){
  std::cerr<<str<<std::endl;
  MPI_Abort(m_comm, -1);
}


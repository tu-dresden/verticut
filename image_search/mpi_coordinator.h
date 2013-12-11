// A wrapper on MPI to make mpi programming easier.
// Author : Yisheng Liao & Chenqi

#ifndef MPI_COORDINATOR_H
#define MPI_COORDINATOR_H
#include "mpi.h"
#include <stdint.h>
#include <string>
#include <vector>

const int MASTER = 0;

class mpi_coordinator{
  public: 
    mpi_coordinator(MPI_Comm comm = MPI_COMM_WORLD);

    int get_size() { return size_; }
    int get_rank() { return rank_; }
    bool is_master() { return rank_ == MASTER; }

    void synchronize(){ MPI_Barrier(comm_); }
    void bitwise_or(int *send_bmp, int *recv_bmp, int count);   
    
    //Broadcast one integer to all processes.
    void bcast(int* buf, int count = 1, int root = 0);
    void gather(int *send_buf, int *recv_buf, int count);

    //Gather the vetors from all processes. ONLY return the 
    //gathered result vector to MASTER process.
    std::vector<uint64_t> gather_vectors(std::vector<uint64_t> &data); 

    static void die(const std::string& str);
    static void finalize();
    static void init(int argc, char* argv[]);
    
  protected:
    MPI_Comm comm_;
    int rank_;
    int size_;

  private:
    //Disable copy constructor.
    mpi_coordinator(const mpi_coordinator &c);
    
};

#endif

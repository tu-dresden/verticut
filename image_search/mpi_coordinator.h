// A wrapper on MPI to make mpi programming easier.
// Author : Yisheng Liao & Chenqi

#ifndef MPI_COORDINATOR_H
#define MPI_COORDITATOR_H
#include "mpi.h"
#include <stdint.h>
#include <string>
#include <vector>

const int MASTER = 0;

class mpi_coordinator{
  public: 
    mpi_coordinator(MPI_Comm comm = MPI_COMM_WORLD);

    int get_size() { return m_size; }
    int get_rank() { return m_rank; }
    bool is_master() { return m_rank == MASTER; }

    void synchronize(){ MPI_Barrier(m_comm); }
    void bitwise_or(int *send_bmp, int *recv_bmp, int count);   
    
    //Broadcast one integer to all processes.
    void bcast(int* buf, int count = 1, int root = 0);
    void gather(int *send_buf, int *recv_buf, int count);

    //Gather the vetors from all processes. ONLY return the 
    //gathered result vector to MASTER process.
    std::vector<int> gather_vectors(std::vector<int> &data); 

    static void die(const std::string& str);
    static void finalize();
    static void init(int argc, char* argv[]);
    
  protected:
    MPI_Comm m_comm;
    int m_rank;
    int m_size;

  private:
    //Disable copy constructor.
    mpi_coordinator(const mpi_coordinator &c);
    
};

#endif

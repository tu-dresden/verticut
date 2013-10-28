#include "mpi.h"
#include <stdint.h>
#include <string>

class mpi_coordinator{
  public: 
    mpi_coordinator(MPI_Comm comm = MPI_COMM_WORLD);

    int get_size() { return m_size; }
    int get_rank() { return m_rank; }
    bool is_master() { return m_rank == MASTER; }

    void synchronize(){ MPI_Barrier(m_comm); }
    void bitwise_or(int *send_bmp, int *recv_bmp, int count); 
    
    static void die(const std::string& str);
    static void finalize();
    static void init(int argc, char* argv[]);
    

  protected:
    MPI_Comm m_comm;
    int m_rank;
    int m_size;
    const int MASTER;

  private:
    //Disable copy constructor.
    mpi_coordinator(const mpi_coordinator &c);
    
};




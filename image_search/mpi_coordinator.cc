#include "mpi_coordinator.h"

mpi_coordinator::mpi_coordinator(MPI_Comm comm){
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
  MPI_Abort(MPI_COMM_WORLD, -1);
}

void mpi_coordinator::bcast(int *buf, int count, int root){
  MPI_Bcast(buf, count, MPI_INT, root, m_comm);
}

void mpi_coordinator::gather(int *send_buf, int *recv_buf, int count){
  MPI_Gather(send_buf, count, MPI_INT, recv_buf, count, MPI_INT, MASTER, m_comm);
}
    
std::vector<int> mpi_coordinator::gather_vectors(std::vector<int> &data){
  int count = data.size();
  int *count_array = 0;
  int *disp_array = 0;
  int *result_array = 0;
  int sum = 0;

  if(is_master()){
    count_array = new int[m_size];
    disp_array = new int[m_size];
  }
  //Gather vector size for each processes.
  MPI_Gather(&count, 1, MPI_INT, count_array, 1, MPI_INT, MASTER, m_comm);
  
  if(is_master()){
    //Build displacement array.
    for (int i = 0; i < m_size; i++){
      disp_array[i] = (i > 0) ? (disp_array[i-1] + count_array[i-1]) : 0;
      sum += count_array[i];
    } 
    result_array = new int[sum];
  }
  
  //Gather vector data.
  MPI_Gatherv(data.data(), count, MPI_INT, result_array, count_array, disp_array, MPI_INT,
      MASTER, m_comm);
  
  if(is_master()){
    std::vector<int> ret(result_array, result_array + sum);
    delete result_array;
    delete disp_array;
    delete count_array;
    return ret;
  } 
  return std::vector<int>();
}


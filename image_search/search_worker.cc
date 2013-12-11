#include "search_worker.h"
#include "image_tools.h"
#include <iostream>
#include "timer.h"
#include <stdlib.h>
#include "image_search_constants.h"
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#define GET_ID(v) (v & 0xffffffff)
#define GET_DIST(v) (v >> 32)

bool operator<(const SearchWorker::search_result_st &a,const SearchWorker::search_result_st &b)
{
  return a.dist<b.dist;
}

void SearchWorker::get_stat(uint64_t &n_main_reads, uint64_t &n_sub_reads, 
                                uint64_t &n_local_reads, uint32_t &radius){
  n_main_reads = n_main_reads_;
  n_sub_reads = n_sub_reads_;
  n_local_reads = n_local_reads_;
  radius = radius_;
}

bool SearchWorker::connect_bitmap_deamon(unsigned long long size){
  int shared_fd_ = shm_open(MEM_ID, O_RDWR, 0666);
  if(shared_fd_ == -1)
    return false;

  unsigned long long size_per_table = size / coord_->get_size();
  void* addr = mmap(0, size, PROT_READ, MAP_SHARED, shared_fd_, 0);
  
  close(shared_fd_);

  if(addr == MAP_FAILED)
    return false;

  bmp_ = new ImageBitmap(size_per_table, (char*)addr + coord_->get_rank() * size_per_table);
  
  return true;
}

SearchWorker::SearchWorker(mpi_coordinator *coord, 
                          BaseProxy<protobuf::Message, protobuf::Message> *proxy_clt,
                          int image_total
                          ){
  coord_  = coord;
  proxy_clt_ = proxy_clt;
  knn_ = 0;
  image_total_ = image_total;
  table_idx_ = coord->get_rank();
  bmp_ = 0;
  
  connect_bitmap_deamon();
  //printf("init : %d\n", connect_bitmap_deamon());
}

std::list<SearchWorker::search_result_st> SearchWorker::find(const char *binary_code, 
    size_t nbytes, int knn, bool approximate){
  
  knn_ = knn;
  knn_found_.clear();
  result_.clear();
  n_main_reads_ = 0;
  n_sub_reads_ = 0;
  n_local_reads_ = 0;

  assert(nbytes % coord_->get_size() == 0);
  n_local_bytes_ = nbytes / coord_->get_size();

  BinaryCode code;
  ID image_id;
  
  code.set_code(binary_code, nbytes);
  
  if(approximate) //find approximate neighbors
    radius_ = search_K_approximate_nearest_neighbors(code);
  else
    radius_ = search_K_nearest_neighbors(code);

  return result_;
}

#define MAIN_GET_P
//Find approximate KNN, this is supposed to be much faster than exact KNN when k is large.
size_t SearchWorker::search_K_approximate_nearest_neighbors(BinaryCode& code){
  std::priority_queue<search_result_st> qmax;
  size_t radius = 0; //Current searching radius.
  std::vector<uint64_t> kn_candidates; //KNN candidates for current searching radius.
  std::string query_code = code.code();

  int start_pos = coord_->get_rank() * n_local_bytes_; 
  std::string local_query_code = query_code.substr(start_pos, n_local_bytes_);
  uint32_t search_index = binaryToInt(local_query_code.c_str(), n_local_bytes_);  
  int is_stop = 0;

  while(!is_stop && radius <= n_local_bytes_ * 8){ 
    //Clear kn_candidates
    kn_candidates.clear();
    kn_candidates.reserve(8192 * 500);
    
    search_R_neighbors(radius, search_index, kn_candidates);
  
#ifdef MAIN_GET_P
    ID image_id;
    BinaryCode code;

    for(int i = 0; i < kn_candidates.size(); ++i){
      uint32_t id = kn_candidates[i];


      image_id.set_id(id);
      
      if(proxy_clt_->get(image_id, code) != PROXY_FOUND)
        mpi_coordinator::die("No corresponding image found.\n");

      uint64_t dist = compute_hamming_dist(code.code(), query_code);
      kn_candidates[i] |= (dist << 32);
    }
#endif
    
    vector<uint64_t> gathered_vector = coord_->gather_vectors(kn_candidates);

    if(coord_->is_master()){
#ifdef MAIN_GET_P
      for(int i = 0; i < gathered_vector.size(); ++i){
        uint32_t id = GET_ID(gathered_vector[i]);
        int dist = GET_DIST(gathered_vector[i]);
        
        if(knn_found_.find(id) != knn_found_.end())
          continue;
        
        search_result_st item;
        item.image_id = id;
        item.dist = dist;
#else
      //sort(gathered_vector.begin(), gathered_vector.end()); 
      //Eliminate duplicates.
      //vector<uint64_t>::iterator iter = unique(gathered_vector.begin(), gathered_vector.end());
      //gathered_vector.resize(distance(gathered_vector.begin(), iter));
      
      BinaryCode code;
      ID image_id;
      
      for(uint32_t i = 0; i < gathered_vector.size(); ++i){
        int id = gathered_vector[i];
      
        //we've already added this to neighbor candidates lists
        if(knn_found_.find(id) != knn_found_.end())
          continue;
        
        image_id.set_id(id);
        n_main_reads_++;
        
        if(proxy_clt_->get(image_id, code) != PROXY_FOUND)
          mpi_coordinator::die("No corresponding image found.\n");

        search_result_st item;
        item.image_id = id;
        item.dist = compute_hamming_dist(code.code(), query_code);
#endif    
      
        knn_found_[id] = 1;
      
        if (qmax.size() < knn_ * APPROXIMATE_FACTOR) {
          qmax.push(item);
        }else if (qmax.top().dist > item.dist) {
          qmax.pop();
          qmax.push(item);
        }  
        //if(qmax.size() == knn_ * APPROXIMATE_FACTOR)
        //  break;
      }
    }

    
    radius += 1; 
    //If the mininum distance next epoch we may find is less than the max one of 
    //what we've found, then stop.
    if(coord_->is_master() && qmax.size() == knn_ * APPROXIMATE_FACTOR)
      is_stop = 1;

    coord_->bcast(&is_stop);
  }
  
  int i = 0;
  int beg = qmax.size() - knn_;

  if(coord_->is_master())
    while (!qmax.empty()) {
      search_result_st item = qmax.top();
      
      if(i >= beg)
        result_.push_back(item);
      
      qmax.pop();
      i++;
    }

  return radius - 1;
}

//Find exact KNN
size_t SearchWorker::search_K_nearest_neighbors(BinaryCode& code){
  std::priority_queue<search_result_st> qmax;
  size_t radius = 0; //Current searching radius.
  std::vector<uint64_t> kn_candidates; //KNN candidates for current searching radius.
  std::string query_code = code.code();

  int start_pos = coord_->get_rank() * n_local_bytes_; 
  std::string local_query_code = query_code.substr(start_pos, n_local_bytes_);
  uint32_t search_index = binaryToInt(local_query_code.c_str(), n_local_bytes_);  
  int is_stop = 0;

  while(!is_stop && radius <= n_local_bytes_ * 8){ 
    //Clear kn_candidates
    kn_candidates.clear();
    kn_candidates.reserve(8192 * 500);
    search_R_neighbors(radius, search_index, kn_candidates);
    vector<uint64_t> gathered_vector = coord_->gather_vectors(kn_candidates);
     
    if(coord_->is_master()){

      sort(gathered_vector.begin(), gathered_vector.end());
      
      //Eliminate duplicates.
      vector<uint64_t>::iterator iter = unique(gathered_vector.begin(), gathered_vector.end());
      gathered_vector.resize(distance(gathered_vector.begin(), iter));
      
      BinaryCode code;
      ID image_id;
      for(uint32_t i = 0; i < gathered_vector.size(); ++i){
        int id = gathered_vector[i];
      
        //we've already added this to neighbor candidates lists
        if(knn_found_.find(id) != knn_found_.end())
          continue;
        
        n_main_reads_++;
        image_id.set_id(id);
        
        if(proxy_clt_->get(image_id, code) != PROXY_FOUND)
          mpi_coordinator::die("No corresponding image found.\n");
        
        search_result_st item;
        item.image_id = id;
        item.dist = compute_hamming_dist(code.code(), query_code);
        knn_found_[id] = 1;
      
        if (qmax.size() < knn_) {
          qmax.push(item);
        }else if (qmax.top().dist > item.dist) {
          qmax.pop();
          qmax.push(item);
        }
      }
    }
    
    radius += 1; 
    //If the mininum distance next epoch we may find is less than the max one of 
    //what we've found, then stop.
    if(coord_->is_master() && qmax.size() == knn_ && qmax.top().dist < radius * 4)
      is_stop = 1;

    coord_->bcast(&is_stop);
  }
  
  if(coord_->is_master())
    while (!qmax.empty()) {
      search_result_st item = qmax.top();
      result_.push_back(item);
      qmax.pop();
    }

  return radius - 1;
}

void SearchWorker::search_R_neighbors(int r, uint32_t search_index, 
    std::vector<uint64_t>& kn_candidates){ 
  HashIndex idx;
  idx.set_table_id(coord_->get_rank());
  enumerate_entry(search_index, 0, r, idx, kn_candidates);
}

//Enumerate all the entries.
void SearchWorker::enumerate_entry(uint32_t curr, int len, int rr, HashIndex& idx, 
    std::vector<uint64_t> &kn_candidates){ 
  
  if (rr == 0) {
    ImageList img_list;
    idx.set_index(curr);
    int rval;
  
    if(bmp_){
      n_local_reads_++;
      if(bmp_->get_idx(curr) == 0){
        return;
      }
    }
  
    n_sub_reads_++;
    rval = proxy_clt_->get(idx, img_list);
    
    if (rval == PROXY_FOUND) {
      for(int i = 0; i < img_list.images_size(); i++){
        if(img_list.images(i) < image_total_){
          kn_candidates.push_back(img_list.images(i));
        }
      }
    }
  }else{
    enumerate_entry(curr^(1<<len), len+1, rr-1, idx, kn_candidates);
    if(n_local_bytes_ * 8 - len > rr)
      enumerate_entry(curr, len+1, rr, idx, kn_candidates);
  }
}

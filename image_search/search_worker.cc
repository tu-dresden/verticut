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

static void* addr1;
static void* addr2;
static void* addr3;
static void* addr4;
static int sd;
static unsigned long long total_size =  ((unsigned long long)1 << 32) / 8 * 4;

static int shared_mem_init(){
  sd = shm_open(MEM_ID, O_RDWR, 0666);
  if(sd == -1)
    return 0;
  
  unsigned long long size = total_size / 4;
  addr1 = mmap(0, total_size, PROT_READ, MAP_SHARED, sd, 0);
  
  if(addr1 == MAP_FAILED){
    addr1 = 0;
    return 0;
  }
  addr2 = (char*)addr1 + size;
  addr3 = (char*)addr1 + 2 * size;
  addr4 = (char*)addr1 + 3 * size;
  
  return 1;
}

static void shared_mem_release(){

}

int get_idx(uint32_t *ptr, uint32_t bit_idx){
  uint32_t word_idx = bit_idx / 32;
  bit_idx = bit_idx % 32;
  return ptr[word_idx] & (1 << bit_idx);
}

bool operator<(const SearchWorker::search_result_st &a,const SearchWorker::search_result_st &b)
{
  return a.dist<b.dist;
}

SearchWorker::SearchWorker(mpi_coordinator *coord, 
                          BaseProxy<protobuf::Message, protobuf::Message> *proxy_clt,
                          int knn,
                          int image_total
                          ){
  printf("init : %d\n", shared_mem_init());
  coord_  = coord;
  proxy_clt_ = proxy_clt;
  knn_ = knn;
  image_total_ = image_total;
  table_idx_ = coord->get_rank();
  
  if(table_idx_ == 0)
    addr_ = addr1;
  else if(table_idx_ == 1)
    addr_ = addr2;
  else if(table_idx_ == 2)
    addr_ = addr3;
  else if(table_idx_ == 3)
    addr_ = addr4;
}

std::list<SearchWorker::search_result_st> SearchWorker::find(const char *binary_code, 
    size_t nbytes, bool approximate, size_t &r){
  knn_found_.clear();
  result_.clear();
  
  assert(nbytes % coord_->get_size() == 0);
  n_local_bytes_ = nbytes / coord_->get_size();

  BinaryCode code;
  ID image_id;
  
  code.set_code(binary_code, nbytes);
  
  if(approximate) //find approximate neighbors
    r = search_K_approximate_nearest_neighbors(code);
  else
    r = search_K_nearest_neighbors(code);

  return result_;
}

//Find approximate KNN, this is supposed to be much faster than exact KNN when k is large.
size_t SearchWorker::search_K_approximate_nearest_neighbors(BinaryCode& code){
  std::priority_queue<search_result_st> qmax;
  size_t radius = 0; //Current searching radius.
  std::vector<int> kn_candidates; //KNN candidates for current searching radius.
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
    vector<int> gathered_vector = coord_->gather_vectors(kn_candidates);
     
    if(coord_->is_master()){

      sort(gathered_vector.begin(), gathered_vector.end()); 
      //Eliminate duplicates.
      vector<int>::iterator iter = unique(gathered_vector.begin(), gathered_vector.end());
      gathered_vector.resize(distance(gathered_vector.begin(), iter));
      
      BinaryCode code;
      ID image_id;
      
      for(uint32_t i = 0; i < gathered_vector.size(); ++i){
        int id = gathered_vector[i];
      
        //we've already added this to neighbor candidates lists
        if(knn_found_.find(id) != knn_found_.end())
          continue;
        
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
    if(coord_->is_master() && qmax.size() == knn_)
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

//Find exact KNN
size_t SearchWorker::search_K_nearest_neighbors(BinaryCode& code){
  std::priority_queue<search_result_st> qmax;
  size_t radius = 0; //Current searching radius.
  std::vector<int> kn_candidates; //KNN candidates for current searching radius.
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
    vector<int> gathered_vector = coord_->gather_vectors(kn_candidates);
     
    if(coord_->is_master()){

      sort(gathered_vector.begin(), gathered_vector.end());
      
      //Eliminate duplicates.
      vector<int>::iterator iter = unique(gathered_vector.begin(), gathered_vector.end());
      gathered_vector.resize(distance(gathered_vector.begin(), iter));
      
      BinaryCode code;
      ID image_id;
      for(uint32_t i = 0; i < gathered_vector.size(); ++i){
        int id = gathered_vector[i];
      
        //we've already added this to neighbor candidates lists
        if(knn_found_.find(id) != knn_found_.end())
          continue;
        
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
    //if(coord_->is_master() && qmax.size() == knn_)
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
    std::vector<int>& kn_candidates){ 
  HashIndex idx;
  idx.set_table_id(coord_->get_rank());
  enumerate_entry(search_index, 0, r, idx, kn_candidates);
}

//int hit = 0;
//int miss = 0;

//Enumerate all the entries.
void SearchWorker::enumerate_entry(uint32_t curr, int len, int rr, HashIndex& idx, 
    std::vector<int> &kn_candidates){ 
  
  //if(miss % 100000 == 0)
  //  printf("miss / hit : %d / %d\n", miss, hit);

  if (rr == 0) {
    ImageList img_list;
    idx.set_index(curr);
    int rval;

    if(addr_){
      //printf("here\n");
      if(get_idx((uint32_t*)addr_, curr) == 0){
        return;
      }
    }
  
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

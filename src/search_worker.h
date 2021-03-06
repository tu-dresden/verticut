// Copyright (C) 2013, Peking University & New York University
// Author: Qi Chen (chenqi871025@gmail.com) & Yisheng Liao(eason.liao@nyu.edu)

#ifndef SEARCH_WORKER_H
#define SEARCH_WORKER_H
#include "memcached_proxy.h"
#include "pilaf_proxy.h"
#include "mpi_coordinator.h"
#include "image_search.pb.h"
#include <list>
#include <stdint.h>
#include "bitmap.h"
//#include <unordered_map>
#define APPROXIMATE_FACTOR 20

using namespace google;

class SearchWorker{
  public: 
    struct search_result_st{
      uint32_t image_id;
      uint32_t dist;
    };
    
    SearchWorker(mpi_coordinator *coord, 
                BaseProxy<protobuf::Message, protobuf::Message> *proxy_clt,
                int image_total);

    std::list<search_result_st> find(const char *binary_code, size_t nbytes, 
                                      int knn, bool approximate);

    std::list<search_result_st> get_knn() { return result_; };
    void get_stat(uint64_t &n_main_reads, uint64_t &n_sub_reads, uint64_t &n_local_reads, uint32_t &radius);

  protected:
    mpi_coordinator* coord_;
    BaseProxy<protobuf::Message, protobuf::Message> *proxy_clt_;
    std::list<search_result_st> result_;
    //std::unordered_map<int, bool> knn_found_;
    std::map<int, bool> knn_found_;
    ImageBitmap *bmp_;
    uint64_t n_main_reads_;
    uint64_t n_sub_reads_;
    uint64_t n_local_reads_;
    uint32_t radius_;

    int knn_;
    int image_total_;
    int n_local_bytes_;
    int table_idx_;

    //Find exact KNN
    size_t search_K_nearest_neighbors(BinaryCode &code);
    
    //Find approximate KNN, this is supposed to be much faster than exact KNN when k is large.
    size_t search_K_approximate_nearest_neighbors(BinaryCode &code);
    void search_R_neighbors(std::string &query_code, int r, uint32_t search_index, 
        std::vector<uint64_t> &knn_candidates);
    void enumerate_entry(std::string &query_code, uint32_t curr, int len, int rr, HashIndex &idx, 
        std::vector<uint64_t> &knn_candidates);
    
    //try to map the memory space of bitmap deamon to local memory.
    bool connect_bitmap_deamon(unsigned long long size = ((unsigned long long )1 << 32) / 8 * 4);
};
#endif

#include "bitmap.h"

ImageBitmap::ImageBitmap(unsigned long n_bytes){
  assert(n_bytes > 0);
  data_ = (uint32_t*)calloc(n_bytes, 1);
  n_bytes_ = n_bytes; 
}

ImageBitmap::ImageBitmap(unsigned long n_bytes, void* ptr){
  assert(ptr != 0);
  data_ = (uint32_t*)ptr;
  n_bytes_ = n_bytes;
}

ImageBitmap::~ImageBitmap(){
  if(data_){
    free(data_);
    data_ = 0;
  }
}

bool ImageBitmap::get_idx(unsigned long bit_idx){
  unsigned long word_idx = bit_idx / 32;
  bit_idx = bit_idx % 32;
  return data_[word_idx] & (1 << bit_idx);
}

void ImageBitmap::set_idx(unsigned long bit_idx){
  unsigned long word_idx = bit_idx / 32;
  bit_idx = bit_idx % 32;
  data_[word_idx] |= (1 << bit_idx);
}

void ImageBitmap::reset_idx(unsigned long bit_idx){
  unsigned long word_idx = bit_idx / 32;
  bit_idx = bit_idx % 32;
  data_[word_idx] &= ~(1 << bit_idx);
}

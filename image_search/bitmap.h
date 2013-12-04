#ifndef IMG_BITMAP_H
#define IMG_BITMAP_H
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>

class ImageBitmap{
  protected:
    uint32_t* data_;
    unsigned long n_bytes_;

  public:
    ImageBitmap(unsigned long n_bytes);
    ImageBitmap(unsigned long n_bytes, void* ptr);
    ~ImageBitmap();
    
    void* data() { return (void*)data_; }
    bool get_idx(unsigned long bit_idx);
    void set_idx(unsigned long bit_idx);
    void reset_idx(unsigned long bit_idx);
    unsigned long len() { return n_bytes_; }
};



#endif

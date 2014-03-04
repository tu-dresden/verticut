#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <sys/signal.h>
#include <unistd.h>
#include "image_search_constants.h"
#include <iostream>

static unsigned long long total_size =  ((unsigned long long)1 << 32) / 8 * 4;
static void *addr;

void f_exit(int s){
  munmap(addr, total_size);
  shm_unlink(MEM_ID);
  exit(-1);
}

int main(int argc, char* argv[]){
  
  signal(SIGINT, f_exit);
  signal(SIGTSTP, f_exit);
  
  int sd = shm_open(MEM_ID, O_CREAT | O_TRUNC | O_RDWR, 0666);
  
  if(sd == -1){
    fprintf(stderr, "Can't create share memory oject\n");
    exit(-1);
  }
  
  assert(ftruncate(sd, total_size) != -1);
  addr = mmap(0, total_size, PROT_READ | PROT_WRITE, MAP_SHARED, sd, 0);
  close(sd);

  assert(addr != MAP_FAILED);
  
  FILE* f1 = fopen("/scratch2/cq/2b_images_raw.data_bmp_1_2b.raw", "r");
  FILE* f2 = fopen("/scratch2/cq/2b_images_raw.data_bmp_2_2b.raw", "r");
  FILE* f3 = fopen("/scratch2/cq/2b_images_raw.data_bmp_3_2b.raw", "r");
  FILE* f4 = fopen("/scratch2/cq/2b_images_raw.data_bmp_4_2b.raw", "r");
  
  /*FILE* f1 = fopen("/scratch2/cq/2b_images_raw.data_bmp_1_2b.raw", "r");
  FILE* f2 = fopen("/scratch2/cq/2b_images_raw.data_bmp_2_2b.raw", "r");
  FILE* f3 = fopen("/scratch2/cq/2b_images_raw.data_bmp_3_2b.raw", "r");
  FILE* f4 = fopen("/scratch2/cq/2b_images_raw.data_bmp_4_2b.raw", "r");
  */

  if(f1 == 0 || f2 == 0 || f3 == 0 || f4 == 0){
    fprintf(stderr, "can't open fiel file\n");
    exit(-1);
  }
  
  unsigned long long size = total_size / 4;
  fread(addr, size, 1, f1);
  fread((char*)addr + size, size, 1, f2);
  fread((char*)addr + size * 2, size, 1, f3);
  fread((char*)addr + size * 3, size, 1, f4);
  fclose(f1);
  fclose(f2);
  fclose(f3);
  fclose(f4);
  
  std::cout<<"ok"<<std::endl;

  while(1){
    sleep(1);
  }

  munmap(addr, total_size);
  shm_unlink(MEM_ID);
  
  return 0;
}

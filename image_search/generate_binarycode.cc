// Copyright (C) 2013, Peking University
// Author: Qi Chen (chenqi871025@gmail.com)
//
// Description:

#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include "../Pilaf/image_tools.h"

void usage() {
  printf("./generate_binarycode <binarycode_path> <image_count> <binary_bits>\n");
}

int main (int argc, char *argv[]) {

  char* binarycode_path = "lsh.code";
  int image_count = 10;
  int binary_bits = 128;

  if (argc == 4) {
    binarycode_path = argv[1];
    image_count = atoi(argv[2]);
    binary_bits = atoi(argv[3]);
  } else if (argc == 2 && strcmp(argv[1], "--help") == 0) {
    usage();
    exit(0);
  }

  printf("Run with binary_path=%s image_count=%d binary_bits=%d\n", binarycode_path, image_count, binary_bits);

  FILE* fh;
  if (NULL == (fh = fopen(binarycode_path, "w"))) {
    return 0;
  }

  int len = binary_bits / 32;
  uint32_t * p = new uint32_t [len];

  srand(time(NULL));
  char p_end = '\0';
  for (int i = 0; i < image_count; i++) {
    for (int j = 0; j < len; j++)
      p[j] = rand();
    printf("%d %s\n", i, binaryToString((char *)p, sizeof(uint32_t)*len/sizeof(char)).c_str());
    fwrite((const void*)p, sizeof(uint32_t)*len, 1, fh);
    fwrite(&p_end, sizeof(char), 1, fh);
  }
  fclose(fh);

  delete p;
  return 0;
}


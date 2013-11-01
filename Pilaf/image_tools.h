// Copyright (C) 2013, Peking University
// Author: Qi Chen (chenqi871025@gmail.com)
//
// Description:
//

#ifndef IMAGE_TOOLS_H
#define IMAGE_TOOLS_H
#include <iostream>
using namespace std;

inline uint32_t binaryToInt(const char * p, int len) {
  uint32_t result = 0 | p[len-1];
  for (int i = len-2; i >= 0; i--) {
    result = (result << (8 * sizeof(char))) | (0xFF & p[i]);
  }
  return result;
}

#ifndef SUB_BITS
#define SUB_BITS 32
#endif
inline int compute_hamming_dist(std::string code1, std::string code2) {
#if SUB_BITS == 64
  const uint64_t * p1 = (uint64_t *)(code1.c_str());
  const uint64_t * p2 = (uint64_t *)(code2.c_str());
#else
  const uint32_t * p1 = (uint32_t *)(code1.c_str());
  const uint32_t * p2 = (uint32_t *)(code2.c_str());
#endif

  int unit_len = SUB_BITS / 8 / sizeof(char);
  int len = code1.size() / unit_len;
  int dist = 0;

  for (int i = 0; i < len; i++) {
    dist += __builtin_popcount(*(p1+i) ^ *(p2+i));
  }
  return dist;
}

inline std::string binaryToString(const char *p, int len) {
  int char_bits = sizeof(char) * 8;
  std::string s(char_bits * len + 1 + len, '0');
  int k=0;
  for (int i = 0; i < len; i++) {
    for (int j = char_bits-1; j >=0; j--)
      s[k++] = (p[i] & (1 << j))? '1' : '0';
    s[k++] = ' ';
  }
  s[k] = '\0';
  return s;
}
#endif // IMAGE_TOOLS_H

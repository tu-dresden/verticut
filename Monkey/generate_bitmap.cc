#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <stdint.h>
#include <string.h>
#include <string>

static char* data_file = 0;
static uint32_t image_total = 100000000;
static void* table_1;
static void* table_2;
static void* table_3;
static void* table_4;

static struct option long_options[] = {
  {"data_file",     required_argument,  0,  'f'},
  {"image_total",   required_argument,  0,  'i'}
};

void usage(){
  printf("Usage : \n");
  exit(-1);
}

void parse_args(int argc, char* argv[]){
  int opt_index = 0;
  int opt;

  while((opt = getopt_long(argc, argv, "f:i:", long_options, &opt_index)) != -1){
    switch(opt){
      case 0:
        fprintf(stderr, "get_opt but?\n");
        break;
      
      case 'f':
        data_file = optarg;
        break;

      case 'i':
        image_total = atoi(optarg);
        break;

      case '?':
        usage();
        break;
      
      default:
        fprintf(stderr, "Invald arguments (%c)\n", opt);
        usage();
    } 
  }
}

void set_idx(uint32_t *ptr, uint32_t bit_idx){
  uint32_t word_idx = bit_idx / 32;
  bit_idx = bit_idx % 32;
  ptr[word_idx] |= (1 << bit_idx);
}

int main(int argc, char* argv[]){
  
  parse_args(argc, argv);

  if(data_file == 0){
    fprintf(stderr, "You must specify the input file with -f.\n");
    exit(-1);
  } 
  
  FILE* f = fopen(data_file, "r"); 
  if(f == NULL){
    fprintf(stderr, "Can't open file %s.\n", data_file);
    exit(-1);
  }
  
  char buffer[32];
  uint32_t segs[4];
  int i = 0;
  
  std::string f1_name;
  std::string f2_name;
  std::string f3_name;
  std::string f4_name;
  
  f1_name = std::string(data_file) + "_bmp_1_2b_4k.raw";
  f2_name = std::string(data_file) + "_bmp_2_2b_4k.raw";
  f3_name = std::string(data_file) + "_bmp_3_2b_4k.raw";
  f4_name = std::string(data_file) + "_bmp_4_2b_4k.raw";

  FILE* f1 = fopen(f1_name.c_str(), "w");
  FILE* f2 = fopen(f2_name.c_str(), "w");
  FILE* f3 = fopen(f3_name.c_str(), "w");
  FILE* f4 = fopen(f4_name.c_str(), "w");
  
  if(f1 == NULL || f2 == NULL || f3 == NULL || f4 == NULL){
    fprintf(stderr, "can't create files.\n");
    exit(-1);
  }

  unsigned long long size = ((unsigned long long)1<<32) / 8;
  table_1 = calloc(size, 1);
  table_2 = calloc(size, 1);
  table_3 = calloc(size, 1);
  table_4 = calloc(size, 1);

  while(fread(buffer, 4, 4, f)){
    memcpy(segs, buffer, 16);
    i++;
    if(i % 100000 == 0) 
      printf("%d\n", i);
    
    set_idx((uint32_t*)table_1, segs[0]);
    set_idx((uint32_t*)table_2, segs[1]);
    set_idx((uint32_t*)table_3, segs[2]);
    set_idx((uint32_t*)table_4, segs[3]);
    
    if(i == 40000000)
      break;

  }
  
  
  fwrite(table_1, size, 1, f1);
  fwrite(table_2, size, 1, f2);
  fwrite(table_3, size, 1, f3);
  fwrite(table_4, size, 1, f4);

  fclose(f);
  fclose(f1);
  fclose(f2);
  fclose(f3);
  fclose(f4);
  free(table_1);
  free(table_2);
  free(table_3);
  free(table_4);

  return 0;
}

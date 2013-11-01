//Parse arguments and set the environment.
#ifndef ARGS_CONFIG_H
#define ARGS_CONFIG_H

extern const char* config_path;
extern const char* server;
extern const char* binary_file;
extern int binary_bits;
extern int n_tables;
extern int read_mode;
extern int image_total;
extern int knn;

void configure(int argc, char* argv[]);

#endif

#ifndef IMAGE_SEARCH_CONSTANTS_H
#define IMAGE_SEARCH_CONSTATNS_H

#define DIE mpi_coordinator::finalize(); exit(-1);

#define MEMCACHED_CONFIG "../config/memcached.cnf"
#define PILAF_CONFIG "../config/pilaf.cnf"
#define REDIS_CONFIG "../config/redis.cnf"
#define DEFAULT_KNN 10
#define N_BINARY_BITS 128
#define DEFAULT_SERVER "pilaf"
#define BINARY_CODE_FILE "lsh.code"
#define DEFAULT_N_TABLES 4
#define DEFAULT_IMAGE_TOTAL 1000000
#define REPORT_SIZE 100000
#define DEFAULT_WORKERS_CONFIG "../config/workers.cnf"
#define DEFAULT_SERVER_PORT 9191
#define MEM_ID "image_search_project_bitmap"
#endif

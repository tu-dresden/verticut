/***********************************************
 *                                             *
 * -__ /\\     ,,         /\\                  *
 *   ||  \\  ' ||   _    ||                    *
 *  /||__|| \\ ||  < \, =||=                   *
 *  \||__|| || ||  /-||  ||                    *
 *   ||  |, || || (( ||  ||                    *
 * _-||-_/  \\ \\  \/\\  \\,                   *
 *   ||                                        *
 *                                             *
 *   Pilaf Infiniband DHT                      *
 *   (c) 2012-2013 Christopher Mitchell et al. *
 *   New York University, Courant Institute    *
 *   Networking and Wide-Area Systems Group    *
 *   All rights reserved.                      *
 *                                             *
 *  ploadtest.cc: Testing loading of key-value *
 *              pairs into key-value storages. *
 ***********************************************/

#include <iostream>
#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "../table_types.h"
#include "../store-server.h"
#include "../store-client.h"
#include "../ib.h"
#include "../dht.h"
#include "../config.h"
#if REDIS
#include "hiredis.h"
#endif
#if MEMCACHED
#include "libmemcached/memcached.h"
#endif

void unload_files();

#define MAX_PAIR_NUM 5000000
#define MAX_KEY_SIZE 30
#define MAX_VAL_SIZE 128
#define GET_PERCENTAGE 1000 //among 1000
#define TIMEVAL_NUM 100
#define LATENCY_NUM 10000
#define START_TIME 1 //s
#define INTERVAL_TIME 1 //s
//#define START_INTERVAL 5000000 //us
//#define TIME_INTERVAL 1000000 //us
struct timeval timeS;
struct timeval timeE;
int op_num[TIMEVAL_NUM+1];
int op_index=0;
int cur_op_count=0;
int latency[LATENCY_NUM];
int lindex=-1;
void measure(int num){
    if(op_index<TIMEVAL_NUM+1){
        op_num[op_index]=cur_op_count;
    }
    op_index++;
    if(lindex<0)
        lindex=0;
}
static int int_compare(const void *aptr, const void *bptr) {
    int a = *(int *)aptr;
    int b = *(int *)bptr;
    return (a-b);
}
#if REDIS
class Tclient{
    public:
        redisContext *c;
        redisReply *reply;
        Tclient(const char *conf){
            init(conf);
        }
        void init(const char *conf){
          struct timeval timeout = { 1, 500000 }; // 1.5 seconds

          ConfigReader config(conf);
        //  c->verbosity(VERB_DEBUG);
          while(!config.get_end()) {
            struct server_info* this_server = config.get_next();
            c = redisConnectWithTimeout(this_server->host->c_str(), atoi(this_server->port->c_str()), timeout);
            if (c->err) {
                printf("Connection error: %s\n", c->errstr);
                exit(1);
            }
            break;
          }
        }
        int get(char *key,redisReply *&r){
            r = (redisReply *)redisCommand(c,"GET %s",key);
//            printf("GET foo: %s\n", r->str);
            return ((r->type == REDIS_REPLY_STRING && r->str!=NULL))?0:-1;
        };
        int put(char *key,char *val){
            reply = (redisReply *)redisCommand(c,"SET %s %s", key, val);
//            printf("SET: %s\n", reply->str);
            int rval=((reply->type == REDIS_REPLY_STATUS && strcasecmp(reply->str,"OK") == 0))?0:-1;
            freeReplyObject(reply);
            return rval;
        }
};
#elif MEMCACHED
class Tclient{
    public:
        memcached_st *c;
        Tclient(const char *conf){
            init(conf);
        }
        void init(const char *conf){
            c = memcached_create(NULL); 
            ConfigReader config(conf);
        //  c->verbosity(VERB_DEBUG);
            while(!config.get_end()) {
                struct server_info* this_server = config.get_next();
                memcached_return rc=memcached_server_add(c,this_server->host->c_str(),atoi(this_server->port->c_str()));
                if(rc!=MEMCACHED_SUCCESS)
                    die("Failed to add server");
            }
            memcached_behavior_set(c, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 0);
        }
        int get(char *key,char **val){
            size_t val_length;
            uint32_t flags;
            memcached_return_t rc;
            *val = memcached_get(c, key, strlen(key),&val_length, &flags,&rc);
//            printf("key len %d val len %ld\n",strlen(key),strlen(val));
            return (rc==MEMCACHED_SUCCESS)?0:-1;
        };
        int put(char *key,char *val){
            memcached_return rc= memcached_set(c, key, strlen(key), val, strlen(val), (time_t)0, (uint32_t)0);
//            printf("key len %d val len %d\n",strlen(key),strlen(val));
            return (rc==MEMCACHED_SUCCESS)?0:-1;
        }
        ~Tclient(){
            delete c;
        };
};
#else
class Tclient{
    public:
        Client *c;
        Tclient(const char *conf){
            init(conf);
        }
        void init(const char *conf){
          c = new Client;
          ConfigReader config(conf);
//		  c->set_read_mode(READ_MODE_SERVER);
          c->verbosity(VERB_DEBUG);
          while(!config.get_end()) {
            struct server_info* this_server = config.get_next();
           if (c->add_server(this_server->host->c_str(),this_server->port->c_str()))
              die("Failed to add server");
//            printf("add server %s:%s\n",this_server->host->c_str(),this_server->port->c_str());
          }
         int rval;
          if (rval= c->ready())
            diewithcode("ready() failed with code:",rval);
        
        }
        int get(char *key,char *&val){
            return c->get(key,val);
        };
        int put(char *key,char *val){
            return c->put(key,val); 
        }
		void print_stats(void) {
			c->print_stats();
		}
        ~Tclient(){
//            c->print_stats();
            delete c;
        };
};
#endif
void get_test(Tclient *c,int count,int total,int iters = 1){
    size_t kbytes=0, vbytes=0;
    int ret;
    int keys_fd=-1;
    keys_fd=shm_open("shm_get_keysk",O_RDWR, 00777);
    if(-1==keys_fd){
        die("shm failed");
    }
    ret=ftruncate(keys_fd,MAX_KEY_SIZE*MAX_PAIR_NUM);
    if(-1==ret)
        die("keys ftruncate failed");
//    void *keys_addr=mmap(NULL,MAX_KEY_SIZE*MAX_PAIR_NUM,PROT_READ,MAP_SHARED,keys_fd,SEEK_SET);
    void *keys_addr=mmap(NULL,MAX_KEY_SIZE*MAX_PAIR_NUM,PROT_READ|PROT_WRITE,MAP_SHARED,keys_fd,SEEK_SET);
    if(NULL==keys_addr){
        die("mmap failed");
    }
    char *keys=(char *)keys_addr;
//    char* chk_buf_v = (char*)malloc(sizeof(char)*(1<<20)); //1 MB
//    char * chk_buf_v;
    struct timeval start;
    struct timeval end;
    int i,rval,pos;
    gettimeofday(&start,NULL);
#if REDIS
    redisReply *r;
    for(i=0;i<count;i++){
        pos=rand()%total;
        rval = c->get(&keys[pos*MAX_KEY_SIZE],r);
        if (rval) {
          fprintf(stderr,"Key '%s' was not found after being put\n",&keys[pos*MAX_KEY_SIZE]);
        }else{
            vbytes += (size_t)strlen(r->str);
        }
        kbytes += (size_t)strlen(&keys[pos*MAX_KEY_SIZE]);
        cur_op_count++;
        freeReplyObject(r);
    }
#elif MEMCACHED
    char * chk_buf_v;
    for(i=0;i<count;i++){
        pos=rand()%total;
        rval = c->get(&keys[pos*MAX_KEY_SIZE],&chk_buf_v);
        if (rval) {
          fprintf(stderr,"Key '%s' was not found after being put\n",&keys[pos*MAX_KEY_SIZE]);
        }else{
            vbytes += (size_t)strlen(chk_buf_v);
            free(chk_buf_v);
        }
        kbytes += (size_t)strlen(&keys[pos*MAX_KEY_SIZE]);
        cur_op_count++;
    }
#else
    char* chk_buf_v = (char*)malloc(sizeof(char)*(1<<20)); //1 MB
	do { 
	    for(i=0;i<count;i++){
		    pos=rand()%total;
//			pos=i;
	        rval = c->get(&keys[pos*MAX_KEY_SIZE],chk_buf_v);
	        if (rval) {
				fprintf(stderr,"Key '%s' was not found after being put\n",&keys[pos*MAX_KEY_SIZE]);
			}else{
				vbytes += (size_t)strlen(chk_buf_v);
	        }
		    kbytes += (size_t)strlen(&keys[pos*MAX_KEY_SIZE]);
			cur_op_count++;
		}
	} while (--iters);
    free(chk_buf_v);
#endif
    gettimeofday(&end,NULL);
//    free(chk_buf_v);
    printf("Successfully get %d k-v pairs with %zu bytes of keys and %zu bytes of vals\n",count,kbytes,vbytes);
    printf("Get Throughput: %f ops/sec \n",count*1.0/((end.tv_sec-start.tv_sec)+(end.tv_usec-start.tv_usec)*1.0/1000000));
    printf("Throughput number every %d sec begining with %d sec\n",INTERVAL_TIME,START_TIME);
    for(i=0;i<TIMEVAL_NUM+1;i++){
        if(i==0)
            printf("%d\n",op_num[i]);
        else{
            int res=op_num[i]-op_num[i-1];
            if(res<0)
                res=0;
            printf("%d\n",res);
        }
    }
	c->print_stats();
}
void mixed_test(Tclient *c,int count, int total){
    size_t kbytes=0, vbytes=0;
    size_t get_kbytes=0, get_vbytes=0;
    int ret;
    int keys_fd=-1;
    int values_fd=-1;
    int get_keys_fd=-1;
    keys_fd=shm_open("shm_put_keysk",O_RDWR, 00777);
    values_fd=shm_open("shm_put_values",O_RDWR, 00777);
    if(-1==keys_fd|| -1==values_fd){
        die("shm failed");
    }
    ret=ftruncate(keys_fd,MAX_KEY_SIZE*MAX_PAIR_NUM);
    if(-1==ret)
        die("keys ftruncate failed");
    ret=ftruncate(values_fd,MAX_VAL_SIZE*MAX_PAIR_NUM);
    if(-1==ret)
        die("values ftruncate failed");
    void *keys_addr=mmap(NULL,MAX_KEY_SIZE*MAX_PAIR_NUM,PROT_READ|PROT_WRITE,MAP_SHARED,keys_fd,SEEK_SET);
    void *values_addr=mmap(NULL,MAX_VAL_SIZE*MAX_PAIR_NUM,PROT_READ|PROT_WRITE,MAP_SHARED,values_fd,SEEK_SET);
    if(NULL==keys_addr||NULL==values_addr){
        die("mmap failed");
    }
    char *keys=(char *)keys_addr;
    char *values=(char *)values_addr;
    get_keys_fd=shm_open("shm_get_keysk",O_RDWR|O_CREAT, 00777);
    if(-1==get_keys_fd){
        die("shm failed");
    }
    ret=ftruncate(get_keys_fd,MAX_KEY_SIZE*MAX_PAIR_NUM);
    if(-1==ret)
        die("keys ftruncate failed");
    void *get_keys_addr=mmap(NULL,MAX_KEY_SIZE*MAX_PAIR_NUM,PROT_READ|PROT_WRITE,MAP_SHARED,get_keys_fd,SEEK_SET);
    if(NULL==get_keys_addr){
        die("mmap failed");
    }
    char *get_keys=(char *)get_keys_addr;
    
    struct timeval start;
    struct timeval end;
    struct timeval lata;
    struct timeval latb;
    int i,rval;
    int cond,pos;
#if REDIS
    redisReply *r;
#elif MEMCACHED
    char *chk_buf_v;
#else
    char * chk_buf_v=(char *)malloc(sizeof(char)*(1<<20));
#endif 
    int p_count=0,g_count=0;
    int n=0;
    gettimeofday(&start,NULL);
    for(i=0;i<count;i++){
        cond=rand()%1000;
        pos=rand()%total;
        gettimeofday(&lata,NULL);
        if(cond>=GET_PERCENTAGE){
            
            rval = c->put(&keys[pos*MAX_KEY_SIZE],&values[pos*MAX_VAL_SIZE]); 
            if (rval < 0) {
              fprintf(stderr,"Failed with code %d to put key='%s',val='%s'\n",rval,&keys[pos*MAX_KEY_SIZE],&values[pos*MAX_VAL_SIZE]);
            }
//            printf("key %s ,val %s\n",&keys[pos*MAX_KEY_SIZE],&values[pos*MAX_VAL_SIZE]);
            kbytes += (size_t)strlen(&keys[pos*MAX_KEY_SIZE]);
            vbytes += (size_t)strlen(&values[pos*MAX_VAL_SIZE]);
            p_count++;
        }else{
#if REDIS
            rval = c->get(&get_keys[pos*MAX_KEY_SIZE],r);
//            printf("key %s ,val %s\n",&get_keys[pos*MAX_KEY_SIZE],r->str);
            if (rval) {
              fprintf(stderr,"Key '%s' was not found after being put\n",&get_keys[pos*MAX_KEY_SIZE]);
            }else{
                get_vbytes += (size_t)strlen(r->str);
            }
            freeReplyObject(r);
#elif MEMCACHED
            rval = c->get(&get_keys[pos*MAX_KEY_SIZE],&chk_buf_v);
            if (rval) {
              fprintf(stderr,"Key '%s' was not found after being put\n",&get_keys[pos*MAX_KEY_SIZE]);
            }else{
                get_vbytes += (size_t)strlen(chk_buf_v);
                free(chk_buf_v);
            }
#else
            rval = c->get(&get_keys[pos*MAX_KEY_SIZE],chk_buf_v);
            if (rval) {
              fprintf(stderr,"Key '%s' was not found after being put\n",&get_keys[pos*MAX_KEY_SIZE]);
            }else{
                get_vbytes += (size_t)strlen(chk_buf_v);
            }
#endif
            get_kbytes += (size_t)strlen(&get_keys[pos*MAX_KEY_SIZE]);
            g_count++;
        }
        gettimeofday(&latb,NULL);
//        if(lindex<0&&(count-i==2*LATENCY_NUM)){
//            lindex=0;
//        }
//        if(lindex<LATENCY_NUM&&lindex>=0){
//            latency[lindex]=(latb.tv_sec-lata.tv_sec)*1000000+(latb.tv_usec-lata.tv_usec);
//            lindex++;
//        }
        cur_op_count++;
   }
#if REDIS
#elif MEMCACHED
#else
    free(chk_buf_v);
#endif
    gettimeofday(&end,NULL);
    printf("Successfully put %d k-v pairs with %zu bytes of keys and %zu bytes of vals\n",p_count,kbytes,vbytes);
    printf("Successfully get %d k-v pairs with %zu bytes of keys and %zu bytes of vals\n",g_count,get_kbytes,get_vbytes);
    printf("Put Throughput: %f ops/sec \n",count*1.0/((end.tv_sec-start.tv_sec)+(end.tv_usec-start.tv_usec)*1.0/1000000)); 
    qsort(latency, LATENCY_NUM, sizeof(int), int_compare);
//    printf("valid %d latencies ",lindex);
//    printf("RTT min %d us, 1p %d us, 10p %d us, median %d us, 90p %d us, 99p %d us, max %d us\n", latency[0], latency[LATENCY_NUM/100],latency[LATENCY_NUM/10],latency[LATENCY_NUM/2],latency[(LATENCY_NUM*90)/100], latency[(LATENCY_NUM*99)/100], latency[LATENCY_NUM-1]);
//    for(int i=0;i<LATENCY_NUM;i++){
//        printf("%d\n",latency[i]);
//    }
    printf("Throughput number every %d sec begining with %d sec\n",INTERVAL_TIME,START_TIME);
    for(i=0;i<TIMEVAL_NUM+1;i++){
        if(i==0)
            printf("%d\n",op_num[i]);
        else{
            int res=op_num[i]-op_num[i-1];
            if(res<0)
                res=0;
            printf("%d\n",res);
        }
    }
}
//void get_test(Tclient *c,int count,int total){
//    size_t kbytes=0, vbytes=0;
//    int ret;
//    int keys_fd=-1;
//    keys_fd=shm_open("shm_get_keysk",O_RDWR|O_CREAT, 00777);
//    if(-1==keys_fd){
//        die("shm failed");
//    }
//    ret=ftruncate(keys_fd,MAX_KEY_SIZE*MAX_PAIR_NUM);
//    if(-1==ret)
//        die("keys ftruncate failed");
//    void *keys_addr=mmap(NULL,MAX_KEY_SIZE*MAX_PAIR_NUM,PROT_READ|PROT_WRITE,MAP_SHARED,keys_fd,SEEK_SET);
//    if(NULL==keys_addr){
//        die("mmap failed");
//    }
//    char *keys=(char *)keys_addr;
//    char* chk_buf_v = (char*)malloc(sizeof(char)*(1<<20)); //1 MB
//    if(NULL==chk_buf_v)
//        die("chk_buf_v error\n");
//    struct timeval start;
//    struct timeval end;
//    gettimeofday(&start,NULL);
//    int i,rval,pos;
//    for(i=0;i<count;i++){
//        pos=rand()%total;
////        printf("pos %d %s\n",pos,&keys[pos*MAX_KEY_SIZE]);
////        rval = c->get(&keys[pos*MAX_KEY_SIZE],&chk_buf_v);
//        rval = c->get(&keys[pos*MAX_KEY_SIZE],chk_buf_v);
//        if (rval) {
//          fprintf(stderr,"Key '%s' was not found after being put\n",&keys[pos*MAX_KEY_SIZE]);
//    //    } else if (strlen(&values[pos*MAX_VAL_SIZE]) != strlen(chk_buf_v)) {
//    //      fprintf(stderr,"For key %s, put length %zu but got length %zu\n",&keys[i*MAX_KEY_SIZE],strlen(&values[pos*MAX_VAL_SIZE]),strlen(chk_buf_v));
//    //      fprintf(stderr,"PUT: '%s'\nGOT: '%s'\n",&values[pos*MAX_VAL_SIZE],chk_buf_v);
//        }else{
////            printf("chk_buf_v len %ld\n",strlen(chk_buf_v));
//            vbytes += (size_t)strlen(chk_buf_v);
//        }
//        kbytes += (size_t)strlen(&keys[pos*MAX_KEY_SIZE]);
//    }
//    gettimeofday(&end,NULL);
//    free(chk_buf_v);
//    printf("Successfully get %d k-v pairs with %zu bytes of keys and %zu bytes of vals\n",count,kbytes,vbytes);
//    printf("Get Throughput: %f ops/sec \n",count*1.0/((end.tv_sec-start.tv_sec)+(end.tv_usec-start.tv_usec)*1.0/1000000));
//}
int load_put_file(char *filename){
    FILE* fh;
    if (NULL == (fh = fopen(filename,"r"))) {
    die("Failed to read given k-v file");
    }      

    char* buf_k = (char*)malloc(sizeof(char)*(1<<20)); //1 MB
    char* buf_v = (char*)malloc(sizeof(char)*(1<<20)); //1 MB


    if (buf_k == NULL || buf_v == NULL) {
        die("Failed to reserve mem for k/v buffers");
    }

    int lines=0;
//    size_t kbytes=0, vbytes=0;
    int ret;
    int keys_fd=-1;
    int values_fd=-1;
    keys_fd=shm_open("shm_put_keysk",O_RDWR|O_CREAT, 00777);
    values_fd=shm_open("shm_put_values",O_RDWR|O_CREAT, 00777);
    if(-1==keys_fd|| -1==values_fd){
    die("shm failed");
    }
    ret=ftruncate(keys_fd,MAX_KEY_SIZE*MAX_PAIR_NUM);
    if(-1==ret)
      die("keys ftruncate failed");
    ret=ftruncate(values_fd,MAX_VAL_SIZE*MAX_PAIR_NUM);
    if(-1==ret)
      die("values ftruncate failed");
    void *keys_addr=mmap(NULL,MAX_KEY_SIZE*MAX_PAIR_NUM,PROT_READ|PROT_WRITE,MAP_SHARED,keys_fd,SEEK_SET);
    void *values_addr=mmap(NULL,MAX_VAL_SIZE*MAX_PAIR_NUM,PROT_READ|PROT_WRITE,MAP_SHARED,values_fd,SEEK_SET);
    if(NULL==keys_addr||NULL==values_addr){
    die("mmap failed");
    }
    char *keys=(char *)keys_addr;
    char *values=(char *)values_addr;
    long lkey;
    size_t max_keylen=0;
    size_t max_vallen=0;
    while(!feof(fh)) {
        int rval;

        if (NULL == fgets(buf_k,(1<<20),fh) || feof(fh)) //key
          break;
        if (NULL == fgets(buf_v,(1<<20),fh))  //value
          break;

        // Cut off trailing newlines
        if (buf_k[strlen(buf_k)-1] == '\n')
          buf_k[strlen(buf_k)-1] = '\0';
        if (buf_v[strlen(buf_v)-1] == '\n')
          buf_v[strlen(buf_v)-1] = '\0';

//        sscanf(&buf_k[4],"%ld",&lkey);
//        memcpy(buf_k,(char *)(&lkey),8);
//        buf_k[8]='\0';
//        printf("key :%ld\n",*((long *)(buf_k)));
//        exit(1);
//        printf("key :%s\n",&keys[lines*MAX_KEY_SIZE]);
//        printf("lines %d, keylen %ld ",lines,strlen(buf_k));
//        printf("vallen %ld \n",strlen(buf_v));
        strcpy(&keys[lines*MAX_KEY_SIZE],buf_k);
        strcpy(&values[lines*MAX_VAL_SIZE],buf_v);
        if(strlen(buf_k)>max_keylen){
            max_keylen=strlen(buf_k);
        }
        if(strlen(buf_v)>max_vallen){
            max_vallen=strlen(buf_v);
        }
    //    kbytes += (size_t)strlen(buf_k);
    //    vbytes += (size_t)strlen(buf_v);
        lines++;
    }
    free(buf_k);
    free(buf_v);
    fprintf(stdout,"max key size:%ld\n",max_keylen);
    fprintf(stdout,"max value size:%ld\n",max_vallen);
    ret=munmap(keys_addr,MAX_KEY_SIZE*MAX_PAIR_NUM);
    if(-1==ret)
        die("Failed to munmap keys");
    ret=munmap(values_addr,MAX_KEY_SIZE*MAX_PAIR_NUM);
    if(-1==ret)
        die("Failed to munmap vlaues");
    if(max_keylen > MAX_KEY_SIZE || max_vallen > MAX_VAL_SIZE) {
        std::cout << "Key or value longer than expected." << std::endl;
        std::cout << "Max expected key len: " << MAX_KEY_SIZE << std::endl;
        std::cout << "Longest loaded key: " << max_keylen << std::endl;
        std::cout << "Max expected value len: " << MAX_VAL_SIZE << std::endl;
        std::cout << "Longest loaded value: " << max_vallen << std::endl;
        std::cout << "Adjust MAX_KEY_SIZE and/or MAX_VAL_SIZE and recompile." << std::endl;
        unload_files();
        exit(EXIT_FAILURE);
    }
    return lines;    
}
int load_get_file(char *filename){
    FILE* fh;
    if(NULL==(fh=fopen(filename,"r"))){
        die("Faild to read given k-v files"); 
    }
    char *buf_k=(char*)malloc(sizeof(char)*(1<<20));
    if(NULL==buf_k){
        die("Failed to reserve mem for k/v buffers");
    }
    int lines=0;
//    size_t kbytes=0, vbytes=0;
    int ret;
    int keys_fd=-1;
    keys_fd=shm_open("shm_get_keysk",O_RDWR|O_CREAT, 00777);
      if(-1==keys_fd){
        die("shm failed");
      }
      ret=ftruncate(keys_fd,MAX_KEY_SIZE*MAX_PAIR_NUM);
      if(-1==ret)
          die("keys ftruncate failed");
      void *keys_addr=mmap(NULL,MAX_KEY_SIZE*MAX_PAIR_NUM,PROT_READ|PROT_WRITE,MAP_SHARED,keys_fd,SEEK_SET);
      if(NULL==keys_addr){
        die("mmap failed");
      }
      char *keys=(char *)keys_addr;
      long lkey;
      while(!feof(fh)) {
        int rval;

        if (NULL == fgets(buf_k,(1<<20),fh) || feof(fh)) //key
          break;

        // Cut off trailing newlines
        if (buf_k[strlen(buf_k)-1] == '\n')
          buf_k[strlen(buf_k)-1] = '\0';

//        sscanf(&buf_k[4],"%ld",&lkey);
//        memcpy(buf_k,(char *)(&lkey),8);
//        buf_k[8]='\0';
        strcpy(&keys[lines*MAX_KEY_SIZE],buf_k);
//        kbytes += (size_t)strlen(buf_k);
//        vbytes += (size_t)strlen(buf_v);
        lines++;
      }
      free(buf_k);
      ret=munmap(keys_addr,MAX_KEY_SIZE*MAX_PAIR_NUM);
      if(-1==ret)
        die("Failed to munmap keys");
      return lines;
}
void unload_files(){
      int ret;
      ret=shm_unlink("shm_put_keysk");
      if(ret==-1){
        die("shm_unlink failed");
      }
      ret=shm_unlink("shm_put_values");
      if(ret==-1){
        die("shm_unlink failed");
      }
      ret=shm_unlink("shm_get_keysk");
      if(ret==-1){
        die("shm_unlink failed");
      }
}
void put_test(Tclient *c,int begin, int count, bool run_forever = false){
//    struct timeval start1;
//    struct timeval end1;
//    gettimeofday(&start1,NULL);
    size_t kbytes=0, vbytes=0;
    int ret;
    int keys_fd=-1;
    int values_fd=-1;
    keys_fd=shm_open("shm_put_keysk",O_RDWR, 00777);
    values_fd=shm_open("shm_put_values",O_RDWR, 00777);
    if(-1==keys_fd|| -1==values_fd){
        die("shm failed");
    }
    ret=ftruncate(keys_fd,MAX_KEY_SIZE*MAX_PAIR_NUM);
    if(-1==ret)
        die("keys ftruncate failed");
    ret=ftruncate(values_fd,MAX_VAL_SIZE*MAX_PAIR_NUM);
    if(-1==ret)
        die("values ftruncate failed");
    void *keys_addr=mmap(NULL,MAX_KEY_SIZE*MAX_PAIR_NUM,PROT_READ|PROT_WRITE,MAP_SHARED,keys_fd,SEEK_SET);
    void *values_addr=mmap(NULL,MAX_VAL_SIZE*MAX_PAIR_NUM,PROT_READ|PROT_WRITE,MAP_SHARED,values_fd,SEEK_SET);
    if(NULL==keys_addr||NULL==values_addr){
        die("mmap failed");
    }
//    gettimeofday(&end1,NULL);
    char *keys=(char *)keys_addr;
    char *values=(char *)values_addr;
    struct timeval start;
    struct timeval end;
    gettimeofday(&start,NULL);
    int i,rval,pos;
    char * chk_buf_v=(char *)malloc(sizeof(char)*(1<<20));
    int bound=begin+count;
	do {
	    for(i=begin;i<bound;i++){
		    pos=i;
//	        pos=rand()%1000000;
			rval = c->put(&keys[pos*MAX_KEY_SIZE],&values[pos*MAX_VAL_SIZE]); 
//			rval = c->get(&keys[i*MAX_KEY_SIZE],chk_buf_v); 
//			c->get("a",NULL);
			if (rval < 0) {
				fprintf(stderr,"Failed with code %d to put key='%s',val='%s'\n",rval,&keys[pos*MAX_KEY_SIZE],&values[pos*MAX_VAL_SIZE]);
			}
//            fprintf(stdout,"Put key '%s' val '%s'\n",&keys[pos*MAX_KEY_SIZE],&values[pos*MAX_VAL_SIZE]);
			kbytes += (size_t)strlen(&keys[pos*MAX_KEY_SIZE]);
			vbytes += (size_t)strlen(&values[pos*MAX_VAL_SIZE]);
			cur_op_count++;
	   }
	} while(run_forever);        

    gettimeofday(&end,NULL);
    printf("Successfully put %d k-v pairs with %zu bytes of keys and %zu bytes of vals\n",count,kbytes,vbytes);
    printf("Put Throughput: %f ops/sec \n",count*1.0/((end.tv_sec-start.tv_sec)+(end.tv_usec-start.tv_usec)*1.0/1000000)); 
#if 0
    printf("Throughput number every %d sec begining with %d sec\n",INTERVAL_TIME,START_TIME);
    for(i=0;i<TIMEVAL_NUM+1;i++){
        if(i==0)
            printf("%d\n",op_num[i]);
        else{
            int res=op_num[i]-op_num[i-1];
            if(res<0)
                res=0;
            printf("%d\n",res);
        }
    }
#endif
}
void usage(char **argv){
    printf("Usage:%s l <k-v file for putting> <k file for gettting> \t[load files into the shared memory]\n",argv[0]);
    printf("      %s d \t[unload files in the shared memory]\n",argv[0]);
    printf("      %s p <start> <count> <server_config> \t[run the putting test]\n",argv[0]);
    printf("      %s g <count> <number of key in file> <server_config> \t[run the getting test]\n",argv[0]);
    printf("      %s m <count> <number of key in file> <server_config> \t[run the mixed test]\n",argv[0]);
}


int main(int argc, char **argv) {
  
//  gettimeofday(&timeS,NULL);
  signal(SIGALRM, measure);
  struct itimerval tick;
  memset(&tick, 0, sizeof(tick));
  tick.it_value.tv_sec = START_TIME;  
  tick.it_value.tv_usec = 0; 
  tick.it_interval.tv_sec  =INTERVAL_TIME; 
  tick.it_interval.tv_usec = 0;
  int res=setitimer(ITIMER_REAL,&tick,NULL);
  if(res){
    printf("set timer failed!\n");
  }
  memset(op_num, 0, sizeof(int)*(TIMEVAL_NUM+1));
  memset(latency,0, sizeof(int)*LATENCY_NUM);  

  int rval=0;
  if (argc < 2||strcmp(argv[1],"-h")==0) {
      usage(argv);
      exit(-1);
  } 
  if(strcmp(argv[1],"g")==0){
    Tclient *c =new Tclient(argv[4]);
	int iters = 1;
	if (argc >= 6) {
		iters = atoi(argv[5]);
	}
    get_test(c,atoi(argv[2]),atoi(argv[3]),iters); 
    delete c;
  }else if(strcmp(argv[1],"p")==0){
    Tclient *c =new Tclient(argv[4]);
	bool run_forever = false;
	if (argc >= 6) {
		run_forever = true;
	}
    put_test(c,atoi(argv[2]),atoi(argv[3]),run_forever); 
    delete c;
  }else if(strcmp(argv[1],"m")==0){
    Tclient *c =new Tclient(argv[4]);
    mixed_test(c,atoi(argv[2]),atoi(argv[3])); 
    delete c;
  }else if(strcmp(argv[1],"l")==0){
      load_put_file(argv[2]);
      load_get_file(argv[3]);
  }else if(strcmp(argv[1],"d")==0){
      unload_files();
  }
  return 0;
}


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
 *   loadtest.cc: Testing loading of key-value *
 *                pairs into a Pilaf DHT.      *
 ***********************************************/

#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>

#include "table_types.h"
#include "store-server.h"
#include "store-client.h"
#include "ib.h"
#include "dht.h"
#include "config.h"

#define uint64_t unsigned long long int

int main(int argc, char **argv) {
  int rval=0;

  if (argc != 3) {
    printf("Usage: %s <server_config> <k-v_file>\n",argv[0]);
    exit(-1);
  }

  Client* c = new Client;
  ConfigReader config(argv[1]);

//  c->verbosity(VERB_DEBUG);

  while(!config.get_end()) {
    struct server_info* this_server = config.get_next();
    if (c->add_server(this_server->host->c_str(),this_server->port->c_str()))
      die("Failed to add server");
  }

  FILE* fh;
  if (NULL == (fh = fopen(argv[2],"r"))) {
    die("Failed to read given k-v file");
  }      

  if (rval = c->ready())
    diewithcode("ready() failed with code:",rval);

  char* buf_k = (char*)malloc(sizeof(char)*(1<<20)); //1 MB
  char* buf_v = (char*)malloc(sizeof(char)*(1<<20)); //1 MB

  char* chk_buf_v = (char*)malloc(sizeof(char)*(1<<20)); //1 MB

  if (buf_k == NULL || buf_v == NULL) {
    die("Failed to reserve mem for k/v buffers");
  }

  int lines=0;
  size_t kbytes=0, vbytes=0;
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

    rval = c->put(buf_k,buf_v); 
    if (rval < 0) {
      fprintf(stderr,"Failed with code %d to put key='%s',val='%s'\n",rval,buf_k,buf_v);
    }
    //fprintf(stdout,"Put key '%s' val '%s'\n",buf_k,buf_v);

    rval = c->get(buf_k,chk_buf_v);
    if (rval) {
      fprintf(stderr,"Key '%s' was not found after being put\n",buf_k);
    } else if (strlen(buf_v) != strlen(chk_buf_v)) {
      fprintf(stderr,"For key %s, put length %zu but got length %zu\n",buf_k,strlen(buf_v),strlen(chk_buf_v));
      fprintf(stderr,"PUT: '%s'\nGOT: '%s'\n",buf_v,chk_buf_v);
    }
    kbytes += (size_t)strlen(buf_k);
    vbytes += (size_t)strlen(buf_v);
    lines++;
  }

  printf("Successfully put %d k-v pairs with %zu bytes of keys and %zu bytes of vals\n",lines,kbytes,vbytes);
  c->print_stats();
  return 0;
}


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
 *   dht-test.cc: DHT put/get driver           *
 ***********************************************/

#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>

#include <boost/random.hpp>
#include <boost/random/normal_distribution.hpp>

#include "table_types.h"
#include "store-server.h"
#include "store-client.h"
#include "ibman.h"
#include "config.h"
#include <signal.h>
#include <getopt.h>

#define uint64_t unsigned long long int

enum tests {
  TEST_GET,
  TEST_PUT,
  TEST_CONTDEL,
};

#define TEST_STEP 19
//#define CHECK_GET_RESULT
#define CHECK_GET_SUCCESS
#define DHT_SIZE 10000
const int STRESS_COUNT_BASE = 2000000;

void usage(const char *argv0);

// Testing variables
size_t updated_key_count_;
size_t locked_reread_count_;
size_t data_xchgs;
size_t bytes_xchged;
int test_type_;
size_t key_read_offset;
int role;

// Used by client-role IBDHTCs
DHTClient<KEY_TYPE,VAL_TYPE>* dhtclient;

// Used for orderly Server shutdown
void sighandler(int);
Server* s = NULL;

// Arguments
static struct option long_options[] = {
  {"client",  required_argument, 0,  'c' },
  {"server",  required_argument, 0,  's' },
  {"test",    required_argument, 0,  't' },
  {"logging", required_argument, 0,  'l' },
  {"port",    required_argument, 0,  'p' },
  {"dump",    required_argument, 0,  'd' },
  {"randomize", no_argument,     0,  'r' },
  {"presize", no_argument,       0,  'P' },
  {"thru",    no_argument,       0,  'T' },
  {0,         0,                 0,  0   }
};

int main(int argc, char **argv) {

  // Test Parameters
  char* test = "p";
  char* configpath = NULL;
  char* logfile = NULL;
  char* dump_data = NULL;
  int role = R_SERVER;
  unsigned short port = 36001;
  int verb = VERB_ERROR;
  bool randomize = false;
  bool thru = false;
  size_t presize = 0;

  int c;
  while (1) {
    int this_option_optind = optind ? optind : 1;
    int option_index = 0;

    c = getopt_long(argc, argv, "c:s:t:l:vqTP:r",
                    long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
      case 0:
        fprintf(stderr,"get_opt bug?\n");
        break;

      case 't':
        test = optarg; //valid are 'p', 'g', 'cd', 'Lp', and 'Lg'
        break;

      case 'c':
        role = R_CLIENT;
        configpath = optarg;
        break;

      case 's':
        role = R_SERVER;
        port = (short)atoi(optarg);
        break;

      case 'l':
        logfile = optarg;
        break;

      case 'd':
        dump_data = optarg;
        break;

      case 'v':
        verb++;
        break;

      case 'q':
        verb = VERB_VITAL;
        break;

      case 'T':
        thru = true;
        break;

      case 'r':
        randomize = true;
        break;

      case 'P':
        presize = (size_t)strtoul(optarg,NULL,0);
        break;

      default:
        printf("Invalid argument (%c)\n",c);

      case '?':
        usage(argv[0]);
        break;

    }
  }

  if (role == R_SERVER) {

    signal(SIGUSR1, sighandler);
    // Set up server
    s = new Server;

    s->setup();
    s->verbosity((ibman_verb)verb);

    if (logfile != NULL) {
      s->set_logging(true,logfile);
    }

    // Start the server running
    if (argc >= 3) {
      unsigned short port = atoi(argv[2]);
      s->ready(port);
    } else {
      s->ready();
    }

	if (presize) {
      s->dht.resize(presize*2+1);
      if (test[0] == 'L') {
        s->dht.resize_extents(presize*2UL*(1024UL+64UL));
      } else {
        s->dht.resize_extents(presize*2UL*(64UL+16UL));
      }
    }

  } else {

    // Test-specific vars
    data_xchgs = 0;
    updated_key_count_ = 0;
    locked_reread_count_ = 0;
    key_read_offset = rand() % DHT_SIZE;
    struct timeval start_time, end_time;

    // Create new client
    Client* c = new Client;
    int test_type = TEST_GET;
    if (!strcmp("g",test) || !strcmp("Lg",test))
      test_type = TEST_GET;
    else if (!strcmp("p",test) || !strcmp("Lp",test))
      test_type = TEST_PUT;
    else if (!strcmp("cd",test) || !strcmp("Lcd",test))
      test_type = TEST_CONTDEL;
    else
      test_type = TEST_GET;

    char *mpi_usize = NULL, *mpi_urank = NULL;
	size_t rank = 0;
    bool mpi = false;
    if (NULL != (mpi_urank = getenv("OMPI_COMM_WORLD_RANK"))) {
      rank = atoi(mpi_urank);
      mpi = true;
    }
    if (mpi) {
      printf("Running under MPI. I am worker rank %zu.\n",rank+1);
    }

    size_t STRESS_COUNT = STRESS_COUNT_BASE;
    if (presize) {
      STRESS_COUNT = presize;
    }

    printf("Generating %zu k-v pairs of workload\n",STRESS_COUNT);
    std::vector<char*> keys;
    std::vector<char*> vals;

    int key_len = 8;
    int val_len = 64;
    key_len = (test[0] == 'L')?64:8;
    val_len = (test[0] == 'L')?1024:64;

    boost::mt19937 rnga, rngb;
    boost::normal_distribution<> nd_key(key_len, key_len/3.f);
    boost::normal_distribution<> nd_val(val_len, val_len/2.f);

    boost::variate_generator<boost::mt19937&,
                             boost::normal_distribution<> > key_nor(rnga, nd_key);
    boost::variate_generator<boost::mt19937&,
                             boost::normal_distribution<> > val_nor(rngb, nd_val);

    key_nor.engine().seed(mpi?(1337+rank):1337);
    val_nor.engine().seed(mpi?(42+rank):42);
    srand(34);

    size_t bytes = 0;
    for(int i=0; i<STRESS_COUNT; i++) {
      size_t klen = 0, vlen = 0;

      do {
        double klen_;
        klen_ = key_nor();
        if (klen_ <= 1)
          continue;
        klen = (size_t)klen_;
      } while (klen <= 1);

      do {
        double vlen_;
        vlen_ = val_nor();
        if (vlen_ <= 1)
          continue;
        vlen = (size_t)vlen_;
      } while (vlen <= 1);

      const char* charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
      char* key = (char*)malloc(sizeof(char)*klen);
      char* val = (char*)malloc(sizeof(char)*vlen);

      bytes += klen + vlen;

      for (int i=0; i<klen; i++) {
        key[i] = charset[rand() % (26+26+10)];
      }
      for (int i=0; i<vlen; i++) {
        val[i] = charset[rand() % (26+26+10)];
      }
      key[klen-1] = val[vlen-1] = '\0';

      keys.push_back(key);
      vals.push_back(val);
    }

    printf("Generated %zu bytes of k-v; beginning requested test\n",bytes);

    if (dump_data != NULL) {
      FILE* fh;
      if (NULL == (fh = fopen("dht-test-dump","w"))) {
        die("Failed to open k-v file for writing");
      }
      std::vector<char*>::iterator itk = keys.begin();
      std::vector<char*>::iterator itv = vals.begin();

      size_t max_keylen = 0, max_vallen = 0;
      for(; itk != keys.end(); itk++, itv++) {
        if (strlen(*itk) > max_keylen) max_keylen = strlen(*itk);
        if (strlen(*itv) > max_vallen) max_vallen = strlen(*itv);
        fprintf(fh,"%s\n%s\n",*itk,*itv);
      }
      fclose(fh);
      printf("Finished dumping data to %s\nMax keylen: %zu\nMax vallen:%zu\n",
             dump_data,max_keylen,max_vallen);
    }

	if (c->setup())
      die("Failed to set up client");

    ConfigReader config(argv[2]);

    c->verbosity((ibman_verb)verb);

    while(!config.get_end()) {
      struct server_info* this_server = config.get_next();
      if (c->add_server(this_server->host->c_str(),this_server->port->c_str()))
        die("Failed to add server");
    }

    c->set_read_mode(READ_MODE_SERVER);

    c->ready();

    // Set starting time and begin testing
    printf("Setup is complete; beginning test\n");

    if (!thru) {
      gettimeofday(&start_time, NULL);
    }

    size_t thru_count = (STRESS_COUNT/5);
	size_t thru_offset = 2*(STRESS_COUNT/5);

    int rval;
    if (test_type == TEST_GET) {
      char* val = (char*)malloc(sizeof(char)*16384);

      std::vector<char*>::iterator itk = keys.begin();
      std::vector<char*>::iterator itv = vals.begin();

      for(; itk < keys.end(); itk++, itv++) {

      #if KEY_VAL_PAIRTYPE==KVPT_SIZET_DOUBLE
        double val;
        rval = c->get(key_read_offset,val);

        //if (rval != POST_GET_FOUND) die("Failed to get key");
        //printf("%s: key %lu is %s!\n",role_to_str(),key_read_offset,(rval==POST_GET_FOUND)?"FOUND":"MISSING");

      #ifdef CHECK_GET_RESULT
        if (rval == POST_GET_FOUND) {
          if (val == key_read_offset) {
            updated_key_count_++;
          } else if (val != sqrt(key_read_offset%311)) {
            printf("Fetched key %lu with corrupt value %f\n",key_read_offset,val);
            die("Aborting.");
          }
        }
      #endif
        key_read_offset = (key_read_offset+TEST_STEP)%DHT_SIZE;
        data_xchgs++;
      #elif KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP
        rval = c->get(*itk,val);

        #ifdef CHECK_GET_SUCCESS
        if (rval != POST_GET_FOUND) {
          fprintf(stderr,"Failed to get key %s [%d]\n", *itk, rval);
          die("aborting.");
        }
        #endif

        #ifdef CHECK_GET_RESULT
        if (strcmp(val,*itv)) {
          fprintf(stderr,"Got wrong value for key '%s': '%s' vs '%s'\n", *itk, *itv, val);
          die("aborting.");
        }
        #endif

        data_xchgs++;
      #else
        die("get: Unknown K/V pairtype");
      #endif
        if (thru) {
          if (data_xchgs == thru_offset) {
            gettimeofday(&start_time, NULL);
          } else if (data_xchgs == thru_offset + thru_count) {
            gettimeofday(&end_time, NULL);
          }
        }

      }
    } else if (test_type == TEST_PUT) {

      std::vector<char*>::iterator itk = keys.begin();
      std::vector<char*>::iterator itv = vals.begin();

      for(; itk < keys.end(); itk++, itv++) {

      #if KEY_VAL_PAIRTYPE==KVPT_SIZET_DOUBLE
        rval = c->put(key_read_offset,(VAL_TYPE)key_read_offset);
        key_read_offset = (key_read_offset+TEST_STEP)%DHT_SIZE;

      #elif KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP

        // Create a key-val pair
        rval = c->put(*itk,*itv);
      #endif

        if (!rval)
          data_xchgs++;

        if (thru) {
          if (data_xchgs == thru_offset) {
            gettimeofday(&start_time, NULL);
          } else if (data_xchgs == thru_offset + thru_count) {
            gettimeofday(&end_time, NULL);
          }
        }

      }
    } else if (test_type == TEST_CONTDEL) {
      std::vector<char*>::iterator itk = keys.begin();
      std::vector<char*>::iterator itv = vals.begin();

      for(; itk < keys.end(); itk++, itv++) {
          rval = c->put(*itk,*itv);
          if (rval) diewithcode("Put failed",rval);
      }

      itk = keys.begin();
      itv = vals.begin();
      for(; itk < keys.end(); itk++, itv++) {

        int retry = 1;
        for(retry = 1; retry > 0 && retry < 2048; retry++) {
          rval = c->contains(*itk);
          if (0 > rval) diewithcode("Contains failed",rval);
          if (1 == rval) retry = -retry;
        }
        if (-retry > 0)
          printf("Warning: %d retries for contains expecting contains=1 key=%zu\n",
                 -retry, key_read_offset);

        if (retry >= 2048) die("Contains test timed out [1]!");

      }

      itk = keys.begin();
      itv = vals.begin();
      for(; itk < keys.end(); itk++, itv++) {
        rval = c->remove(*itk);
        if (rval) die("Remove failed");
      }

      itk = keys.begin();
      itv = vals.begin();
      for(; itk < keys.end(); itk++, itv++) {

        int retry = 1;
        for(retry = 1; retry > 0 && retry < 2048; retry++) {
          rval = c->contains(*itk);
          if (0 > rval) diewithcode("Contains failed",rval);
          if (0 == rval) retry = -retry;
        }
        if (-retry > 0)
          printf("Warning: %d retries for contains expecting contains=1 key=%zu \n",
                 -retry, key_read_offset);

        if (retry >= 2048) die("Contains test timed out [0]!");
      }

    }

    // Display final results
    int pid = getpid();

    if (!thru)
      gettimeofday(&end_time, NULL);

    long long totaltime = (long long) (end_time.tv_sec - start_time.tv_sec) * 1000000
                          + (end_time.tv_usec - start_time.tv_usec);
    if (thru) {
      printf("%f\n",1000000.f*(((double)thru_count)/(double)totaltime));
    } else {
      printf("%f\n",((double)totaltime/(double)data_xchgs));
    }

    // Tear down the client and exit
    c->print_stats();
    c->teardown();
	printf("COMPLETE -<>-\n");
    exit(0);

  }

}

void usage(const char *argv0)
{
  fprintf(stderr, "usage: %s <server|s> [<listen_port>]\n",argv0);
  fprintf(stderr, "usage: %s <client|c> <config_file> [<test_type>]\n",argv0);
  fprintf(stderr,"  <config_file>: lines of \"ip port\"\n");
  fprintf(stderr,"  <test_type>: 'g' (get) or 'p' (put) [optional]\n");
  exit(1);
}

void sighandler(int)
{
  fprintf(stderr,"Received orderly shutdown request\n");
  //if (s) s->teardown();
  exit(0);
}

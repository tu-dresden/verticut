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
 *   ib-test.cc: Driver for benchmarking class *
 *               that tests out performance of *
 *               the underlying Infiniband     *
 *               connection class.             *
 ***********************************************/

#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/time.h>
#include <time.h>
#include <google/profiler.h>
#include <signal.h>
#include <getopt.h>

#include "ib.h"
#include "ibman.h"
#include "config.h"

#include "ib-client.h"
#include "ib-server.h"

#include "time/get_clock.h"

#define uint64_t unsigned long long int

const int STRESS_COUNT = 200000;
const int PWRS = 18;			// max powers of 2 to test

void usage(const char *argv0);
void sighandler(int);


// Used for qsort()
int ulong_compare(const void *aptr, const void *bptr) {
  unsigned long a = *(unsigned long *)aptr;
  unsigned long b = *(unsigned long *)bptr;
  return (a-b);
}

long timediff(cycles_t *now, cycles_t *then) {
  return *now-*then;
}

// Arguments
static struct option long_options[] = {
  {"client",  required_argument, 0,  'c' },
  {"server",  no_argument,       0,  's' },
  {"msglen",  required_argument, 0,  'm' },
  {"all",     no_argument,       0,  'a' },
  {"rdma",    no_argument,       0,  'r' },
  {"1verb",   no_argument,       0,  '1' },
  {"2verb",   no_argument,       0,  '2' },
  {"test",    required_argument, 0,  't' },
  {0,         0,                 0,  0   }
};

int main(int argc, char **argv) {

  // Test Parameters
  char test='a';   // 'a'll, 'r'dma, '1'-Way Verb, or '2'-Way Verb
  char metric='l'; // 'l'atency or 't'hroughput
  int fixedsize = -1;
  char* configpath = NULL;
  int role = R_SERVER;

  int c;
  while (1) {
    int this_option_optind = optind ? optind : 1;
    int option_index = 0;

    c = getopt_long(argc, argv, "c:sm:ar12t:",
                    long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
      case 0: 
        fprintf(stderr,"get_opt bug?\n");
        break;

      case 'a':
      case '1':
      case '2':
      case 'r':
        test = c;
        break;

      case 'm':
        fixedsize = atoi(optarg);  // fixed msg size
        break;

      case 't':
        metric = (optarg[0] == 'l')?'l':'t'; //latency or throughput
        break;

      case 'c':
        role = R_CLIENT;
        configpath = optarg;
        break;

      case 's':
        role = R_SERVER;
        break;

      default:
        printf("Invalid argument (%c)\n",c);
      case '?':
        usage(argv[0]);
        break;

    }
  }

  if (role == R_SERVER) {

    // Set up server
    IBServer* s = new IBServer;
//  s->verbosity(VERB_DEBUG);
    s->setup();

    // Start the server running
    signal(SIGUSR1, sighandler);
    if (argc == 2) {
      s->ready();
    } else {
      s->ready(atoi(argv[2]));
    }

  } else {

    // Test-specific vars
    cycles_t start_time, end_time;
    long long totaltime;
    unsigned int iters;
    double mintime; 
    double maxtime;

    // Create new client
    IBClient* c = new IBClient;
//  c->verbosity(VERB_DEBUG);

	if (c->setup())
      die("Failed to set up client");

    ConfigReader config(configpath);

    // if instead of while: get only a single server.
    if(!config.get_end()) {
      struct server_info* this_server = config.get_next();
      if (c->add_server(this_server->host->c_str(),this_server->port->c_str()))
        die("Failed to add server");
    }

    c->ready();

    // Set starting time and begin testing
    printf("Setup is complete; beginning test\n");
      
    unsigned long int* times;
    times = (unsigned long int*)malloc(STRESS_COUNT*sizeof(unsigned long int));
    if (NULL == times) {
      die("Failed to reserve array for times");
    }
  
    // Test RDMA latency
    if (test == 'a' || test == 'r') {
      printf("------RDMA TESTS-----------------------------------------\n");
      printf("           \tmin time [us]\tavg time [us]\tmax time [us]\n");
      for(int i=0; i<PWRS; i++) {
  
        // Set up variables
        iters = 0;
        totaltime = 0;
        mintime = 1.e20f;
        maxtime = 0.f;

		int thissize = (fixedsize == -1)?(1<<i):fixedsize;

        if (metric == 't') {
          start_time = get_cycles();
        }

        // Run the test iteration
        for(int j=0; j<STRESS_COUNT; j++) {
          if (metric == 'l') {
            start_time = get_cycles();
          }

          if (c->rdma_fetch(thissize))
            die("RDMA fetch failed");

          if (metric == 'l') {
            end_time = get_cycles();
  
            long long deltatime = timediff(&end_time,&start_time);
            totaltime += deltatime;
            times[j] = deltatime;

            //if (deltatime < mintime) mintime = deltatime;
            //if (deltatime > maxtime) maxtime = deltatime;
          }
          iters++;
        }

        if (metric == 't') {
          end_time = get_cycles();

/*
          double avg = (double)totaltime/(double)iters;
          long long devsum = 0;
          for(int j=0; j<STRESS_COUNT; j++) {
            long long val = times[j];
            devsum += (val-avg)*(val-avg);
          }
          double dev = sqrt((double)devsum/(double)STRESS_COUNT);
*/
          printf("%2d %9dB\t%8.3f\n",i,thissize,(double)iters/((double)timediff(&end_time,&start_time)/1000000.));

        } else {
		  qsort(times, STRESS_COUNT, sizeof(unsigned long), ulong_compare);
          double avg = (double)totaltime/(double)iters;
		  printf("LAT\t%d\t%lu\t%lu\t%lu\t%lu\t%f\t%lu\t%lu\t%lu\n",thissize,
                 times[0], times[(STRESS_COUNT)/100], times[(STRESS_COUNT)/10], times[STRESS_COUNT/2], avg,
                 times[(STRESS_COUNT*9)/10],times[(STRESS_COUNT*99)/100], times[STRESS_COUNT-1]);
        }
      }
    }

    // Test 1-way Verb msg latency
    if (test == 'a' || test == '1') {
      printf("------VERB TESTS-[1-WAY]---------------------------------\n");
      printf("           \tmin time [us]\tavg time [us]\tmax time [us]\n");
      for(int i=0; i<PWRS; i++) {
  
        // Set up variables
        iters = 0;
        totaltime = 0;
        mintime = 1.e20f;
        maxtime = 0.f;
  
		int thissize = (fixedsize == -1)?(1<<i):fixedsize;

        if (metric == 't') {
          start_time = get_cycles();
        }

        // Run the test iteration
        for(int j=0; j<STRESS_COUNT; j++) {
          if (metric == 'l') {
            start_time = get_cycles();
          }

          if (c->ib_ping(thissize))
            die("1-Way ping failed");

          if (metric == 'l') {
            end_time = get_cycles();
  
            long long deltatime = timediff(&end_time,&start_time);
            totaltime += deltatime;
            times[j] = deltatime;

            if (deltatime < mintime) mintime = deltatime;
            if (deltatime > maxtime) maxtime = deltatime;
          }
          iters++;
        }

        if (metric == 't') {
          end_time = get_cycles();

/*
          double avg = (double)totaltime/(double)iters;
          long long devsum = 0;
          for(int j=0; j<STRESS_COUNT; j++) {
            long long val = times[j];
            devsum += (val-avg)*(val-avg);
          }
          double dev = sqrt((double)devsum/(double)STRESS_COUNT);
*/
          printf("%2d %9dB\t%8.3f\n",i,thissize,(double)iters/((double)timediff(&end_time,&start_time)/1000000.));

        } else {
		  qsort(times, STRESS_COUNT, sizeof(unsigned long), ulong_compare);
          double avg = (double)totaltime/(double)iters;
		  printf("LAT\t%d\t%lu\t%lu\t%lu\t%lu\t%f\t%lu\t%lu\t%lu\n",thissize,
                 times[0], times[(STRESS_COUNT)/100], times[(STRESS_COUNT)/10], times[STRESS_COUNT/2], avg,
                 times[(STRESS_COUNT*9)/10],times[(STRESS_COUNT*99)/100], times[STRESS_COUNT-1]);
        }
      }
    }

    // Test 2-way Verb msg latency
    if (test == 'a' || test == '2') {
      printf("------VERB TESTS-[2-WAY]---------------------------------\n");
      printf("           \tmin time [us]\tavg time [us]\tmax time [us]\n");
      for(int i=0; i<PWRS; i++) {
  
        // Set up variables
        iters = 0;
        totaltime = 0;
        mintime = 1.e20f;
        maxtime = 0.f;
  
		int thissize = (fixedsize == -1)?i:fixedsize; // was 1<<i for the true case

        if (metric == 't') {
          start_time = get_cycles();
        }

        // Run the test iteration
        for(int j=0; j<STRESS_COUNT; j++) {
          if (metric == 'l') {
            start_time = get_cycles();
          }

          if (c->ib_pingpong(thissize,false))	//now uses pwrs
            die("2-Way pingpong failed");

          if (metric == 'l') {
            end_time = get_cycles();
  
            long long deltatime = timediff(&end_time,&start_time);
            totaltime += deltatime;
            times[j] = deltatime;

            if (deltatime < mintime) mintime = deltatime;
            if (deltatime > maxtime) maxtime = deltatime;
          }
          iters++;
        }

        if (metric == 't') {
          end_time = get_cycles();

/*
          double avg = (double)totaltime/(double)iters;
          long long devsum = 0;
          for(int j=0; j<STRESS_COUNT; j++) {
            long long val = times[j];
            devsum += (val-avg)*(val-avg);
          }
          double dev = sqrt((double)devsum/(double)STRESS_COUNT);
*/
          printf("%2d %9dB\t%8.3f\n",i,thissize,(double)iters/((double)timediff(&end_time,&start_time)/1000000.));

        } else {
		  qsort(times, STRESS_COUNT, sizeof(unsigned long), ulong_compare);
          double avg = (double)totaltime/(double)iters;
		  printf("LAT\t%d\t%lu\t%lu\t%lu\t%lu\t%f\t%lu\t%lu\t%lu\n",thissize,
                 times[0], times[(STRESS_COUNT)/100], times[(STRESS_COUNT)/10], times[STRESS_COUNT/2], avg,
                 times[(STRESS_COUNT*9)/10],times[(STRESS_COUNT*99)/100], times[STRESS_COUNT-1]);
        }
      }
    }

    // Tear down the client and exit
    c->teardown();
    exit(0);

  }

}

void usage(const char *argv0) {
  fprintf(stderr, "usage: %s [server|s]\n",argv0);
  fprintf(stderr, "usage: %s [client|c] <config_file> [a|r|1|2] [fixed size]\n",argv0);
  fprintf(stderr,"  <config_file>: lines of \"ip port\"\n");
  fprintf(stderr,"  [a|r|1|2]: (a)ll, (R)DMA only, (1)-way Verb only, (2)-way Verb only\n");
  fprintf(stderr,"  [fixed size]: All messages will be this size.\n");
  exit(1);
}

void sighandler(int)
{
  fprintf(stderr,"Received orderly shutdown request\n");
  //if (s) s->teardown();
  exit(0);
}


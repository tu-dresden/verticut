/* multi-client-echo-server.c - a multi-client echo server */
/* Copyrights - Guy Keren 1999 (c)			   */

#include <stdio.h>		/* Basic I/O routines		*/
#include <stdlib.h>
#include <sys/types.h>		/* standard system types	*/
#include <netinet/in.h>		/* Internet address structures	*/
#include <sys/socket.h>		/* socket interface functions	*/
#include <linux/tcp.h>
#include <netdb.h>		/* host to IP resolution	*/
#include <sys/time.h>		/* for timeout values		*/
#include <unistd.h>		/* for table size calculations	*/
#include <cstring>
#include <signal.h>
#include <assert.h>
#include <time.h>

#include "helper.h"

#define	BUFLEN		262144	/* buffer length	   */
#define LATITEMS_BASE   2097152
#define INIT_COMPLETE_SIZE 256

static long timediff(struct timespec *now, struct timespec *then)
{
  return (now->tv_sec-then->tv_sec)*1000000+(now->tv_nsec-then->tv_nsec)/1000;
}

static int long_compare(const void *aptr, const void *bptr) {
	long a = *(long *)aptr;
	long b = *(long *)bptr;
	return (a-b);
}

int ParseCmdLine(int argc, char *argv[], int *szPort, size_t *msgSize, size_t *respSize, int *latRec);

struct timeval start_time, end_time;
size_t msgs = 0;
size_t msgSize = 0;
size_t respSize = 0;

void* sigUSR1(int signal) {
	gettimeofday(&end_time,NULL);
		
	long long totaltime = (long long) (end_time.tv_sec - start_time.tv_sec) * 1000000
							        + (end_time.tv_usec - start_time.tv_usec);
	FILE* fh = fopen("echo_thru.log","a");
	fprintf(fh,"%zu %ld %zu\n",msgSize,totaltime,msgs);
	fclose(fh);

	start_time = end_time;
	msgs = 0;

	return NULL;
}

int main(int argc, char* argv[])
{
	int			i;		/* index counter for loop operations */
	int			rc;			/* system calls return value storage */
	int			s;		/* socket descriptor */
	int			cs;			/* new connection's socket descriptor */
	char		buf[BUFLEN+1];  /* buffer for incoming data */
	struct sockaddr_in	sa;			/* Internet address struct */
	struct sockaddr_in	csa;		/* client's address struct */
	int				size_csa;	/* size of client's address struct */
	fd_set		rfd;		/* set of open sockets */
	fd_set		c_rfd;		/* set of sockets waiting to be read */
	int			dsize;		/* size of file descriptors table */
	int         szPort;

	int			latRec = 0;			// Record latency numbers?
	long*		latArr = NULL;		// Array of latencies
	size_t		latOff = 0;
	struct timespec start_time_lat, end_time_lat;
	long LATITEMS;

    ParseCmdLine(argc, argv, &szPort, &msgSize, &respSize, &latRec);

	size_t* completions = NULL;
	int completions_size = 0;

	if (NULL == (completions = (size_t*)malloc(INIT_COMPLETE_SIZE*sizeof(size_t)))) {
		perror("Could not reserve space for copletions array");
	}
	completions_size = INIT_COMPLETE_SIZE;
	memset(completions,0,INIT_COMPLETE_SIZE);

	if (latRec) {
		LATITEMS = (LATITEMS_BASE/(msgSize/8));	//don't make tests run forever.

		latArr = (long*)malloc(sizeof(long)*LATITEMS);
		if (latArr == NULL) {
			printf("No memory for latency array\n");
		}
		
	}

	// Set up address
	memset(&sa, 0, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_port = htons(szPort);
	sa.sin_addr.s_addr = INADDR_ANY;

	printf("Listening on port %d for %zu-byte messages\n",szPort,msgSize);
	// Allocate a free socket
	s = socket(AF_INET, SOCK_STREAM, 0);
	if (s < 0) {
		perror("socket: allocation failed");
	}
	int flag = 1;
	int result = setsockopt(s,               /* socket affected */
							IPPROTO_TCP,     /* set option at TCP level */
							TCP_NODELAY,     /* name of option */
							(char *) &flag,  /* the cast is historical cruft */
							sizeof(int));    /* length of option value */
	if (result < 0) {
		perror("setsockopt: couldn't disable Nagel's algorithm.");
	}

	/* bind the socket to the newly formed address */
	rc = bind(s, (struct sockaddr *)&sa, sizeof(sa));

	/* check there was no error */
	if (rc) {
		perror("bind");
	}

	/* ask the system to listen for incoming connections	*/
	/* to the address we just bound. specify that up to		*/
	/* 5 pending connection requests will be queued by the	*/
	/* system, if we are not directly awaiting them using	*/
	/* the accept() system call, when they arrive.		*/
	rc = listen(s, 5);

	/* check there was no error */
	if (rc) {
		perror("listen");
	}

	/* remember size for later usage */
	size_csa = sizeof(csa);

	/* calculate size of file descriptors table */
	//dsize = getdtablesize();
	dsize = s+1;

	/* close all file descriptors, except our communication socket	*/
	/* this is done to avoid blocking on tty operations and such.	*/
	for (i=0; i<getdtablesize(); i++)
		if (i != s && i != 0)
			close(i);

	/* we innitialy have only one socket open,	*/
	/* to receive new incoming connections.	*/
	FD_ZERO(&rfd);
	FD_SET(s, &rfd);

	/* enter an accept-write-close infinite loop */
	gettimeofday(&start_time,NULL);
	signal(SIGUSR1,sigUSR1);
	while (1) {
		/* the select() system call waits until any of	*/
		/* the file descriptors specified in the read,	*/
		/* write and exception sets given to it, is	*/
		/* ready to give data, send data, or is in an	*/
		/* exceptional state, in respect. the call will	*/
		/* wait for a given time before returning. in	*/
		/* this case, the value is NULL, so it will	*/
		/* not timeout. dsize specifies the size of the	*/
		/* file descriptor table.			*/
		c_rfd = rfd;
		rc = select(dsize, &c_rfd, NULL, NULL, (struct timeval *)NULL);

		/* if the 's' socket is ready for reading, it	*/
		/* means that a new connection request arrived.	*/
		if (FD_ISSET(s, &c_rfd)) {
			/* accept the incoming connection */
			cs = accept(s, (struct sockaddr *)&csa, &size_csa);

			/* check for errors. if any, ignore new connection */
			if (cs < 0)
				continue;

			/* add the new socket to the set of open sockets */
			dsize = (dsize > cs+1)?dsize:cs+1;
			if (cs > completions_size) {
				perror("Ran out of completion space!");
			}
			completions[cs] = 0;
			FD_SET(cs, &rfd);

			/* and loop again */
			continue;
		}

		/* check which sockets are ready for reading,	*/
		/* and handle them with care.			*/
		for (i=0; i<dsize; i++) {
			if (i != s && FD_ISSET(i, &c_rfd)) {
				/* read from the socket */
				rc = read(i, buf, msgSize-completions[i]);

				/* if client closed the connection... */
				if (rc <= 0) {
					/* close the socket */
					close(i);
					FD_CLR(i, &rfd);
				}	
				/* if there was data to read */
				else {
					completions[i] += rc;

					if (completions[i] == msgSize) {
						completions[i] = 0;
						if (latRec) {
							clock_gettime(CLOCK_REALTIME, &end_time_lat);
							if (latOff > 0) {
								latArr[latOff-1] = timediff(&end_time_lat,&start_time_lat);
							}
							latOff++;
							if (latOff > LATITEMS) {
								qsort(latArr, LATITEMS, sizeof(long), long_compare);
								FILE* fh = fopen("echo_lat.log","w");
								fprintf(fh,"%lu\t%lu\t%lu\t%lu\t%lu\t%lu\t%lu\n", latArr[0], latArr[LATITEMS/100],
								        latArr[LATITEMS/10],latArr[LATITEMS/2], 
								        latArr[(LATITEMS*9)/10], latArr[(LATITEMS*99)/100], latArr[LATITEMS-1]);
								fclose(fh);
								exit(0);
							}
							start_time_lat = end_time_lat;
						}
	
						/* echo it back to the client */
						rc = write(i, buf, respSize);
						if (rc <= 0) {
							/* close the socket */
							close(i);
							FD_CLR(i, &rfd);
						}	
						assert(rc == respSize);
						msgs++;
					}
				}
			}
		}
	}
}

int ParseCmdLine(int argc, char *argv[], int *szPort, size_t *msgSize, size_t *respSize, int *latRec) {

    int n = 1;

    while ( n < argc ) {
	if ( !strncmp(argv[n], "-p", 2) || !strncmp(argv[n], "-P", 2) ) {
	    *szPort = atoi(argv[++n]);
	}
	else if ( !strncmp(argv[n], "-h", 2) || !strncmp(argv[n], "-H", 2) ) {
	    printf("Usage:\n\n");
	    printf("    %s -p (local port) -s (message size) -r (response size) [-l]\n\n",argv[0]);
	    exit(0);
	}
	else if ( !strncmp(argv[n], "-s", 2) || !strncmp(argv[n], "-S", 2) ) {
		*msgSize = atoi(argv[++n]);
	}
	else if ( !strncmp(argv[n], "-r", 2) || !strncmp(argv[n], "-R", 2) ) {
		*respSize = atoi(argv[++n]);
	} else if ( !strncmp(argv[n], "-l", 2) || !strncmp(argv[n], "-L", 2) ) {
		*latRec = 1;
	}
	++n;
    }

    return 0;
}

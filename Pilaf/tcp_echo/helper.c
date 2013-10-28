/*

  HELPER.C
  ========
  (c) Paul Griffiths, 1999
  Email: mail@paulgriffiths.net

  Implementation of sockets helper functions.

  Many of these functions are adapted from, inspired by, or 
  otherwise shamelessly plagiarised from "Unix Network 
  Programming", W Richard Stevens (Prentice Hall).

  Re-written by Christopher Mitchell (c) 2013.

*/

#include "helper.h"
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>


/*  Read a line from a socket  */

size_t Readmsg(int sockd, char *vptr, size_t len) {
    ssize_t n=0, rc;

    while (n < len) {
		if ( (rc = read(sockd, vptr, len-n)) >= 0 ) {
			n+=rc;
			vptr+=rc;
		} else return -1;
	}
    return len;
}


/*  Write a line to a socket  */

size_t Writemsg(int sockd, const char *vptr, size_t len) {
    ssize_t n=0, rc;

    while (n < len) {
		if ( (rc = write(sockd, vptr, len-n)) >= 0 ) {
			n+=rc;
			vptr+=rc;
		} else return -1;
	}
    return len;
}


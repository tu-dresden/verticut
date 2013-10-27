#ifndef IBMAN_H
#define IBMAN_H

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <stdarg.h>
#include "ib.h"
#include <sys/time.h>
#include <fcntl.h>

enum ibman_verb {
    VERB_VITAL = -1,
	VERB_ERROR = 0,
	VERB_WARN,
	VERB_INFO,
	VERB_DEBUG
};

class IBConnManager {
private:
  // Global memory regions, common to all incoming connections
  struct mr_chain_node* global_mrs;
  int n_global_mrs;
  int s_role;
  enum ibman_verb verb_;

  struct ibv_pd* gpd; //shared protection domain
  struct ibv_cq* gcq; //shared completion queue
  struct rdma_event_channel* gec; //shared event channel

  int (*on_event_hook)(struct rdma_cm_event*, void*, void*);
  void* event_hook_context;

  // Make the manager thread-safe
  struct ibv_wc wc;

public:
  IBConnManager(int role);
  void verbosity(enum ibman_verb verb);
  void log(enum ibman_verb loglevel, char* fmt, ...);

  // Local/Global MR commands
  struct ibv_mr* create_mr(void* addr, size_t length, int flags, enum mr_scope scope, IBConn* IBC);
  int set_mr(enum mr_location location, enum mr_scope scope, struct ibv_mr* mr, int mr_id, IBConn* IBC);
  int unset_mr(enum mr_scope scope, int mr_id, IBConn* IBC);
  struct ibv_mr* fetch_mr(enum mr_location location, int mr_id, IBConn* IBC);
  void destroy_global_mrs(void);

  int poll_cq(int do_not_terminate);
  void set_event_hook(int(*event_hook)(struct rdma_cm_event *event, void* ec_context, void* event_context), void* ec_context);

  // Grab a new connection
  IBConn* new_conn(size_t msgbuf_size);
  ibv_pd* get_pd(void) { return gpd; }
  struct rdma_event_channel* get_ec() { return gec; }
  void hint_client_count(size_t n_clients);

  friend class IBConn;

};

#endif //IBMAN_H

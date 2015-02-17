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
 *   ibman.cc: Re-usable Infiniband connection *
 *             manager class for distributed   *
 *             storage systems.                *
 ***********************************************/

#include "ibman.h"

IBConnManager::IBConnManager(int role) {
 gpd = NULL;
 gcq = NULL;
 gec = NULL;
 global_mrs = NULL;
 n_global_mrs = 0;
 s_role = role;
 verb_ = VERB_ERROR;

 // Create shared event channel
 if (NULL == (gec = rdma_create_event_channel())) {
   log(VERB_ERROR,"Failed to create global event channel\n"); 
   die("");
 }

 // Change the blocking mode of the event channel
 int flags = fcntl(gec->fd, F_GETFL);
 int rc = fcntl(gec->fd, F_SETFL, flags | O_NONBLOCK);
 if (rc < 0) {
   log(VERB_ERROR,"Failed to change file descriptor of event channel\n");
   die("");
 }

}

struct ibv_mr* IBConnManager::create_mr(void* addr, size_t length, int flags, enum mr_scope scope, IBConn* IBC) {
  struct ibv_mr* rval;
  if (NULL == (rval = ibv_reg_mr(
    /*(scope == MR_SCOPE_GLOBAL?*/IBC->s_conn->id->qp->pd/*global_pd*/ /*:IBC->s_ctx->pd)*/, 
    addr,
    length,
    flags)))
  {
    log(VERB_ERROR,"Failed to register memory region at %p, size %zu, flags %d\n",addr,length,flags);
    diewithcode("Failed with code",errno);
  }
  return rval;
}

int IBConnManager::poll_cq(int do_not_terminate) {
  struct rdma_cm_event *event = NULL;

  do {

    if (gcq) {
      // If we get a completion queue event, deal with it.
      // Unless we're disconnecting, in which case failures are expected.
      while (ibv_poll_cq(gcq, 1, &wc))
        ((IBConn*)((struct recv_buf*)wc.wr_id)->instance)->on_completion(&wc);
    }

    if (gec) {
      // Poll event channel
      int rgce_rval;
      if ((rgce_rval = rdma_get_cm_event(gec, &event)) != 0) {
        if (errno != EAGAIN)
         return errno;
      } else {

        //events are waiting
        struct rdma_cm_event event_copy;

        memcpy(&event_copy, event, sizeof(*event));
        rdma_ack_cm_event(event);

        if (on_event_hook(&event_copy,event_hook_context,event_copy.id->context))
          return 1;
      }
    }

  } while(do_not_terminate);

  return 0;
}

int IBConnManager::set_mr(enum mr_location location, enum mr_scope scope, struct ibv_mr* mr, int mr_id, IBConn* IBC) {
  struct mr_chain_node* node = NULL;
  struct mr_chain_node** prev = NULL;

  if (scope == MR_SCOPE_GLOBAL) {
    node = global_mrs;
    prev = &global_mrs;
  } else if (scope == MR_SCOPE_LOCAL && IBC != NULL) {
    node = IBC->s_conn->local_mrs;
    prev = &(IBC->s_conn->local_mrs);
  } else
    diewithcode("set_mr has invalid scope type",scope);

  log(VERB_DEBUG,"Setting MR id=%d, loc=%s, scope=%s, len %zu @ %p\n",mr_id,
      (location==MR_LOC_LOCAL)?"local":"remote",
      (scope==MR_SCOPE_LOCAL)?"local":"global",
      mr->length,mr->addr);

  while (node != NULL) {
    if (node->mr_id == mr_id) {
      if (node->mr == NULL)
        die("BUG: node->mr is NULL in set_mr");
      memcpy(node->mr,mr,sizeof(struct ibv_mr));
      return 1;
    }
    prev = &(node->next);
    node = node->next;
  }

  *prev = node = (struct mr_chain_node*)malloc(sizeof(struct mr_chain_node));
  if (node == NULL)
    die("Memory exhausted reserving chain_node in set_mr");

  node->mr_id = mr_id;
  node->next = NULL;
  node->scope = scope;
  node->location = location;
  node->mr = (struct ibv_mr*)malloc(sizeof(struct ibv_mr));
  if (node->mr == NULL)
    die("Memory exhausted reserving ibv_mr for chain_node in set_mr");

  memcpy(node->mr,mr,sizeof(struct ibv_mr));
  return 0;
}

// Remove the MR with the given ID from the global or local linked list
// and, if it's a local MR, deregister it.
int IBConnManager::unset_mr(enum mr_scope scope, int mr_id, IBConn* IBC) {
  struct mr_chain_node* node = NULL;

  if (scope == MR_SCOPE_GLOBAL)
    node = global_mrs;
  else if (scope == MR_SCOPE_LOCAL && IBC != NULL)
    node = IBC->s_conn->local_mrs;
  else
    diewithcode("set_mr has invalid scope type",scope);

  struct mr_chain_node* prev = NULL;

  while (node != NULL) {
    if (node->mr_id == mr_id) {
      if (node->location == MR_LOC_LOCAL) {
        ibv_dereg_mr(node->mr);
      }
      free(node->mr);
      struct mr_chain_node* next = node->next;
      if (next != NULL) {
        if (prev == NULL) {
          if (scope == MR_SCOPE_GLOBAL)
            IBC->s_conn->local_mrs = next;
          else
            global_mrs = next;
        } else
          prev->next = next;
      }
      return 0;
    }
    prev = node;
    node = node->next;
  }
  return -1;
}

// Find the MR with the given mr_id, and return its stats
struct ibv_mr* IBConnManager::fetch_mr(enum mr_location location, int mr_id, IBConn* IBC) {

  struct mr_chain_node* node = global_mrs;
  while (node != NULL) {
    if (node->mr_id == mr_id) {
      return node->mr;
    }
    node = node->next;
  }

  if (IBC == NULL)
    return NULL;

  node = IBC->s_conn->local_mrs;
  while (node != NULL) {
    if (node->mr_id == mr_id) {
      return node->mr;
    }
    node = node->next;
  }

  return NULL;
}

void IBConnManager::destroy_global_mrs(void) {
  for(struct mr_chain_node* node = global_mrs; node != NULL; ) {
    struct mr_chain_node* next = node->next;
    if (node->location == MR_LOC_LOCAL) {
      //free(node->mr->addr);
      ibv_dereg_mr(node->mr);
    }
    free(node);
    node = next;
  }
  global_mrs = NULL;
  gpd = nullptr;
}

IBConn* IBConnManager::new_conn(size_t msgbuf_size) {
  IBConn* conn = new IBConn(this, msgbuf_size);
  conn->s_role = s_role;
  return conn;
}

void IBConnManager::verbosity(enum ibman_verb verb) {
  verb_ = verb;
}

void IBConnManager::log(enum ibman_verb loglevel, char* fmt, ...) {
  if (loglevel <= verb_ || loglevel == VERB_VITAL) {
    struct timeval now;
    gettimeofday(&now, NULL);

    va_list argptr;
    va_start(argptr,fmt);

    fprintf(stderr,"[%5d | %03ld.%06ld ] ",getpid(),now.tv_sec%1000,now.tv_usec);
    vfprintf(stderr,fmt,argptr);
    va_end(argptr);
  }
}

void IBConnManager::set_event_hook(int(*event_hook)(struct rdma_cm_event *event, void* ec_context,
                                   void* event_context), void* ec_context)
{
  on_event_hook = event_hook;
  event_hook_context = ec_context;
}


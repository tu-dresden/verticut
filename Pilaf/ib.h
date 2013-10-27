#ifndef IB_H
#define IB_H

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <vector>
#include <queue>

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

#define MAX(x,y) ((x)>(y)?(x):(y))

// Reference-counting macros
#define IBC_INCREF() (this->refcount++);
#define IBC_DECREF() (this->refcount=((this->refcount)==0?(die("Refcount < 0!"),0):(refcount-1)));

// Constants
const int TIMEOUT_IN_MS = 500; /* ms */
const int RECV_BUF_SIZE = 2048; /* 2 KB */
const int BLOCK_READ_COUNT = 1;
const int RDMA_BUFFER_SIZE = 1024;
const int CQ_ACK_THRESH = 16;
const int RECV_BUFS = 8;
const int MAX_INLINE_SEND = 400; //bytes

#pragma pack(push)
#pragma pack(4)
struct message {
  int type;
  char mdata;  // first byte of user message contents; dummy
};
#pragma pack(pop)

// Enums
enum conn_state {
  CONN_DISCO,
  CONN_SETUP,
  CONN_READY,
  CONN_TEARD,
  CONN_FINIS
};

enum msg_type {
  MSG_MR,
  MSG_DONE,

  MSG_USER_FIRST
};

enum SS {
  SS_INIT,
  SS_MR_SENDING,
  SS_MR_SENT,
  SS_RDMA_SENT,
  SS_DONE_SENT
};

enum RS {
  RS_INIT,
  RS_MR_RECV,
  RS_DONE_RECV
};

enum role {
  R_SERVER,
  R_CLIENT
};

enum mr_location {
  MR_LOC_LOCAL,
  MR_LOC_REMOTE
};

enum mr_scope {
  MR_SCOPE_LOCAL,
  MR_SCOPE_GLOBAL
};

//Structs Part 2
struct mrmessage {
  int type;

  int mr_id;
  enum mr_scope scope;
  enum mr_location location;
  struct ibv_mr mr;
  bool last;
};

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;
};

struct mr_chain_node {
  int mr_id;						//application-defined
  struct ibv_mr *mr;				//actual mr structure
  enum mr_location location;
  enum mr_scope scope;
  struct mr_chain_node* next;	//pointer to next node in list, or null
};

struct recv_buf {
  struct message* msg;
  struct ibv_mr* mr;
  void* instance;
};

struct connection {

  struct rdma_cm_id *id;
  struct ibv_qp *qp;

  int connected;
  int disconnecting;

  // Local memory regions, specific to this connection
  struct mr_chain_node* local_mrs;    // stores connection-specific MRs
  struct mr_chain_node* sending_mrs;  // holds MRs being current transmitted; only used during setup
  int n_local_mrs;

  struct message *send_msg;

  struct ibv_mr *send_mr; 

  int send_state;
  int recv_state;

  unsigned int send_nonce;
  unsigned int recv_nonce;

  bool post_recv_pending;
  int cq_ack_pending;

  // These three used to be static structs, pulled out to make things thread-safe
  struct ibv_send_wr ts_wr_rf;		// for rdma_fetch
  struct ibv_sge ts_sge_rf;			// for rdma_fetch
};

void die(const char *reason);
void diewithcode(const char *reason, int code);
const char* role_to_str(int s_role);

class IBConnManager;

class IBConn {
private:
  int build_context(struct rdma_cm_id* id);
  int build_qp_attr(struct ibv_qp_init_attr *qp_attr);
  char* get_peer_message_region(struct connection *conn);
  void on_completion(struct ibv_wc *);
  void post_receives(struct connection *conn);
  void register_memory(struct connection *conn);
  void send_message(struct connection *conn, int msg_size);
  int  send_head_mr();

  size_t send_buf_size;
  size_t recv_buf_size;

  // Receive buffers
  std::vector<struct recv_buf*> recv_bufs;
  std::queue<struct recv_buf*> recv_pend_post;

  // Send & RDMA buffers
  struct recv_buf* send_buf;

  // Used for the callbacks
  void (*on_recv_hook)(int, struct message*, size_t, IBConn*, void*);
  void (*on_rdma_recv_hook)(int, IBConn*, void*);
  void (*on_ready_hook)(void*);
  void (*on_mr_hook)(int*, struct ibv_mr*, int*, void*);
  void (*on_send_complete_hook)(void*);

  // Contexts for the callbacks
  void* recv_hook_context;
  void* rdma_recv_hook_context;
  void* ready_hook_context;
  void* mr_hook_context;
  void* send_complete_hook_context;

  IBConnManager* manager_;

public:
  IBConn(IBConnManager* manager, size_t msgbuf_size);

  // Meta creation and destruction of connections
  int build_connection(struct rdma_cm_id *id);
  void build_params(struct rdma_conn_param *params);
  void destroy_connection();
  void destroy_context();

  void on_connect();
  void disconnect();
  static void reject(struct rdma_cm_id *id, const char* imm);
  void *get_local_message_region();
  int send_mr();

  // Setting app-specified hooks
  // The last void* arg to all these hooks is the context
  void set_recv_hook(void(*recv_hook)(int,struct message*,size_t,IBConn*,void*), void* recv_context);
  void set_rdma_recv_hook(void(*rdma_recv_hook)(int,/*void*,void*,*/IBConn*,void*), void* rdma_recv_context);
  void set_ready_hook(void(*ready_hook)(/*void*,size_t,*/ void*), void* rdma_recv_context);
  void set_mr_hook(void(*mr_hook)(int* status, struct ibv_mr* mr, int* mr_id, void*), void* mr_context);
  void set_send_complete_hook(void(*send_complete_hook)(void*), void* send_complete_context);

  // External messages/commands
  void send_message_ext(int type, char* data, size_t data_len);
  int rdma_fetch(uintptr_t addr, size_t length, struct ibv_mr* remote_mr, ibv_mr* local_mr);
  int rdma_push(uintptr_t addr, size_t length, uintptr_t local_addr, struct ibv_mr* remote_mr, ibv_mr* local_mr);

  message* get_send_buf(void) { return s_conn->send_msg; }
  int is_connected(void) { return (s_conn->connected); }

  // Connection and management
  struct connection *s_conn;
  struct context *s_ctx;
  bool built;
  unsigned int refcount;

  bool ready;
  int s_role;

  // Roles
  void set_role(enum role m);

  friend class IBConnManager;
};

#endif // IB_H

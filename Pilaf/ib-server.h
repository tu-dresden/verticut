#ifndef IB_SERVER_H
#define IB_SERVER_H

#include <vector>
#include <sys/mman.h>
#include "ib.h"
#include "ibman.h"

#include <fcntl.h>
#include <errno.h>

#define SERVER_PORT 36001

#define MAX_RDMA_BUF_SIZE (1<<29)
#define MAX_RDMA_BUF_RECV_SIZE (1<<20)
#define MSG_BUF_SIZE MAX_RDMA_BUF_RECV_SIZE

#define MR_TYPE_RDMA_POOL 13
#define RESP_SIZE 10  //To match Eth/IPoIB tester, bulk data only one way for 2-way transfers

class IBServer {
private:
  IBConnManager* manager;
  std::vector<IBConn*> clients;
  struct sockaddr_in addr;
  struct rdma_cm_id *listener;
  uint16_t port;
  bool is_ready;                              // set once ready for clients

  // Memory regions
  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;
  struct ibv_mr *recv_buf_mr;

  struct ibv_mr *rdma_local_mr;
  void* rdma_local_buf;

  void create_mrs(IBConn* conn);
  bool mr_init;

  // Asynchronous
  int do_event_loop(void);
  int on_connect_request(struct rdma_cm_id *id);
  int on_connection(struct rdma_cm_id *id);
  int on_disconnect(struct rdma_cm_id *id);
  static int on_event(struct rdma_cm_event *event, void* ec_context, void* event_context);

public:

  // Synchronous
  IBServer();
  void verbosity(enum ibman_verb verb);
  int setup(void);
  int ready(unsigned short port = SERVER_PORT);

  // Hooks
  static void hook_ibv_recv(int type, struct message* msg, size_t len, IBConn* conn, void* context);

};
#endif // IB_SERVER_H

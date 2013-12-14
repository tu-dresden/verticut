#ifndef IB_CLIENT_H
#define IB_CLIENT_H

#include <string>
#include <vector>
#include "ib.h"
#include "ibman.h"

#include <fcntl.h>
#include <errno.h>

#define MAX_RDMA_BUF_SIZE (1<<30)
#define MAX_RDMA_RECV_BUF_SIZE (1<<26)
#define MSG_BUF_SIZE MAX_RDMA_RECV_BUF_SIZE

#define MR_TYPE_RDMA_LADLE 37
#define MR_TYPE_RDMA_POOL 13
#define RESP_SIZE 10  //To match Eth/IPoIB tester, bulk data only one way for 2-way transfers

class IBClient;

struct ServerInfo {
  IBClient* client;     // loops self-referentially to the IBClient that contains this ServerInfo
  IBConn* connection;
  char* server_host;
  char* server_port;
  struct rdma_cm_event* event;
  struct rdma_cm_id* conn;
  bool ready;   // sort of like "connected", but also with MRs swapped
  unsigned int epoch;

  // Memory regions
  struct ibv_mr* rdma_fetch_buf_mr;
  struct ibv_mr* rdma_remote_buf_mr;

  // Flags and mem to connect to IB
  void* rdma_fetch_buf;

  struct message* ibv_recv_buf;
  bool rdma_msg_ready;
  bool ibv_msg_ready;

  bool mr_init;
  bool sent;

};

class IBClient {
private:
  IBConnManager* manager;
  struct ServerInfo server;
  unsigned int server_count;

  // Asynchronous
  int on_addr_resolved(struct ServerInfo* server, struct rdma_cm_id *id);
  int on_connection(struct ServerInfo* server, struct rdma_cm_id *id);
  int on_disconnect(struct ServerInfo* server, struct rdma_cm_id *id);
  int on_route_resolved(struct ServerInfo* server, struct rdma_cm_id *id);
  int on_reject(struct ServerInfo* server, const char* private_data);

  void create_mrs(struct ServerInfo* server);
public:

  // Synchronous
  IBClient();

  void verbosity(enum ibman_verb verb);
  int setup();
  int ready();
  int add_server(const char* server_host, const char* server_port);

  int rdma_fetch(size_t bytes);
  int ib_pingpong(size_t bytes, bool bigreply);
  int ib_ping(size_t bytes);

  int do_event_loop();
  void teardown();

  static void hook_ready(/*void* table, size_t entries,*/ void* context);
  static void hook_ibv_recv(int type, struct message* msg, size_t len, IBConn* conn, void* context);
  static void hook_rdma_recv(int type, /*void* entry, void* extents,*/ IBConn* conn, void* context);
  static void hook_send_complete(void* context);
  static int on_event(struct rdma_cm_event *event, void* ec_context, void* event_context);

};

#endif // IB_CLIENT_H

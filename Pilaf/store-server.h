#ifndef STORE_SERVER_H
#define STORE_SERVER_H

#include <vector>
#include "ib.h"
#include "ibman.h"
#include "dht.h"
#include "table_types.h"

#include <fcntl.h>
#include <errno.h>

#define SERVER_PORT 36001
#define LOG_BUF_SIZE (1<<20)
#define LOG_BUF_FLUSH (1<<18)

class Server {
private:
  IBConnManager* manager;
  std::vector<IBConn*> clients;
  bool resizing;
  struct sockaddr_in addr;
  struct rdma_cm_id *listener;
  uint16_t port;
  unsigned int epoch;
  bool is_ready;                              // set once ready for clients
  DHTClient<KEY_TYPE, VAL_TYPE>* dhtclient;    // used for server-mediated reads

  // Logging
  bool logging;
  char* log_fname;
  FILE* log_fh;
  char* log_buf;
  size_t log_offset;

  // Memory regions
  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;
  struct ibv_mr *recv_buf_mr;

  struct ibv_mr *dht_table_mr;
#if KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP
  struct ibv_mr *dht_ext_mr;
#endif

  void create_mrs(IBConn* conn);
  bool mr_init;

  // Asynchronous
  int do_event_loop(void);
  int on_connect_request(struct rdma_cm_id *id);
  int on_connection(struct rdma_cm_id *id);
  int on_disconnect(struct rdma_cm_id *id);
public:

  // Synchronous
  Server();
  void verbosity(enum ibman_verb verb);
  int setup(void);
  int ready(unsigned short port = SERVER_PORT);

  // Logging
  void set_logging(bool state, char* fname);
  void log_flush(void);

  // Hooks
  static int hook_pre_resize(size_t oldsize, std::vector<DHT<KEY_TYPE, VAL_TYPE>::dht_block>*, void* context);
  static int hook_post_resize(size_t newsize, std::vector<DHT<KEY_TYPE, VAL_TYPE>::dht_block>*, void* context);
  static void hook_ibv_recv(int type, struct message* msg, size_t len, IBConn* conn, void* context);
  static int on_event(struct rdma_cm_event *event, void* ec_context, void* event_context);

  // Data
  DHT<KEY_TYPE, VAL_TYPE> dht;

};
#endif // STORE_SERVER_H

#ifndef STORE_CLIENT_H
#define STORE_CLIENT_H

#include <string>
#include <vector>
#include "ib.h"
#include "ibman.h"
#include "dht.h"
#include "table_types.h"
#include "image_tools.h"
#include <fcntl.h>
#include <errno.h>
#include <sys/time.h>

#define MAX_BUF 10000000
enum read_modes {
  READ_MODE_RDMA,
  READ_MODE_SERVER
};

class Client;

enum conn_setup_state {
  CS_CREATED = 0,
  CS_ADR_RES,
  CS_RT_RES,
  CS_ESTAB
};

struct ServerInfo {
  Client* client;     // loops self-referentially to the Client that contains this ServerInfo
  IBConn* connection;
  char* server_host;
  char* server_port;
//  struct rdma_event_channel* ec;
  struct rdma_cm_event* event;
  struct rdma_cm_id* conn;
  bool ready;   // sort of like "connected", but also with MRs swapped
  enum conn_setup_state state;
  unsigned int epoch;

  // Memory regions
//  struct ibv_mr *recv_mr;
//  struct ibv_mr *send_mr;
//  struct ibv_mr *recv_buf_mr;
  struct ibv_mr *rdma_fetch_buf_mr;
  struct ibv_mr *rdma_fetch_ext_buf_mr;

  struct ibv_mr* dht_table_mr;
#if KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP
  struct ibv_mr* dht_ext_mr;
#endif

  // DHT-relevant items
  DHTClient<const KEY_TYPE,VAL_TYPE>* dhtclient;
  void* table;
  size_t entries;

  // Flags and mem to connect to IB
  void* rdma_fetch_buf;
  void* rdma_fetch_ext_buf;

  struct message* ibv_recv_buf;
  bool rdma_msg_ready;
  bool ibv_msg_ready;

  bool mr_init;
  bool reconnecting;

};

enum dhtClientOps {
  OP_GET = 1,
  OP_PUT = 2,
  OP_CONTAINS = 3,
  OP_DELETE = 4
};

class Client {
private:
  IBConnManager* manager;
  std::vector<struct ServerInfo*> servers;
  unsigned int server_count;
  int read_mode;

  // Asynchronous
  int on_addr_resolved(struct ServerInfo* server, struct rdma_cm_id *id);
  int on_connection(struct ServerInfo* server, struct rdma_cm_id *id);
  int on_disconnect(struct ServerInfo* server, struct rdma_cm_id *id);
  int on_route_resolved(struct ServerInfo* server, struct rdma_cm_id *id);
  int on_reject(struct ServerInfo* server, const char* private_data);

  int read_(const KEY_TYPE key, size_t key_len, VAL_TYPE& value, size_t& val_len, int op);
  int read_server_(const KEY_TYPE key, size_t key_len, VAL_TYPE& value, size_t& val_len, int op);
  int write_(const KEY_TYPE key, size_t key_len, const VAL_TYPE value, size_t val_len, int op);

  void init_serverinfo(struct ServerInfo* this_info);
  void create_mrs(struct ServerInfo* server);
public:

  // Setup operations
  Client();
  int setup();
  void verbosity(enum ibman_verb verb);
  void set_read_mode(read_modes mode);
  int ready();
  int add_server(const char* server_host, const char* server_port);

  // DHT operations
  int get(const KEY_TYPE key, VAL_TYPE& value);
  int put(const KEY_TYPE key, const VAL_TYPE value);
  int contains(const KEY_TYPE key);
  int remove(const KEY_TYPE key);
 
  //similar to get and put, but with size parameter
  int put_with_size(const KEY_TYPE key, VAL_TYPE value, size_t key_len, size_t val_len);
  int get_with_size(const KEY_TYPE Key, VAL_TYPE& value, size_t& val_en);


  template <class K, class V>
  int get_ext(K key, V& value) {
    std::string kstr;
    key.SerializeToString(&kstr);

    int rval;
    size_t val_len;
    char* val_buf = (char*)malloc(sizeof(char)*MAX_BUF);

    if (val_buf == NULL) {
      manager->log(VERB_VITAL,"MSG_DHT_GET: malloc failed!\n");
    }

    if (read_mode == READ_MODE_RDMA) {
      rval = read_(kstr.c_str(), kstr.size(), val_buf, val_len, OP_GET);
    } else {
      rval = read_server_(kstr.c_str(), kstr.size(), val_buf, val_len, OP_GET);
    }
    //manager->log(VERB_VITAL,"MSG_DHT_GET: key:%s value:%s key_len:%lu val_len:%lu\n",
    //             binaryToString(kstr.c_str(), kstr.size()).c_str(),
    //             binaryToString(val_buf, val_len).c_str(), kstr.size(), val_len);

    if (rval == POST_GET_FOUND)
      value.ParseFromString(std::string(val_buf,val_len));
    free(val_buf);
    return rval;
  }

  template <class K, class V>
  int put_ext(K key, V value) {
    std::string kstr, vstr;
    key.SerializeToString(&kstr);
    value.SerializeToString(&vstr);
    //manager->log(VERB_VITAL,"MSG_DHT_PUT: key:%s value:%s key_len:%lu val_len:%lu\n",
    //             binaryToString(kstr.c_str(), kstr.size()).c_str(),
    //             binaryToString(vstr.c_str(), vstr.size()).c_str(), kstr.size(), vstr.size());

    return write_(kstr.c_str(), kstr.size(), vstr.c_str(), vstr.size(), OP_PUT);
  }

  template <class K>
  int contains_ext(K key) {
    std::string kstr;
    key.SerializeToString(&kstr);

    int rval;
    size_t val_len;
    char* val_buf = (char*)malloc(sizeof(char)*MAX_BUF);
    if (read_mode == READ_MODE_RDMA) {
      rval = read_(key.c_str(), key.size(), val_buf, val_len, OP_CONTAINS);
    } else { //read_mode == READ_MODE_SERVER
      rval = read_server_(key.c_str(), key.size(), val_buf, val_len, OP_CONTAINS);
    }
    free(val_buf);
    return rval;
  }

  template <class K>
  int remove_ext(K key) {
    std::string kstr;
    key.SerializeToString(&kstr);
    return write_(kstr.c_str(), kstr.size(), NULL, 0, OP_DELETE);
  }
  // Maintenance operations
  int do_event_loop();
  void teardown();

  // Hooks
  static void hook_ready(/*void* table, size_t entries,*/ void* context);
  static void hook_ibv_recv(int type, struct message* msg, size_t len, IBConn* conn, void* context);
  static void hook_rdma_recv(int type, /*void* entry, void* extents,*/ IBConn* conn, void* context);
  static int on_event(struct rdma_cm_event *event, void* ec_context, void* event_context);

  // RT stats/variables
  void print_stats(void);
  size_t stats_rdma_rts;
  size_t stats_rdma_ht_reprobes;
  size_t stats_rdma_locked;
  size_t stats_rdma_bad_extents;
};

#endif // STORE_CLIENT_H

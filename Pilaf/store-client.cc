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
 *   store-client.cc: DHT client functions     *
 ***********************************************/

#include "store-client.h"
#include "store-constants.h"

/**
 * Client constructor. Creates a connection manager,
 * initialize statistics variables.
 */
Client::Client() {
  server_count = 0;
  servers.clear();
  read_mode = READ_MODE_RDMA;

  manager = new IBConnManager(R_CLIENT);
  manager->verbosity(VERB_WARN);
  manager->set_event_hook(on_event,(void*)this);

  stats_rdma_rts = 0;
  stats_rdma_ht_reprobes = 0;
  stats_rdma_locked = 0;
  stats_rdma_bad_extents = 0;
}

/**
 * Set client verbosity. Really just a wrapper to
 * the manager's verbosity setting.
 */
void Client::verbosity(enum ibman_verb verb) {
  if (manager) manager->verbosity(verb);
}

/**
 * For comparing server-mediated (verb msg) reads
 * to client-mediated (RDMA) reads. By default, the
 * RDMA mode is used.
 */
void Client::set_read_mode(read_modes mode) {
  read_mode = mode;
}

/**
 * Checks if the (set of) underlying DHT server(s) contains
 * the given key or not.
 */
int Client::contains(const KEY_TYPE key) {

  VAL_TYPE dummy;
  size_t val_len;
  if (read_mode == READ_MODE_RDMA) {
    return read_(key, strlen(key), dummy, val_len, OP_CONTAINS);
  } else { //read_mode == READ_MODE_SERVER
    return read_server_(key, strlen(key), dummy, val_len, OP_CONTAINS);
  }
}

/**
 * Gets the specified key, if it exists, or returns an error
 * code if the key cannot be found.
 */
int Client::get(const KEY_TYPE key, VAL_TYPE& value) {

  size_t val_len;

  if (read_mode == READ_MODE_RDMA) {
    return read_(key, strlen(key), value, val_len, OP_GET);
  } else { //read_mode == READ_MODE_SERVER
    return read_server_(key, strlen(key), value, val_len, OP_GET);
  }
}

/**
 * Stores a (key, value) pair into the underlying DHT, into
 * whichever server is correct for this key.
 */
int Client::put(const KEY_TYPE key, const VAL_TYPE value) {

  return write_(key, strlen(key), value, strlen(value), OP_PUT);
}

/**
 * Deletes a key from the correct underlying DHT server, if it
 * contained it in the first place.
 */
int Client::remove(const KEY_TYPE key) {

  return write_(key, strlen(key), (VAL_TYPE)NULL, 0, OP_DELETE);
}

/**
 * Perform an RDMA read sequence. This is used for the get() and contains()
 * operations when RDMA (client-mediated) mode is active. It performs one or
 * more RDMA round-trips, depending on the key/value types and whether it
 * runs into missing or locked keys or collisions. The stats_ variables are
 * updated accordingly. It also transparently handles reconnects and DHT
 * resizes the occur while it's working.
 */

/*
int read_total = 0;
int read_fail = 0;
int read_re = 0;
*/

//#define PRINT_READ

#ifdef PRINT_READ
int read_total = 0;
#endif

int Client::read_(const KEY_TYPE key, size_t key_len, VAL_TYPE& value, size_t& val_len, int op) {
  int rval = 0;
  size_t hash_idx = 0;
  unsigned int serverepoch = 0;

#ifdef PRINT_READ
  read_total++;
  
  if(read_total % 100000 == 0)
    printf("read total : %d\n", read_total);

#endif

  //if(read_re % 100000 == 0)
  //  printf("fail / total / re_read:  %d / %d / %d\n", read_fail, read_total, read_re);
  
  //read_total++;

re_read:
  //read_re++;
  // Figure out which server has the key we need
  int whichserver = servers[0]->dhtclient->server_for_key(server_count,key,key_len);
  serverepoch = servers[whichserver]->epoch;

  // Figure out where in that server's DHT this key should be
  uintptr_t remoteaddr = (uintptr_t)servers[whichserver]->dhtclient->pre_get(key,key_len,hash_idx);
  size_t remotelen = BLOCK_READ_COUNT*(sizeof(DHT<KEY_TYPE,VAL_TYPE>::dht_block));

  // Issue DHT get request
  stats_rdma_rts++;
  //manager->log(VERB_VITAL,"MSG_DHT_GET: key:%s\n", binaryToString(key, key_len).c_str());
  servers[whichserver]->connection->rdma_fetch(remoteaddr, remotelen, servers[whichserver]->dht_table_mr, servers[whichserver]->rdma_fetch_buf_mr);

  // Wait for response
  do {
    rval = do_event_loop();
  } while(!rval && !servers[whichserver]->rdma_msg_ready &&
          servers[whichserver]->epoch == serverepoch);
  
  servers[whichserver]->rdma_msg_ready = false;

  if (rval) return POST_GET_FAILURE;

  if (servers[whichserver]->epoch != serverepoch) // connection bumped; start again
    goto re_read;

  DHT<const KEY_TYPE,VAL_TYPE>::dht_block* dhtb =
      (DHT<const KEY_TYPE,VAL_TYPE>::dht_block*)(servers[whichserver]->rdma_fetch_buf);

  // Store value, get status
  int result;

  if (op == OP_GET)
    result = servers[whichserver]->dhtclient->post_get(dhtb,key,key_len,value);
  else if (op == OP_CONTAINS)
    result = servers[whichserver]->dhtclient->post_contains(dhtb,key,key_len);
  else diewithcode("Invalid op",op);

  if (result == POST_GET_MISSING) {
    stats_rdma_ht_reprobes++;
    hash_idx++;
    if (hash_idx == CUCKOO_D)
      return (op == OP_CONTAINS)?0:result;
    goto re_read;
  }

  if (result == POST_GET_FOUND) {
    stats_rdma_rts++;
    servers[whichserver]->connection->rdma_fetch((uintptr_t)dhtb->d.key,dhtb->d.ext_capacity,
                                                 servers[whichserver]->dht_ext_mr,
                                                 servers[whichserver]->rdma_fetch_ext_buf_mr);
  } else { //LOCKED
    hash_idx = 0;     // Jinyang's call
    stats_rdma_locked++;
    goto re_read;

  }

  // Wait for response
  do {
    rval = do_event_loop();
  } while(!rval && !servers[whichserver]->rdma_msg_ready &&
          servers[whichserver]->epoch == serverepoch);
  servers[whichserver]->rdma_msg_ready = false;

  if (rval) return POST_GET_FAILURE;

  if (servers[whichserver]->epoch != serverepoch) // connection bumped; start again
    goto re_read;

  // Extents received
  dhtb = (DHT<const KEY_TYPE,VAL_TYPE>::dht_block*)servers[whichserver]->rdma_fetch_buf;

  dhtb->d.value += (char*)servers[whichserver]->rdma_fetch_ext_buf - dhtb->d.key;
  dhtb->d.key = (char*)servers[whichserver]->rdma_fetch_ext_buf;

  if (op == OP_GET)
    result = servers[whichserver]->dhtclient->post_get_extents(dhtb,key,key_len);
  else if (op == OP_CONTAINS)
    result = servers[whichserver]->dhtclient->post_contains_extents(dhtb,key,key_len);
  else diewithcode("Invalid op",op);

  //Integrity64 crc;
  //manager->log(VERB_VITAL,"bucket.crc:%d, crc.crc:%d key_len:%lu val_len:%lu\n", dhtb->d.crc, crc.crc(dhtb->d.key, dhtb->d.key_len+dhtb->d.val_len), dhtb->d.key_len, dhtb->d.val_len);
  //manager->log(VERB_VITAL,"POST_GET: key:%s value:%s\n", binaryToString(dhtb->d.key, dhtb->d.key_len).c_str(), binaryToString(dhtb->d.value, dhtb->d.val_len).c_str());

  if (result == POST_GET_LOCKED) {
    //manager->log(VERB_VITAL, "POST_GET_LOCKED!\n");
    stats_rdma_bad_extents++;
    hash_idx = 0;
    goto re_read;

  } else if (result == POST_GET_COLLISION) {
    //manager->log(VERB_VITAL, "POST_GET_COLLISION!\n");
    //exit(0);
    //read_fail++;
    stats_rdma_ht_reprobes++;
    hash_idx++;
    if (hash_idx == CUCKOO_D)
      return (op == OP_CONTAINS)?0:POST_GET_MISSING;
    goto re_read;

  } else if (result == POST_GET_FOUND) {
    //manager->log(VERB_VITAL, "POST_GET_FOUND!\n");
    if (op == OP_GET) {
      val_len = dhtb->d.val_len;
      memcpy(value,dhtb->d.value,val_len);
    }
    return (op == OP_CONTAINS)?1:result;

  } else if (result == POST_GET_MISSING) {
    return (op == OP_CONTAINS)?0:result;

  } else {
    return result;
  } 
}

/**
 * Perform a server-mediated (verb msg) read to a server, for the
 * get() and contains() operations when the server-mediated mode has
 * been activated.
 */
int Client::read_server_(const KEY_TYPE key, size_t key_len, VAL_TYPE& value, size_t& val_len, int op) {
  int rval = 0;

re_read:
  int whichserver = servers[0]->dhtclient->server_for_key(server_count,key,key_len);
  unsigned int serverepoch = servers[whichserver]->epoch;
  struct dht_message* send_msg = (struct dht_message*)(servers[whichserver]->connection->get_send_buf());
  size_t sendlen = 0;

  // Construct DHT put request
  send_msg->type = (op == OP_GET)?MSG_DHT_GET:MSG_DHT_CONTAINS;

  // Set up message body
  send_msg->data.put.key_len = key_len;

  void* msg_body = &(send_msg->data.put.body);
  memcpy(msg_body, key, key_len);

  //sendlen = sizeof(struct dht_message) + key_len - 1; // -1 for the fake 'char' that is .body
  sendlen = sizeof(send_msg->data.put) + key_len;

  servers[whichserver]->connection->send_message_ext((op == OP_GET)?MSG_DHT_GET:MSG_DHT_CONTAINS,
                                                     (char*)&(send_msg->data),sendlen);

  // Wait for response
  do {
    rval = do_event_loop();
  } while(!rval &&                                      // stop waiting on failure
          !servers[whichserver]->ibv_msg_ready &&       // stop waiting if a message arrives
          servers[whichserver]->epoch == serverepoch);  // stop waiting is the epoch changed
  servers[whichserver]->ibv_msg_ready = false;

  if (servers[whichserver]->epoch != serverepoch) // connection bumped; start again
    goto re_read;

  if (rval) return POST_GET_FAILURE;

  //struct message* recv_msg = servers[whichserver]->connection->get_recv_buf();
  //void* retmsg_body = &(recv_msg->data.put.body);
  struct dht_message* retmsg = (struct dht_message*)servers[whichserver]->ibv_recv_buf;
  char* retmsg_body = &(retmsg->data.valresp);

  if (op == OP_GET) {
    // GET and contains response?
    if (retmsg->type == MSG_DHT_GET_DONE_MISSING) {
      return POST_GET_MISSING;
    } else if (retmsg->type != MSG_DHT_GET_DONE) {
      return POST_GET_FAILURE;
    }
    val_len = *((size_t *)retmsg_body);
    memcpy(value, (char*)(retmsg_body+sizeof(size_t)), val_len);
  } else {

    // CONTAINS and contains response?
    if (retmsg->type != MSG_DHT_CONTAINS_DONE) {
      return POST_CONTAINS_FAILURE;
    }

    return !!(retmsg->data.statusval);
  }
  return 0;

}

/**
 * Perform a verb-msg based write, used for the put() and remove()
 * operations. Sends only one message, and waits for a return
 * confirmation message. If no such message arrives, then the operation
 * caused a resize, and we must perform it again.
 */
int Client::write_(const KEY_TYPE key, size_t key_len, const VAL_TYPE value, size_t val_len, int op) {
  int rval = 0;

re_write:
  int whichserver = servers[0]->dhtclient->server_for_key(server_count,key,key_len);
  unsigned int serverepoch = servers[whichserver]->epoch;
  struct dht_message* send_msg = (struct dht_message*)(servers[whichserver]->connection->get_send_buf());
  size_t sendlen = 0;

  // Construct DHT put request
  send_msg->type = (op == OP_PUT)?MSG_DHT_PUT:MSG_DHT_DELETE;

  // Set up message body
  send_msg->data.put.key_len = key_len;
  if (op == OP_PUT)
    send_msg->data.put.val_len = val_len;

  void* msg_body = &(send_msg->data.put.body);
  if (op == OP_PUT) {
    memcpy((char*)msg_body + sizeof(uint64_t), key, key_len);
    memcpy((char*)msg_body + key_len + sizeof(uint64_t), value, val_len);
    *(uint64_t*)msg_body = servers[whichserver]->dhtclient->check_crc((char*)msg_body+sizeof(uint64_t),key_len+val_len);

    char * kptr = sizeof(uint64_t)+(char*)(&(send_msg->data.put.body));
    char * vptr = sizeof(uint64_t)+send_msg->data.put.key_len+(char*)(&(send_msg->data.put.body));

    //Integrity64 crc;
    //manager->log(VERB_VITAL,"MSG_DHT_PUT: key:%s value:%s crc:%d key_len:%lu val_len:%lu\n", binaryToString(kptr, send_msg->data.put.key_len).c_str(),
    //                     binaryToString(vptr, send_msg->data.put.val_len).c_str(), crc.crc((char*)msg_body+sizeof(uint64_t),key_len+val_len), key_len, val_len);

  } else {
    // REMOVE
    memcpy((char*)msg_body, key, key_len);
  }

  //bytes_xchged += 1 + 2*sizeof(size_t) + key_len + val_len;

  if (op == OP_PUT)
    //sendlen = sizeof(struct message) + key_len + val_len - 1; // -1 for the fake 'char' that is .body
    sendlen = sizeof(send_msg->data.put.key_len) + sizeof(send_msg->data.put.val_len) + key_len + val_len + sizeof(uint64_t);
  else
    //sendlen = sizeof(struct message) + key_len + 1; // -1 for the fake 'char' that is .body
    sendlen = sizeof(send_msg->data.put.key_len) + sizeof(send_msg->data.put.val_len) + key_len;

  servers[whichserver]->connection->send_message_ext((op == OP_PUT)?MSG_DHT_PUT:MSG_DHT_DELETE,
                                                     (char*)&(send_msg->data),sendlen);

  // Wait for response
  do {
    rval = do_event_loop();
  } while(!rval &&                                      // stop waiting on failure
          !servers[whichserver]->ibv_msg_ready &&       // stop waiting if a message arrives
          servers[whichserver]->epoch == serverepoch);  // stop waiting is the epoch changed
  servers[whichserver]->ibv_msg_ready = false;

  if (servers[whichserver]->epoch != serverepoch) // connection bumped; start again
    goto re_write;

  if (rval) return POST_PUT_FAILURE;

  if (op == OP_PUT)
    return (servers[whichserver]->ibv_recv_buf->type == MSG_DHT_PUT_DONE)?0:POST_PUT_FAILURE;
  else
    return (servers[whichserver]->ibv_recv_buf->type == MSG_DHT_DELETE_DONE)?0:POST_PUT_FAILURE;
}


/**
 * For future use. Parallels server setup() method.
 */
int Client::setup(void) {
  return 0;
}

/**
 * Add a server to the client's list of servers. For
 * the round-robinning to work correctly, servers must be added
 * in the same order on all clients. If clients have different
 * server ordering, or different numbers of known servers, then
 * they may/will fail to find key/value pairs that other clients
 * put or read.
 */
int Client::add_server(const char* server_host, const char* server_port) {

  // Reserve space for servers vector element struct
  struct ServerInfo* this_info = (struct ServerInfo*)malloc(sizeof(struct ServerInfo));
  if (this_info == NULL)
    return -1;

  // Set an epoch number so we can detect if the server connection bounces
  srand(getpid());
  this_info->epoch = rand();

  this_info->reconnecting = false;

  // Reserve space for host/port strings
  if (NULL == (this_info->server_host = (char*)malloc(1+strlen(server_host))))
    return -1;
  if (NULL == (this_info->server_port = (char*)malloc(1+strlen(server_port))))
    return -1;

  // Copy in host/port strings
  strcpy(this_info->server_host,server_host);
  strcpy(this_info->server_port,server_port);

  // Initialize every other field
  init_serverinfo(this_info);

  servers.push_back(this_info);
  server_count++;

  return 0;
}

/**
 * Helper function for generic setup of a ServerInfo
 * structure.
 */
void Client::init_serverinfo(struct ServerInfo* this_info) {
  struct addrinfo *addr;

  // MRs not yet initialized for this connection
  this_info->mr_init = false;

  // Needed for hooks
  this_info->client = this;
  this_info->state = CS_CREATED;

  this_info->rdma_fetch_buf = NULL;
  this_info->rdma_fetch_ext_buf = NULL;

  this_info->connection = manager->new_conn(MSG_BUF_SIZE);

  TEST_NZ(getaddrinfo(this_info->server_host, this_info->server_port, NULL, &addr));

  TEST_NZ(rdma_create_id(manager->get_ec(), &(this_info->conn), this_info, RDMA_PS_TCP));


  // Set up other member vars
  this_info->dhtclient = NULL;
  this_info->ready = 0;

  // Initialize required hooks
  this_info->connection->set_recv_hook(hook_ibv_recv,(void*)this_info);
  this_info->connection->set_rdma_recv_hook(hook_rdma_recv,(void*)this_info);
  this_info->connection->set_ready_hook(hook_ready,(void*)this_info);

  TEST_NZ(rdma_resolve_addr(this_info->conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));
  freeaddrinfo(addr);
}

/**
 * Create the necessary client memory regions
 * for RDMA reads and such.
 */
void Client::create_mrs(struct ServerInfo* this_info) {

  if (this_info->mr_init == true) // don't do it twice
    return;

  this_info->rdma_fetch_buf = malloc(RECV_BUF_SIZE);
#if KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP
  this_info->rdma_fetch_ext_buf = malloc(RECV_EXT_SIZE);
#endif

  if (NULL == this_info->rdma_fetch_buf
#if KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP
      || NULL == this_info->rdma_fetch_ext_buf
#endif
  ) {
    die("Failed to allocate local RDMA buffers");
  }

  this_info->rdma_fetch_buf_mr = manager->create_mr(this_info->rdma_fetch_buf,
                                           RECV_BUF_SIZE,
                                           IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE,
                                           MR_SCOPE_LOCAL,
                                           this_info->connection);

#if KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP
  this_info->rdma_fetch_ext_buf_mr = manager->create_mr(this_info->rdma_fetch_ext_buf,
                                               RECV_EXT_SIZE,
                                               IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE,
                                               MR_SCOPE_LOCAL,
                                               this_info->connection);
#endif

  if (NULL == this_info->rdma_fetch_buf_mr
#if KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP
      || NULL == this_info->rdma_fetch_ext_buf_mr
#endif
  ) {
    die("Failed to create MRs for local RDMA buffers");
  }

  manager->set_mr(MR_LOC_LOCAL, MR_SCOPE_LOCAL, this_info->rdma_fetch_buf_mr,
                  MR_TYPE_RDMA_BUF_TABLE, this_info->connection);
#if KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP
  manager->set_mr(MR_LOC_LOCAL, MR_SCOPE_LOCAL, this_info->rdma_fetch_ext_buf_mr,
                  MR_TYPE_RDMA_BUF_EXTENTS, this_info->connection);
#endif

  this_info->mr_init = true;

}

/**
 * Wait for all server connections to be ready before
 * launching any read/write operations.
 */
int Client::ready(void) {
  int rval = 0, ready = 0;
  do {
    rval = do_event_loop();

    ready = 1;
    for(int i = 0; i < servers.size(); i++) {
      ready = ready && servers[i]->ready;
    }
  } while(!ready);

  return rval;
}

/**
 * Tear down all connections to servers.
 */
void Client::teardown(void) {

  for(int i = 0; i < servers.size(); i++) {
    //rdma_destroy_event_channel(servers[i]->ec);
    free(servers[i]->server_host);
    free(servers[i]->server_port);
    free(servers[i]);
  }
  servers.clear();
  return;
}

/**
 * Now that all of the event and message polling
 * has been properly centralized in the connection
 * manager, this is really just a wrapper around the
 * manager's poll_cq() function.
 */
int Client::do_event_loop() {

  return manager->poll_cq(0);
}

/**
 * Called when the address of the server is resolved
 * and a route must therefore be resolved.
 */
int Client::on_addr_resolved(struct ServerInfo* server, struct rdma_cm_id *id) {
  manager->log(VERB_INFO,"client: address resolved.\n");

  if (server->state != CS_CREATED)
    diewithcode("on_addr_resolved() called but server->state was: ",server->state);
  server->state = CS_ADR_RES;

  server->connection->build_connection(id);
  TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));

  return 0;
}

/**
 * Called when a connection is successfully established,
 * after which the server will send its memory regions and
 * we will respond with ours.
 */
int Client::on_connection(struct ServerInfo* server, struct rdma_cm_id *id) {
  manager->log(VERB_INFO,"client: Connection %p established to %s:%s\n",
               server->connection,server->server_host,server->server_port);

  if (server->state != CS_RT_RES)
    diewithcode("on_connection() called but server->state was: ",server->state);
  server->state = CS_ESTAB;

  create_mrs(server);
  server->connection->on_connect();

  return 0;
}

/**
 * A better name for this method would be Client::reconnect.
 * It is called when a server disconnects, and makes every
 * attempt (sometimes iteratively) to reconnect.
 */
int Client::on_disconnect(struct ServerInfo* server, struct rdma_cm_id *id) {

  int wasready = server->ready;
  server->reconnecting = true;

  manager->log(VERB_WARN,"Server %s:%s disconnected, attempting to reconnect\n",
               server->server_host,server->server_port);

  free(server->rdma_fetch_buf);
  server->rdma_fetch_buf = NULL;

  free(server->rdma_fetch_ext_buf);
  server->rdma_fetch_ext_buf = NULL;


  if (server->connection->s_conn->connected < CONN_FINIS) {
    server->connection->disconnect();
    server->connection->disconnect();
  }

reconn_retry:
  if (server->connection->refcount == 0) {
    server->connection->destroy_connection();
  } else {
    manager->log(VERB_ERROR,"Cannot destroy dead connection with refcount %d\n",server->connection->refcount);
    die("");
  }

  server->mr_init = false;

  //server->ready = 0;
  delete server->connection;

  if (server->dhtclient)
    delete server->dhtclient;

  //server->dhtclient = NULL;
  server->epoch++;

  usleep(CONNECT_RETRY_SLEEP);
  init_serverinfo(server);

  int rval = 0;
  if (wasready) {
    manager->log(VERB_DEBUG,"Starting reconnect with rv=%d, s->r=%d\n",rval,server->ready);

    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);
    long long deltatime;

    do {
      rval = do_event_loop();
      gettimeofday(&end_time, NULL);
      deltatime = (long long) (end_time.tv_sec - start_time.tv_sec) * 1000000
                + (end_time.tv_usec - start_time.tv_usec);
    } while(!rval && !server->ready && !(CONNECT_TIMEOUT <= deltatime && server->state == CS_RT_RES));

    manager->log(VERB_DEBUG,"Ending reconnect with rv=%d, s->r=%d\n",rval,server->ready);

    if (server->ready) {
      manager->log(VERB_WARN,"Successfully reconnected to %s:%s\n",
                   server->server_host,server->server_port);
    } else /*if (!rval)*/ {
      manager->log(VERB_DEBUG,"Did not reconnect successfully (timeout?), reconnecting\n");
      goto reconn_retry;
    } /*else
      diewithcode("Connection error during reconnection: ",rval);*/
  }

  server->reconnecting = false;

  return rval;  // Return success or failure
}

/**
 * When the address of a server is resolved, and a route is
 * resolved as well, it's time to connect in earnest.
 */
int Client::on_route_resolved(struct ServerInfo* server, struct rdma_cm_id *id) {
  struct rdma_conn_param cm_params;

  manager->log(VERB_INFO,"client: route resolved to %s:%s.\n",
               server->server_host,server->server_port);

  if (server->state != CS_ADR_RES)
    diewithcode("on_route_resolved() called but server->state was: ",server->state);
  server->state = CS_RT_RES;

  server->connection->build_params(&cm_params);
  TEST_NZ(rdma_connect(id, &cm_params));

  return 0;
}

/**
 * If the server actively rejected a connection, return a 0
 * if the server added the "retry" note that means it is just
 * resizing, or if no note was present, return a 1.
 */
int Client::on_reject(struct ServerInfo* server, const char* private_data) {
  if (private_data == NULL)
    return -1;

  // Return
  return strcmp("retry",private_data);
}

/**
 * Handle connection events by calling the relevant event
 * handlers in this class. Called by the manager's poll_cq() method.
 */
int Client::on_event(struct rdma_cm_event *event, void* ec_context, void* event_context) {
  int r = 0;

  Client* client = (Client*)ec_context;
  struct ServerInfo* server = (struct ServerInfo*)event_context;

  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED) {
    r = client->on_addr_resolved(server,event->id);
  } else if (event->event == RDMA_CM_EVENT_ADDR_ERROR) {
    client->manager->log(VERB_ERROR,"client: Failed to resolve RDMA address %s\n",
                         server->server_host);
    exit(-1);
  } else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED) {
    r = client->on_route_resolved(server,event->id);
  } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
    r = client->on_connection(server,event->id);
  } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
    r = client->on_disconnect(server,event->id);
  } else if (event->event == RDMA_CM_EVENT_REJECTED) {
    r = client->on_reject(server,(event->param.conn.private_data_len == 0)?
                          (const char*)NULL:(const char*)event->param.conn.private_data);
    if (r) {
      client->manager->log(VERB_ERROR,
                            "client: Endpoint rejected connection. Is server running at %s:%s?\n",
                            server->server_host,
                            server->server_port);
      exit(-1);
      if (server->connection->s_conn) {
        server->connection->s_conn->connected = CONN_FINIS;
      }
    }
    // Only relevant if first connection (??)
    if (!server->reconnecting) {
      r = client->on_disconnect(server,event->id);
    }
  } else if (event->event == RDMA_CM_EVENT_UNREACHABLE) {
    client->manager->log(VERB_ERROR,"client: Endpoint is unreachable at %s:%s\n",
                         server->server_host,server->server_port);
    exit(-1);
  } else if (event->event == RDMA_CM_EVENT_TIMEWAIT_EXIT) {
    client->manager->log(VERB_INFO,"timewait_exit confirmed\n");
  } else if (event->event == RDMA_CM_EVENT_CONNECT_ERROR) {
    client->manager->log(VERB_ERROR,"client: Connection handshake encountered an error\n");
    if (server->connection->s_conn) {
      server->connection->disconnect();
      server->connection->disconnect();
    } else {
      client->manager->log(VERB_ERROR,"client: On connection error, no connection to terminate\n");
    }
  } else
    diewithcode("client: on_event: unknown event.",event->event);

  return r;
}

/**
 * Hook called when a verb message is received.
 */
void Client::hook_ibv_recv(int type, struct message* msg, size_t len, IBConn* conn, void* context) {
  struct ServerInfo* server = (struct ServerInfo*)context;
  server->ibv_recv_buf = msg;
  server->ibv_msg_ready = true;
}

/**
 * Hook called when an RDMA operation completes.
 */
void Client::hook_rdma_recv(int type, IBConn* conn, void* context) {
  struct ServerInfo* server = (struct ServerInfo*)context;

  server->rdma_msg_ready = true;
}

/**
 * Hook called when a connection completes MR exchange
 * and becomes ready for DHT operations.
 */
void Client::hook_ready(void* context) {
  struct ServerInfo* server = (struct ServerInfo*)context;

  // The server->client->manager thing is a little unfortunate, but prevents
  // us from needing to pass two different context items back.

  server->dht_table_mr = server->client->manager->fetch_mr(MR_LOC_REMOTE, MR_TYPE_DHT_TABLE, server->connection);
  if (server->dht_table_mr == NULL)
    die("Failed to get DHT_TABLE_MR after hook_ready triggered");

#if KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP
  server->dht_ext_mr = server->client->manager->fetch_mr(MR_LOC_REMOTE, MR_TYPE_DHT_EXTENTS, server->connection);
  if (server->dht_ext_mr == NULL)
    die("Failed to get DHT_EXT_MR after hook_ready triggered");
#endif

  size_t capacity = server->dht_table_mr->length;
  size_t entries = capacity/sizeof(DHT<KEY_TYPE,VAL_TYPE>::dht_block);

  server->entries = entries;
  server->table = server->dht_table_mr->addr;
  server->ibv_msg_ready = false;
  server->rdma_msg_ready = false;

  server->dhtclient = new DHTClient<const KEY_TYPE,VAL_TYPE>(server->dht_table_mr->addr,entries);

  server->client->manager->log(VERB_DEBUG,
               "Ready hook triggered with mem at %p, with %zu entries for %s:%s conn %p\n",
               server->dht_table_mr->addr,entries,server->server_host,server->server_port,server->connection);
  server->ready = 1;
}

/**
 * Print statistics about RDMA operations.
 */
void Client::print_stats(void) {
  manager->log(VERB_WARN,"Current RDMA Stats:\n");
  manager->log(VERB_WARN,"RDMA RTs:         %19d\n",stats_rdma_rts);
  manager->log(VERB_WARN,"RDMA HT Reprobes: %19d\n",stats_rdma_ht_reprobes);
  manager->log(VERB_WARN,"RDMA Locked Rows: %19d\n",stats_rdma_locked);
  manager->log(VERB_WARN,"RDMA Bad Extents: %19d\n",stats_rdma_bad_extents);
}


int Client::put_with_size(const KEY_TYPE key, const VAL_TYPE value, size_t key_len, size_t val_len){

  return write_(key, key_len, value, val_len, OP_PUT);
}

int Client::get_with_size(const KEY_TYPE key, VAL_TYPE value, size_t key_len, size_t &val_len){
  
  if (read_mode == READ_MODE_RDMA) {
    return read_(key, key_len, value, val_len, OP_GET);
  } else { //read_mode == READ_MODE_SERVER
    return read_server_(key, key_len, value, val_len, OP_GET);
  }
}


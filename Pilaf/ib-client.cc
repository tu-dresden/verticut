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
 *   ib-client.cc: Client functions used for   *
 *                 testing performance of      *
 *                 underlying Infiniband       *
 *                 connection class.           *
 ***********************************************/

#include "ib-client.h"

IBClient::IBClient() {
  manager = new IBConnManager(R_CLIENT);
  manager->verbosity(VERB_WARN);
  manager->set_event_hook(on_event,(void*)this);
}

void IBClient::verbosity(enum ibman_verb verb) {
  if (manager) manager->verbosity(verb);
}

// Do an RDMA fetch of (bytes) bytes.
int IBClient::rdma_fetch(size_t bytes) {
  int rval = 0;

  // Issue DHT get request
  void* start_addr = (void*)((char*)server.rdma_remote_buf_mr->addr + (rand() % (server.rdma_remote_buf_mr->length-bytes)));
  server.connection->rdma_fetch((uintptr_t)start_addr, bytes, server.rdma_remote_buf_mr, server.rdma_fetch_buf_mr);
  //server.connection->rdma_push((uintptr_t)start_addr, bytes, (uintptr_t)server.connection->get_send_buf(),server.rdma_remote_buf_mr, server.rdma_fetch_buf_mr);

  // Wait for response
  do {
    rval = do_event_loop();
  } while(!rval && !server.rdma_msg_ready);

  server.rdma_msg_ready = false;
    
  return rval;
}

// Do a ping-pong (2-way verb test) of (bytes) bytes
int IBClient::ib_pingpong(size_t twopower, bool bigreply = false) {
  int rval = 0;

  struct message* send_msg = server.connection->get_send_buf();

  if (bigreply) {
    send_msg->mdata = (unsigned char)twopower;
    server.connection->send_message_ext(1337,(char*)&(send_msg->mdata),RESP_SIZE);
  } else {
    send_msg->mdata = (unsigned char)0xff;
    server.connection->send_message_ext(1337,(char*)&(send_msg->mdata),1<<twopower);
  }

  // Wait for response
  do {
    rval = do_event_loop();
  } while(!rval &&                        // stop waiting on failure
          !server.ibv_msg_ready);         // stop waiting if a message arrives
  server.ibv_msg_ready = false;

  return rval;
}

// Do a ping (1-way verb test) of (bytes) bytes
int IBClient::ib_ping(size_t bytes) {
  int rval = 0;

  struct message* send_msg = server.connection->get_send_buf();

  server.sent = false;

  server.connection->send_message_ext(1338,(char*)&(send_msg->mdata),bytes);

  // Wait for response
  do {
    rval = do_event_loop();
  } while(!rval &&                        // stop waiting on failure
          !server.sent);         // stop waiting if a message arrives
  server.sent = false;

  return rval;
}

int IBClient::setup(void) {
  return 0;
}

int IBClient::add_server(const char* server_host, const char* server_port) {
  struct addrinfo *addr;

  // MRs not yet initialized for this connection
  server.mr_init = false;

  // Needed for hooks
  server.client = this;

  // Reserve space for host/port strings
  if (NULL == (server.server_host = (char*)malloc(1+strlen(server_host))))
    return -1;
  if (NULL == (server.server_port = (char*)malloc(1+strlen(server_port))))
    return -1;

  // Copy in host/port strings
  strcpy(server.server_host,server_host);
  strcpy(server.server_port,server_port);
  server.rdma_fetch_buf = NULL;

  server.connection = manager->new_conn(MSG_BUF_SIZE);

  TEST_NZ(getaddrinfo(server_host, server_port, NULL, &addr));

  TEST_NZ(rdma_create_id(manager->get_ec(), &(server.conn), &server, RDMA_PS_TCP));

  // Set up other member vars
  server.ready = 0;

  // Set an epoch number so we can detect if the server connection bounces
  srand(getpid());
  server.epoch = rand();

  // Initialize required hooks
  server.connection->set_recv_hook(hook_ibv_recv,(void*)&server);
  server.connection->set_rdma_recv_hook(hook_rdma_recv,(void*)&server);
  server.connection->set_ready_hook(hook_ready,(void*)&server);
  server.connection->set_send_complete_hook(hook_send_complete,(void*)&server);

  TEST_NZ(rdma_resolve_addr(server.conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));
  freeaddrinfo(addr);

  return 0;
}

void IBClient::create_mrs(struct ServerInfo* this_info) {

  if (this_info->mr_init == true) // don't do it twice
    return;

  this_info->rdma_fetch_buf = malloc(MAX_RDMA_RECV_BUF_SIZE);

  if (NULL == this_info->rdma_fetch_buf) {
    die("Failed to allocate local RDMA buffer");
  }

  this_info->rdma_fetch_buf_mr = manager->create_mr(this_info->rdma_fetch_buf,
                                           MAX_RDMA_RECV_BUF_SIZE,
                                           IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE,
                                           MR_SCOPE_LOCAL,
                                           this_info->connection);

  if (NULL == this_info->rdma_fetch_buf_mr) {
    die("Failed to create MRs for local RDMA buffer");
  }

  manager->set_mr(MR_LOC_LOCAL, MR_SCOPE_LOCAL, this_info->rdma_fetch_buf_mr,
                  MR_TYPE_RDMA_LADLE, this_info->connection);

  this_info->mr_init = true;

}

int IBClient::ready(void) {
  int rval = 0, ready = 0;
  do {
    rval = do_event_loop();

    ready = server.ready;
  } while(!rval && !ready);

  return rval;
}

void IBClient::teardown(void) {

  free(server.server_host);
  free(server.server_port);

  return;
}

int IBClient::do_event_loop() {
  struct rdma_cm_event *event = NULL;

  manager->poll_cq(0);
  return 0;
}

int IBClient::on_addr_resolved(struct ServerInfo* server, struct rdma_cm_id *id) {
  fprintf(stdout,"client: address resolved.\n");

  server->connection->build_connection(id);
  TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));

  return 0;
}

int IBClient::on_connection(struct ServerInfo* server, struct rdma_cm_id *id) {
  create_mrs(server);
  server->connection->on_connect();

  return 0;
}

int IBClient::on_disconnect(struct ServerInfo* server, struct rdma_cm_id *id) {

  free(server->rdma_fetch_buf);
  server->connection->destroy_connection();//id->context);
  server->mr_init = false;

  fprintf(stdout, "Server %s:%s disconnected, attempting to reconnect\n",server->server_host,server->server_port);

  int wasready = server->ready;
  struct addrinfo *addr;

  server->ready = 0;
  delete server->connection;

  server->epoch++;
  server->rdma_fetch_buf = NULL;

  // Now create a new connection.
  server->connection = manager->new_conn(MSG_BUF_SIZE); //NULL,0);

  TEST_NZ(getaddrinfo(server->server_host, server->server_port, NULL, &addr));

  TEST_NZ(rdma_create_id(manager->get_ec(), &(server->conn), NULL, RDMA_PS_TCP));

  // Initialize required hooks
  server->connection->set_recv_hook(hook_ibv_recv,(void*)server);
  server->connection->set_rdma_recv_hook(hook_rdma_recv,(void*)server);
  server->connection->set_ready_hook(hook_ready,(void*)server);

  TEST_NZ(rdma_resolve_addr(server->conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));
  freeaddrinfo(addr);

  fprintf(stderr, "Attempting to reconnect to %s:%s\n",server->server_host,server->server_port);
  int rval = 0;
  if (wasready) { 
   do {
     rval = do_event_loop();
    } while(!rval && !server->ready);
  }

  if (server->ready)
    fprintf(stderr, "Successfully reconnected to %s:%s\n",server->server_host,server->server_port);

  return rval;  // Return success or failure
}

int IBClient::on_route_resolved(struct ServerInfo* server, struct rdma_cm_id *id) {
  struct rdma_conn_param cm_params;

  printf("client: route resolved to %s:%s.\n",server->server_host,server->server_port);
  server->connection->build_params(&cm_params);
  TEST_NZ(rdma_connect(id, &cm_params));

  return 0;
}

int IBClient::on_reject(struct ServerInfo* server, const char* private_data) {
  if (private_data == NULL)
    return -1;

  // Return
  return strcmp("retry",private_data);
} 

int IBClient::on_event(struct rdma_cm_event *event, void* ec_context, void* event_context) {
  int r = 0;

  IBClient* client = (IBClient*)ec_context;
  struct ServerInfo* server = (struct ServerInfo*)event_context;

  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED) {
    r = client->on_addr_resolved(server,event->id);
  } else if (event->event == RDMA_CM_EVENT_ADDR_ERROR) {
    fprintf(stderr,"client: Failed to resolve RDMA address %s\n",server->server_host);
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
      fprintf(stderr,"client: Endpoint rejected connection. Is server running at %s:%s?\n",
             server->server_host,
             server->server_port);
      exit(-1);
    }
  } else
    diewithcode("client: on_event: unknown event.",event->event);

  return r;
}

void IBClient::hook_ibv_recv(int type, struct message* msg, size_t len, IBConn* conn, void* context) {
  struct ServerInfo* server = (struct ServerInfo*)context;
  server->ibv_recv_buf = msg;
  server->ibv_msg_ready = true;
}

void IBClient::hook_rdma_recv(int type, /*void* entry, void* extents,*/ IBConn* conn, void* context) {
  struct ServerInfo* server = (struct ServerInfo*)context;

  server->rdma_msg_ready = true;
}

void IBClient::hook_send_complete(void* context) {
  struct ServerInfo* server = (struct ServerInfo*)context;

  server->sent = true;
}

//TODO capacity calculation
void IBClient::hook_ready(/*void* peer_mr, size_t capacity,*/ void* context) {
  struct ServerInfo* server = (struct ServerInfo*)context;

  // The server->client->manager thing is a little unfortunate, but prevents
  // us from needing to pass two different context items back.

  server->rdma_remote_buf_mr = server->client->manager->fetch_mr(MR_LOC_REMOTE, MR_TYPE_RDMA_POOL, server->connection);
  if (server->rdma_remote_buf_mr == NULL)
    die("Failed to get RDMA_POOL after hook_ready triggered");

  server->ibv_msg_ready = false;
  server->rdma_msg_ready = false;

  printf("Ready hook triggered with mem at %p\n",server->rdma_remote_buf_mr->addr);
  server->ready = 1;
}


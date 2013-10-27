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
 *   ib-server.cc: Server functions used for   *
 *                 testing performance of      *
 *                 underlying Infiniband       *
 *                 connection class.           *
 ***********************************************/

#include "ib-server.h"

IBServer::IBServer() {
  listener = NULL;
  port = 0;

  // Deal with sharing MRs
  mr_init = false;

  // Connection manager
  manager = new IBConnManager(R_SERVER);
  manager->verbosity(VERB_WARN);
  manager->set_event_hook(on_event,(void*)this);
}

void IBServer::verbosity(enum ibman_verb verb) {
  if (manager) manager->verbosity(verb);
}

void IBServer::create_mrs(IBConn* conn) {

  if (conn == NULL || mr_init == true) //Can't reserve yet, or already done
    return;

  //rdma_local_buf = malloc(MAX_RDMA_BUF_SIZE);
  rdma_local_buf = mmap(NULL,MAX_RDMA_BUF_SIZE,PROT_READ|PROT_WRITE,
                        MAP_ANONYMOUS|MAP_NORESERVE|MAP_PRIVATE,-1,0);
  //if (NULL == rdma_local_buf)
  if ((void*)-1 == rdma_local_buf)
    diewithcode("Could not reserve RDMA buffer",errno);

  rdma_local_mr = manager->create_mr(rdma_local_buf,
                                     MAX_RDMA_BUF_SIZE,
                                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE,
                                     MR_SCOPE_GLOBAL,
                                     conn);
  manager->set_mr(MR_LOC_LOCAL,MR_SCOPE_GLOBAL,rdma_local_mr,MR_TYPE_RDMA_POOL,NULL);

  mr_init = true;
}

int IBServer::setup(void) {
  // IB setup
  clients.clear();

  return 0;
}

int IBServer::ready(unsigned short port) {

  // Launch on port SERVER_PORT
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);

  TEST_NZ(rdma_create_id(manager->get_ec(), &listener, this, RDMA_PS_TCP));
  TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
  TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */

  port = ntohs(rdma_get_src_port(listener));
  manager->log(VERB_INFO,"server: listening on port %d.\n", port);

  is_ready = true;

  int rval;
  do {
    rval = do_event_loop();
  } while(!rval);
  manager->log(VERB_ERROR,"%s terminating because rgce returned %d.\n","server",errno);

  rdma_destroy_id(listener);

  return 0;
}

int IBServer::do_event_loop() {
  struct rdma_cm_event *event = NULL;

  manager->poll_cq(0);

dead_client_cleanup:

  std::vector<IBConn*>::iterator it = clients.begin();
  for(; it != clients.end(); it++) {
    if (CONN_FINIS == (*it)->is_connected()) {
      (*it)->destroy_connection();
      clients.erase(it);
      it--;    // counteract the coming it++;
      continue;
    }
  }
  return 0;
}

int IBServer::on_connect_request(struct rdma_cm_id *id) {
  struct rdma_conn_param cm_params;

  if (!is_ready) { 
    manager->log(VERB_INFO,"server: received connection request, rejecting\n");
    const char* action = "retry";
    TEST_NZ(rdma_reject(id, action, 1+strlen(action)));
    return 0;
  }

  manager->log(VERB_ERROR,"server: received connection request, accepting [%zu conns were active]\n",clients.size());

  IBConn* thisconn = manager->new_conn(MSG_BUF_SIZE);

  id->context = thisconn;
  if (thisconn->build_connection(id)) {
    manager->log(VERB_ERROR,"server: failed to create connection\n");
    thisconn->disconnect();
    thisconn->disconnect();
  }
  thisconn->build_params(&cm_params);

  create_mrs(thisconn);

  clients.push_back(thisconn);
  if (rdma_accept(id, &cm_params)) {
    manager->log(VERB_ERROR,"server: client disconnected before receiving accept; destroying connection\n");
    thisconn->disconnect();
    thisconn->disconnect();
  }

  return 0;
}

void IBServer::hook_ibv_recv(int type, struct message* msg_, size_t len, IBConn* conn, void* context) {
  IBServer* myself = (IBServer*)context;

  if (type == 1337) {
    //ping-pong
	size_t respsize = sizeof(msg_->type);
	if ((unsigned char)(msg_->mdata) != (unsigned char)0xff)
      respsize = 1<<(unsigned char)(msg_->mdata);
    if (RESP_SIZE > respsize)
      respsize = RESP_SIZE-(sizeof(msg_->type));

    conn->send_message_ext(msg_->type,(char*)(&(msg_->mdata)),respsize);
  }
  // else ping
}

int IBServer::on_connection(struct rdma_cm_id *id)
{
  ((IBConn*)id->context)->on_connect();
  ((IBConn*)id->context)->set_recv_hook(IBServer::hook_ibv_recv,(void*)this);

  // NEW
  manager->log(VERB_INFO,"server: sending MR information to client\n");
  if (((IBConn*)id->context)->send_mr()) {
    manager->log(VERB_ERROR,"server: send_mr() failed to complete\n");
    ((IBConn*)id->context)->disconnect();
    //return -1;
  }
  // ^ NEW

  return 0;
}

int IBServer::on_disconnect(struct rdma_cm_id *id) {
  manager->log(VERB_ERROR,"peer disconnected.\n");

  ((IBConn*)id->context)->disconnect();
  ((IBConn*)id->context)->disconnect();
  
  return 0;
}

int IBServer::on_event(struct rdma_cm_event *event, void* ec_context, void* event_context) {

  int r = 0;
  IBServer* server = (IBServer*)ec_context;

  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
    r = server->on_connect_request(event->id);
  } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
    r = server->on_connection(event->id);
  } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
    r = server->on_disconnect(event->id);
  } else if (event->event == RDMA_CM_EVENT_REJECTED) {
    server->manager->log(VERB_ERROR,"client: Endpoint rejected connection.\n");
    exit(-1);
  } else if (event->event == RDMA_CM_EVENT_TIMEWAIT_EXIT) {
    server->manager->log(VERB_DEBUG,"timewait_exit confirmed\n");
  } else {
    die("server: on_event: unknown event.");
  }

  return r;
}


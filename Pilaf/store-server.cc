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
 *   store-server.cc: DHT server functions     *
 ***********************************************/

#include <string>
#include <stdint.h>
#include "image_tools.h"
#include "store-server.h"
#include "store-constants.h"

Server::Server() {
  resizing = false;
  is_ready = false;
  listener = NULL;
  port = 0;
  dhtclient = NULL;

  // Use this to tell if DHT resized
  srand(getpid());
  epoch = rand();

  // Deal with sharing MRs
  mr_init = false;

  // Logging to disk
  logging = false;
  log_fname = NULL;
  log_fh = 0;

  // Connection manager
  manager = new IBConnManager(R_SERVER);
  manager->verbosity(VERB_WARN);
  manager->set_event_hook(on_event,(void*)this);
}

void Server::verbosity(enum ibman_verb verb) {
  if (manager) manager->verbosity(verb);
}

/**
 * For toggling logging to disk on/off
 */
void Server::set_logging(bool state, char* fname = NULL) {
  bool waslogging = logging;
  logging = state;

  // Close an existing logfile
  if (waslogging && log_fname && (!logging || fname != log_fname)) {
    log_flush();
    fclose(log_fh);
  }

  // Open and truncate a new logfile
  if (logging && fname && (!waslogging || fname != log_fname)) {
    log_fh = fopen(fname,"w");
    if (!log_fh) {
      manager->log(VERB_ERROR,"Could not open %s for logging!\n");
      logging = false;
    } else {
      log_fname = fname;  // hopefully that buffer won't disappear...
      log_offset = 0;
      if (NULL == (log_buf = (char*)malloc(sizeof(char)*LOG_BUF_SIZE))) {
        manager->log(VERB_ERROR,"Could not create buffer for logging\n");
        set_logging(false,NULL);
      }
    }
  }
}

/**
 * Flush operation log to disk
 */
void Server::log_flush(void) {
  if (log_offset < LOG_BUF_FLUSH)
    return;

  fwrite(log_buf,log_offset,1,log_fh);
  log_offset = 0;
}


int Server::hook_pre_resize(size_t oldsize, std::vector<DHT<KEY_TYPE,VAL_TYPE>::dht_block>* contents, void* context) {
  Server* myself = (Server*)context;
  myself->resizing = true;

  myself->manager->log(VERB_INFO,"Beginning resize\n");

  if (myself->dhtclient) {
    delete myself->dhtclient;
    myself->dhtclient = NULL;
  }

  if (!myself->is_ready)
    return 0;

  int had_clients = myself->clients.size();
  myself->manager->log(VERB_DEBUG,"Waiting for %d clients to disconnect\n",had_clients);
  int clients = 0;
  int iters = 1;
  do {
    // Disconnect clients and wait for confirmation
    for(int i=0; i<myself->clients.size(); i++) {
      if (myself->clients[i]->is_connected() >= CONN_SETUP &&
          myself->clients[i]->is_connected() <  CONN_TEARD)
      {
        myself->clients[i]->disconnect();
      } else if (myself->clients[i]->is_connected() == CONN_TEARD && !myself->clients[i]->s_conn->disconnecting) {
        myself->clients[i]->disconnect();
      }
    }
    if (myself->do_event_loop()) {
      myself->manager->log(VERB_ERROR,
                           "Something went wrong in the event loop "
                           "during disconnection. Terminating\n");
      exit(1);
    }
    clients = 0;
    for(int i=0; i<myself->clients.size(); i++)
      if (CONN_TEARD > myself->clients[i]->is_connected())
        clients++;
  } while (iters++, clients);

  myself->manager->log(VERB_DEBUG,
                       "Finished disconnecting %d clients in %d iterations.\n",
                       had_clients,iters);

  myself->manager->destroy_global_mrs();
  myself->mr_init = false;

  return 0;
}

int Server::hook_post_resize(size_t newsize, std::vector<DHT<KEY_TYPE,VAL_TYPE>::dht_block>* contents, void* context) {
  Server* myself = (Server*)context;

  myself->dhtclient = new DHTClient<KEY_TYPE, VAL_TYPE>((void*)&((*contents)[0]),newsize);

  myself->manager->log(VERB_INFO,"Resizing is complete; accepting clients again\n");
  myself->resizing = false;
  myself->epoch++;

  return 0;
}

void Server::create_mrs(IBConn* conn_) {

  if (conn_ == NULL || mr_init == true) //Can't reserve yet, or already done
    return;

  struct ibv_mr* dht_table_mr = manager->create_mr((void*)&(this->dht.buckets_[0]),
                                   this->dht.buckets_.size()*sizeof(DHT<KEY_TYPE,VAL_TYPE>::dht_block),
                                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ,
                                   MR_SCOPE_GLOBAL,
                                   conn_);
  manager->set_mr(MR_LOC_LOCAL,MR_SCOPE_GLOBAL,dht_table_mr,MR_TYPE_DHT_TABLE,NULL);

  struct ibv_mr* dht_extents_mr = manager->create_mr(this->dht.memregion,
                                   this->dht.ext_size_,
                                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ,
                                   MR_SCOPE_GLOBAL,
                                   conn_);
  manager->set_mr(MR_LOC_LOCAL,MR_SCOPE_GLOBAL,dht_extents_mr,MR_TYPE_DHT_EXTENTS,NULL);

  mr_init = true;
}

int Server::setup(void) {
  // IB setup
  clients.clear();

  // Use the same resize hooks for the table and the extents
  dht.set_resize_hooks(Server::hook_pre_resize,(void*)this,Server::hook_post_resize,(void*)this);
#if KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP
  dht.set_resize_extents_hooks(Server::hook_pre_resize,(void*)this,Server::hook_post_resize,(void*)this);
#endif

  // This triggers just the resize hooks (no actual resize) and
  // therefore causes the dhtclient to be created
  dht.resize();

  return 0;
}

int Server::ready(unsigned short port) {

  // Launch on port SERVER_PORT
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);

  TEST_NZ(rdma_create_id(manager->get_ec(), &listener, this, RDMA_PS_TCP));
  TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
  TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */

  port = ntohs(rdma_get_src_port(listener));
  manager->log(VERB_VITAL,"server: listening on port %d.\n", port);

  is_ready = true;

  int rval;
  do {
    rval = do_event_loop();
  } while(!rval);

  manager->log(VERB_ERROR,"%s terminating because rgce returned %d.\n","server",errno);

  rdma_destroy_id(listener);

  if (logging)
    set_logging(false,NULL);

  return 0;
}

int Server::do_event_loop() {
  struct rdma_cm_event *event = NULL;
  int rval;

  if (rval = manager->poll_cq(0))
    return rval;

dead_client_cleanup:
  if (resizing)      // if resizing, then need to be careful.
    return 0;

  std::vector<IBConn*>::iterator it = clients.begin();
  for(; it != clients.end(); it++) {
    if (CONN_FINIS == (*it)->is_connected() && 0 == (*it)->refcount) {
      manager->log(VERB_INFO,"Destroying finished connection %p\n",(*it));
      (*it)->destroy_connection();
      clients.erase(it);
      it--;    // counteract the coming it++;
      manager->log(VERB_VITAL,"Client disconnected: %zu clients active.\n", clients.size());
      continue;
    }
  }
  return 0;
}

int Server::on_connect_request(struct rdma_cm_id *id) {
  struct rdma_conn_param cm_params;

  if (resizing || !is_ready) {
    manager->log(VERB_INFO,"server: received connection request, rejecting\n");
    const char* action = "retry";

    IBConn::reject(id,action);

    return 0;
  }

  IBConn* thisconn = manager->new_conn(MSG_BUF_SIZE);

  manager->log(VERB_INFO,"server: received connection request, accepting as %p [had %zu clients]\n",thisconn,clients.size());

  id->context = thisconn;
  if (thisconn->build_connection(id)) {
    manager->log(VERB_ERROR,"server: failed to create connection\n");
    thisconn->disconnect();
    thisconn->disconnect();
    return 0;
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

void Server::hook_ibv_recv(int type, struct message* msg_, size_t len, IBConn* conn, void* context) {
  Server* myself = (Server*)context;
  unsigned int startepoch = myself->epoch;

  struct dht_message* msg = (struct dht_message*)msg_;

  // If resizing, this is a trailing message that arrived before we could tear down the connection
  // to the client(s). We shall just discard it silently.
  if (myself->resizing) {
    myself->manager->log(VERB_INFO,"Discarding ibv packet that arrived during resize\n");
    return;
  }

  if (type == MSG_DHT_PUT) {
    char * kptr = sizeof(uint64_t)+(char*)(&(msg->data.put.body));
    char * vptr = sizeof(uint64_t)+msg->data.put.key_len+(char*)(&(msg->data.put.body));
    //myself->manager->log(VERB_VITAL,"MSG_DHT_PUT: key:%s value:%s crc:%d key_len:%lu val_len:%lu\n", binaryToString(kptr, msg->data.put.key_len).c_str(),
    //                     binaryToString(vptr, msg->data.put.val_len).c_str(), *(uint64_t*)(&(msg->data.put.body)), msg->data.put.key_len, msg->data.put.val_len);
    myself->dht.update(kptr, msg->data.put.key_len, vptr, msg->data.put.val_len,
                       *(uint64_t*)(&(msg->data.put.body)), true);

    if (startepoch == myself->epoch) {
      conn->send_message_ext(MSG_DHT_PUT_DONE,(char*)(&(msg->data.req)),sizeof(struct kv_req));
    }

    if (myself->logging) {
      int key_len = msg->data.put.key_len;
      int val_len = msg->data.put.val_len;

      // Write the log header
      myself->log_buf[myself->log_offset] = 'P';
      memcpy(myself->log_buf+myself->log_offset+sizeof(char),            &key_len,sizeof(int));
      memcpy(myself->log_buf+myself->log_offset+sizeof(char)+sizeof(int),&val_len,sizeof(int));
      myself->log_offset += sizeof(char)+sizeof(int)+sizeof(int);

      // Write the log body
      memcpy(myself->log_buf+myself->log_offset, kptr, key_len);
      memcpy(myself->log_buf+myself->log_offset+key_len, vptr, val_len);
      myself->log_offset += key_len+val_len;

      myself->log_flush();
    }
  } else if (type == MSG_DHT_DELETE) {
    myself->dht.remove((char*)(&(msg->data.put.body)), msg->data.put.key_len);

    if (startepoch == myself->epoch) {
      conn->send_message_ext(MSG_DHT_DELETE_DONE,(char*)(&(msg->data.req)),sizeof(struct kv_req));
    }

    if (myself->logging) {
      int key_len = msg->data.put.key_len;

      // Write the log header
      myself->log_buf[myself->log_offset] = 'R';
      memcpy(myself->log_buf+myself->log_offset+sizeof(char),&key_len,sizeof(int));
      myself->log_offset += sizeof(char)+sizeof(int);

      // Write the log body
      memcpy(myself->log_buf+myself->log_offset,(size_t)sizeof(uint64_t)+(char*)((&(msg->data.put.body))),key_len);
      myself->log_offset += key_len;

      myself->log_flush();
    }
  } else if (type == MSG_DHT_GET || type == MSG_DHT_CONTAINS) {
    KEY_TYPE key = (KEY_TYPE)&(msg->data.put.body);
    size_t key_len = msg->data.put.key_len;

    size_t hash_idx = 0;
    int rval = 0, result, msg_len;
    VAL_TYPE value = 0;
    struct dht_message* outmsg = (struct dht_message*)conn->get_send_buf();

re_server_read:
    void* addr = myself->dhtclient->pre_get(key,key_len,hash_idx);
    DHT<KEY_TYPE,VAL_TYPE>::dht_block* dhtb = (DHT<KEY_TYPE,VAL_TYPE>::dht_block*)addr;

    // Figure out if this is the right thing, or what.
    if (type == MSG_DHT_GET)
      result = myself->dhtclient->post_get(dhtb,key,key_len,value,false);
    else //type == MSG_DHT_CONTAINS
      result = myself->dhtclient->post_contains(dhtb,key,key_len,true);


    if (result == POST_GET_MISSING) {
      if (hash_idx == CUCKOO_D-1) {
        rval = (type == MSG_DHT_CONTAINS)?0:result;
        *(char*)&(outmsg->data.put.body) = '\0';
        msg_len = 0;
      } else {
        hash_idx++;
        goto re_server_read;
      }

    } else {

      if (result == POST_GET_FOUND) {

        if (type == MSG_DHT_GET) {
          if (POST_GET_COLLISION == myself->dhtclient->post_get_extents(dhtb,key,key_len,true)) {
            if (hash_idx == CUCKOO_D-1) {
              rval = POST_GET_MISSING;
              *(char*)&(msg->data.put.body) = '\0';
              msg_len = 0;
            } else {
              hash_idx++;
              goto re_server_read;
            }
          } else {
            rval = POST_GET_FOUND;
            msg_len = dhtb->d.val_len;
/*
			myself->manager->log(VERB_ERROR,"target %p src %p val %zu\n",(char*)&(outmsg->data.valresp),dhtb->d.value,msg_len);
            if (msg_len > MSG_BUF_SIZE-sizeof(outmsg->type)) {
              myself->manager->log(VERB_ERROR,"Can't return K-V response with key '%s' and vallen=%zu\n",key,msg_len);
              myself->manager->log(VERB_ERROR,"Values is '%s'\n",value);
              die("");
            } else {
*/
            memcpy((char*)&(outmsg->data.valresp), &(dhtb->d.val_len), sizeof(size_t));
            memcpy((char*)(&(outmsg->data.valresp)+sizeof(size_t)),dhtb->d.value,msg_len);
            msg_len += sizeof(size_t);
          }
        }

      } else { //LOCKED - this should be IMPOSSIBLE!
        //locked_reread_count++;
        diewithcode("Key locked. Key may not be locked when the server reads it!",result);
      }
    }


    // Send response to client
    if (startepoch == myself->epoch) {
      if (type == MSG_DHT_GET) {
        conn->send_message_ext((rval == POST_GET_MISSING)?MSG_DHT_GET_DONE_MISSING:MSG_DHT_GET_DONE,
                               (char*)(&(outmsg->data)),msg_len);
      } else {
        outmsg->data.statusval = (char)rval;

        conn->send_message_ext(MSG_DHT_CONTAINS_DONE,(char*)(&(outmsg->data)),
                               sizeof(outmsg->data.statusval));
      }
    }

  } else {
    myself->manager->log(VERB_ERROR,"Warning: Unknown verb message type %d\n",type);

  }
}

int Server::on_connection(struct rdma_cm_id *id)
{
  ((IBConn*)id->context)->set_recv_hook(Server::hook_ibv_recv,(void*)this);
  ((IBConn*)id->context)->on_connect();
  manager->log(VERB_DEBUG,"Connection %p established into state %d.\n",
               id->context,((IBConn*)id->context)->s_conn->connected);
  manager->log(VERB_VITAL,"New client connected: %zu clients active.\n", clients.size());

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

int Server::on_disconnect(struct rdma_cm_id *id) {

  if (CONN_FINIS > ((IBConn*)id->context)->is_connected()) {
    manager->log(VERB_DEBUG,"Finalizing disconnection.\n");
    ((IBConn*)id->context)->disconnect();
    ((IBConn*)id->context)->disconnect();
  }

  return 0;
}

int Server::on_event(struct rdma_cm_event *event, void* ec_context, void* event_context)
{
  int r = 0;
  Server* server = (Server*)ec_context;

  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
    r = server->on_connect_request(event->id);
  } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
    r = server->on_connection(event->id);
  } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
    r = server->on_disconnect(event->id);
  } else if (event->event == RDMA_CM_EVENT_UNREACHABLE ||
             event->event == RDMA_CM_EVENT_REJECTED) {
    server->manager->log(VERB_ERROR,
                         "server: Endpoint %p rejected connection (?).\n",
                         event->id->context);
    //in some cases the context is us (the Server), not an IBConn
    if (event->id->context && server != event->id->context) {
      ((IBConn*)event->id->context)->disconnect();
      ((IBConn*)event->id->context)->disconnect();
    } else {
      server->manager->log(VERB_ERROR,"server: On rejection, no connection to terminate\n");
    }
    //die("");
    //r = on_disconnect(event->id);
  } else if (event->event == RDMA_CM_EVENT_TIMEWAIT_EXIT) {
    server->manager->log(VERB_DEBUG,"timewait_exit confirmed\n");
  } else if (event->event == RDMA_CM_EVENT_CONNECT_ERROR) {
    server->manager->log(VERB_ERROR,"server: Connection handshake encountered an error\n");
    if (event->id->context) {
      ((IBConn*)event->id->context)->disconnect();
      ((IBConn*)event->id->context)->disconnect();
    } else {
      server->manager->log(VERB_ERROR,"server: On connection error, no connection to terminate\n");
    }
  } else {
    diewithcode("server: on_event: unknown event.",event->event);
  }

  return r;
}


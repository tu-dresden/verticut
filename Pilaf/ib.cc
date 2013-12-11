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
 *   ib.cc: Re-usable Infiniband connection    *
 *          class for storage sytstems.        *
 ***********************************************/

#include "ib.h"
#include "ibman.h"
#include <errno.h>
#include <fcntl.h>
#include <math.h>

void die(const char *reason) { diewithcode(reason,9999); }
void diewithcode(const char *reason, int code)
{
  if (code == 9999)
    fprintf(stderr, "%s\n", reason);
  else
    fprintf(stderr, "%s: code %d\n", reason, code);
  (*(char*)0) = 'a'; //generate segfault
  exit(EXIT_FAILURE);
}

IBConn::IBConn(IBConnManager* manager, size_t msgbuf_size) {

  manager_ = manager;

  s_ctx = NULL;
  s_conn = NULL;

  on_recv_hook = NULL;
  recv_hook_context = NULL;

  on_rdma_recv_hook = NULL;
  rdma_recv_hook_context = NULL;

  on_ready_hook = NULL;
  ready_hook_context = NULL;

  on_mr_hook = NULL;
  mr_hook_context = NULL;

  on_send_complete_hook = NULL;
  send_complete_hook_context = NULL;

  send_buf_size = MAX(msgbuf_size,MAX(sizeof(struct message),sizeof(struct mrmessage)));
  recv_buf_size = send_buf_size;

  // This is a recv_buf for on_completion
  send_buf = NULL;

  // Set when connection is established and MRs exchanged
  ready = 0;

  // From build_connection
  s_conn = (struct connection *)malloc(sizeof(struct connection));
  built = false;
  refcount = 0;

  s_conn->send_state = SS_INIT;
  s_conn->recv_state = RS_INIT;

  s_conn->send_nonce = 0;
  s_conn->recv_nonce = 0;

  s_conn->connected     = CONN_DISCO;
  s_conn->disconnecting = 0;

  s_conn->local_mrs = NULL;
  s_conn->sending_mrs = NULL;
  s_conn->n_local_mrs = 0;
}

int IBConn::build_connection(struct rdma_cm_id *id) {
  struct connection *conn = s_conn;
  struct ibv_qp_init_attr qp_attr;
  int rval;

  if (rval = build_context(id)) {
    manager_->log(VERB_ERROR,"build_context() failed\n");
    return rval;
  }
  if (rval = build_qp_attr(&qp_attr)) {
    manager_->log(VERB_ERROR,"build_context() failed\n");
    return rval;
  }

  // manager_->gpd is either NULL or valid
  if (0 != (rval = rdma_create_qp(id, manager_->gpd, &qp_attr))) {
    manager_->log(VERB_ERROR,"rdma_create_qp in build_connection failed with code %d\n",errno);
    return -1;
  }

  s_ctx->pd = id->qp->pd;   // Use a global-shared per-HCA PD so that MRs can be shared.
  if (manager_ && manager_->gpd == NULL)
    manager_->gpd = id->qp->pd;

  conn->id = id;
  conn->qp = id->qp;

  register_memory(conn);

  for(int i=0; i<RECV_BUFS; i++) {
    struct recv_buf* this_buf = recv_bufs[i];
    recv_pend_post.push(this_buf);
    manager_->log(VERB_DEBUG,"Pushed %d/%d bufs onto pend_post queue\n",i+1,RECV_BUFS);
  }

  post_receives(conn);

  built = true;
  return 0;
}

int IBConn::build_context(struct rdma_cm_id *id) {
  int rval;

  if (s_ctx) {
    if (s_ctx->ctx != id->verbs)
      die("cannot handle events in more than one context.");
    manager_->log(VERB_WARN, "Warning: not (re)building context for this connection.\n");
    return 0;
  }

  s_ctx = (struct context *)malloc(sizeof(struct context));

  s_ctx->ctx = id->verbs;

  // Create a completion queue, if needed, otherwise use shared CQ.
  if (manager_->gcq == NULL) {
    if (NULL == (s_ctx->cq = ibv_create_cq(s_ctx->ctx, 256, NULL, NULL, 0))) { /* cqe=10 is arbitrary */
      manager_->log(VERB_ERROR,"Failed to create completion queue\n");
      return errno;
    }
    manager_->gcq = s_ctx->cq;
  } else {
    s_ctx->cq = manager_->gcq;
  }

  if (rval = ibv_req_notify_cq(s_ctx->cq, 0)) {
    return rval;
  }

  return 0;
}

void IBConn::destroy_context(void) {

  if (s_ctx && s_ctx->cq)
    TEST_Z(ibv_destroy_cq(s_ctx->cq));
  if (s_ctx) {
    free(s_ctx);
    s_ctx = NULL;
  }
}

void IBConn::build_params(struct rdma_conn_param *params) {
  memset(params, 0, sizeof(*params));

  params->initiator_depth = params->responder_resources = 1;
  params->rnr_retry_count = 7; /* infinite retry */
}

int IBConn::build_qp_attr(struct ibv_qp_init_attr *qp_attr) {
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC;
  qp_attr->sq_sig_all = 1;

  qp_attr->cap.max_send_wr = 10;
  qp_attr->cap.max_recv_wr = 10;
  qp_attr->cap.max_send_sge = 1;
  qp_attr->cap.max_recv_sge = 1;
  qp_attr->cap.max_inline_data = MAX_INLINE_SEND;

  qp_attr->qp_context = this;

  return 0;
}

void IBConn::destroy_connection() {

  if (!built)
    return;

  destroy_context();

  rdma_destroy_qp(s_conn->id);

  ibv_dereg_mr(s_conn->send_mr);

  for(int i=0; i<recv_bufs.size(); i++) {
    ibv_dereg_mr(recv_bufs[i]->mr);
    free(recv_bufs[i]->msg);
    free(recv_bufs[i]);
  }
  recv_bufs.clear();

  for(struct mr_chain_node* node = s_conn->local_mrs; node != NULL; ) {
    struct mr_chain_node* next = node->next;
    if (node->location == MR_LOC_LOCAL) {
      ibv_dereg_mr(node->mr);
    }
    free(node);
    node = next;
  }
  s_conn->local_mrs = NULL;

  free(s_conn->send_msg);
  free(send_buf);

  rdma_destroy_id(s_conn->id);

  free(s_conn);

  built = false;
}

void IBConn::on_completion(struct ibv_wc *wc) {

  if (wc->status != IBV_WC_SUCCESS && (s_conn->disconnecting ||
                                       s_conn->connected > CONN_READY)
     )
  {
    manager_->log(VERB_INFO,"on_completion got bad status during disconnection\n");
    // OK to get non-successes during disconnection
    return;
  }
  if ((wc->status != IBV_WC_SUCCESS) && (s_role == R_SERVER)) {
    manager_->log(VERB_ERROR,"on_completion: status is not IBV_WC_SUCCESS "
                  "(status=%d, opcode=%d, ve=%d). Disconnecting client.\n",
                  wc->status,wc->opcode,wc->vendor_err);
    disconnect();
    return;
  } else if (wc->status != IBV_WC_SUCCESS) {
    manager_->log(VERB_ERROR,"on_completion: status is not IBV_WC_SUCCESS "
                  "(status=%d, opcode=%d, ve=%d). Breaking connection.\n",
                  wc->status,wc->opcode,wc->vendor_err);
    disconnect();
    return;
  } //else printf("on_completion for op %d status %d id %p\n",wc->opcode,wc->status,(void*)wc->wr_id);

/*
  manager_->log(VERB_DEBUG,"Received on_completion for opcode %d with wr_id=%p instance %p\n",
                wc->opcode,wc->wr_id,((struct recv_buf*)wc->wr_id)->instance);
*/

  struct recv_buf* this_buf = (struct recv_buf*)(wc->wr_id);
  if (this_buf->instance != this)
    die("Critical wr_id instance mismatch in on_completion: completion is not for this connection");

  IBC_INCREF();

  // Packet numbers
  if (wc->opcode & IBV_WC_RECV)
    s_conn->recv_nonce++;
  else if (wc->opcode == IBV_WC_SEND)
    s_conn->send_nonce++;

  // Deal with the completion
  if (wc->opcode & IBV_WC_RECV) {

    if (((struct message*)(this_buf->msg))/*(s_conn->recv_msg))*/->type == MSG_MR) {
      struct mrmessage* mrmsg = ((struct mrmessage*)(this_buf->msg));

      manager_->log(VERB_INFO,"%s: Received MR (message region) information in connection %p.\n",
                    role_to_str(s_role),this);

      if (s_conn->connected != CONN_SETUP) {
        manager_->log(VERB_ERROR,"MR received, but connection %p in %d"
                      " state rather than CONN_SETUP state [discarding]\n",this,s_conn->connected);
        IBC_DECREF();
        return;
      }

      bool last_mr = mrmsg->last;

      // If addr is NULL, this is a dummy
      if (mrmsg->mr.addr != NULL) {
        manager_->set_mr(((mrmsg->location == MR_LOC_LOCAL)?MR_LOC_REMOTE:MR_LOC_LOCAL),	// local mrs on the other side are remote here,
                         MR_SCOPE_LOCAL,													// and vice-versa
                         &(mrmsg->mr),
                         mrmsg->mr_id,
                         this);
      } else {
        manager_->log(VERB_WARN,"Warning: peer claimed it has an empty list of local MRs\n");
      }

      // Wait until all MRs are received before sending ours back
      if (last_mr) {
        s_conn->recv_state++;

        manager_->log(VERB_DEBUG,"States are now send=%d, recv=%d\n",
                      s_conn->send_state, s_conn->recv_state);

        if (s_conn->send_state == SS_MR_SENT && s_conn->recv_state >= RS_MR_RECV) {
          manager_->log(VERB_INFO,"%s: setup complete, connection %p entering data mode %d.\n",
                        role_to_str(s_role),this,s_conn->send_state);
          s_conn->connected = CONN_READY;
          if (on_ready_hook && s_conn->send_state >= SS_MR_SENT) {
            on_ready_hook(ready_hook_context);
          }
          ready = 1;
        }
      }

      if (last_mr) {
        if (s_conn->send_state == SS_INIT) { // received peer's MR before sending ours, so send ours back

          manager_->log(VERB_INFO,"%s: Returning MR to peer in conn state %d\n",
                        role_to_str(s_role),s_conn->connected);

          if (send_mr()) {
            manager_->log(VERB_WARN,"send_mr() setup failed.\n");
          }
        }
      }

    } else { //tell our hooked app about it

      s_conn->recv_state++;

      if (on_recv_hook)
        on_recv_hook(this_buf->msg->type,this_buf->msg,wc->byte_len,this,recv_hook_context);

    }

    recv_pend_post.push(this_buf);
    post_receives(s_conn); // done with buffer, now others can use it.


  } else if ((wc->opcode == IBV_WC_RDMA_READ | wc->opcode == IBV_WC_RDMA_WRITE) && s_conn->send_state >= SS_MR_SENT && s_conn->recv_state >= RS_MR_RECV) {
    if (on_rdma_recv_hook) {

#if KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP
      on_rdma_recv_hook(this_buf->msg->type,this,recv_hook_context);
#else

      on_rdma_recv_hook(this_buf->msg->type,this,recv_hook_context);
#endif

    }

  } else if (wc->opcode == IBV_WC_SEND) {
    if (s_conn->send_state == SS_MR_SENDING) {

      if (s_conn->sending_mrs == NULL) {
        // all MRs sent

        manager_->log(VERB_INFO,"Done sending MRs to peer\n");

        // Trigger the ready hook, if necessary
        s_conn->send_state++;			//SS_MR_SENT

        if (s_conn->send_state == SS_MR_SENT && s_conn->recv_state >= RS_MR_RECV) {
          manager_->log(VERB_INFO,"%s: setup complete, connection %p entering data mode %d.\n",
                        role_to_str(s_role),this,s_conn->send_state);

          s_conn->connected = CONN_READY;
          if (on_ready_hook) {
            on_ready_hook(ready_hook_context);
          }
          ready = 1;
        }
      } else {
        // more MRs to send
        send_head_mr();
      }
    } else if (s_conn->send_state >= SS_MR_SENT) {
      if (on_send_complete_hook) {
        on_send_complete_hook(send_complete_hook_context);
      }
    }
  }

  IBC_DECREF();
  return;
}

int IBConn::rdma_push(uintptr_t addr, size_t length, uintptr_t local_addr, struct ibv_mr* remote_mr, ibv_mr* local_mr) {
  static struct ibv_send_wr wr, *bad_wr = NULL;
  static struct ibv_sge sge;

  if ((void*)addr < remote_mr->addr ||
      (void*)((char*)addr+length) > (void*)((char*)remote_mr->addr + remote_mr->length))
  {
    manager_->log(VERB_ERROR,
                  "Caught and prevented invalid memory access at %p of size %zu\n",
                  (void*)addr,length);
    manager_->log(VERB_ERROR,
                  "Valid for this MR are %p-%p\n",remote_mr->addr,
                  (void*)((char*)remote_mr->addr+remote_mr->length));
    exit(-1);
  }
  wr.wr_id = (uintptr_t)(send_buf);
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.rkey = remote_mr->rkey;
  sge.addr = (uintptr_t)(local_mr->addr);
  sge.lkey = local_mr->lkey;

  wr.wr.rdma.remote_addr = (uintptr_t)addr;
  sge.length = length;

  return ibv_post_send(s_conn->qp, &wr, &bad_wr);

}

uint64_t pilaf_n_rdma_read = 0;

int IBConn::rdma_fetch(uintptr_t addr, size_t length, struct ibv_mr* remote_mr, ibv_mr* local_mr) {
  // XXX TODO Solve the issue with segfault on the first ibv_post_send after
  // a reconnect, which I suspect is due to it trying to clean up the last failed
  // rdma_fetch. Make freeing of s_conn structure get deferred until after the first ibv_post_send
  // of the new connection.
  static struct ibv_send_wr wr, *bad_wr = NULL;
  static struct ibv_sge sge;
//  struct ibv_send_wr* bad_wr = NULL;
  pilaf_n_rdma_read++;

  if ((void*)addr < remote_mr->addr ||
      (void*)((char*)addr+length) > (void*)((char*)remote_mr->addr + remote_mr->length))
  {
    manager_->log(VERB_ERROR,
                  "Caught and prevented invalid memory access at %p of size %zu\n",
                  (void*)addr,length);
    manager_->log(VERB_ERROR,
                  "Valid for this MR are %p-%p\n",remote_mr->addr,
                  (void*)((char*)remote_mr->addr+remote_mr->length));
    exit(-1);
  }

/*
  s_conn->ts_wr_rf.wr_id = (uintptr_t)(send_buf);
  s_conn->ts_wr_rf.opcode = IBV_WR_RDMA_READ;
  s_conn->ts_wr_rf.sg_list = &(s_conn->ts_sge_rf);
  s_conn->ts_wr_rf.num_sge = 1;
  s_conn->ts_wr_rf.send_flags = IBV_SEND_SIGNALED;
  s_conn->ts_wr_rf.wr.rdma.rkey = remote_mr->rkey;
  s_conn->ts_sge_rf.addr = (uintptr_t)(local_mr->addr);
  s_conn->ts_sge_rf.lkey = local_mr->lkey;

  s_conn->ts_wr_rf.wr.rdma.remote_addr = (uintptr_t)addr;
  s_conn->ts_sge_rf.length = length;

  return ibv_post_send(s_conn->qp, &(s_conn->ts_wr_rf), &bad_wr);  //bad_wr will essentially get thrown out; no biggie
*/
  wr.wr_id = (uintptr_t)(send_buf);
  wr.opcode = IBV_WR_RDMA_READ;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.rkey = remote_mr->rkey;
  sge.addr = (uintptr_t)(local_mr->addr);
  sge.lkey = local_mr->lkey;

  wr.wr.rdma.remote_addr = (uintptr_t)addr;
  sge.length = length;

  return ibv_post_send(s_conn->qp, &wr, &bad_wr);

}

void IBConn::send_message_ext(int type, char* data, size_t data_len) {

  // Copy the data to the proper buffer, if necessary
  if (data != (char*)&(s_conn->send_msg->mdata)) {
    memcpy((char*)&(s_conn->send_msg->mdata),data,data_len);
  }

  // Set the message type and send it
  s_conn->send_msg->type = type;
  send_message(s_conn,sizeof(s_conn->send_msg->type)+data_len);
}

void IBConn::disconnect() {
  manager_->log(VERB_DEBUG,"Disconnect requested in state c=%d, d=%d\n",
                s_conn->connected,s_conn->disconnecting);

  if (s_conn->connected >= CONN_SETUP && s_conn->connected < CONN_TEARD && s_conn->disconnecting == 0) {
    s_conn->disconnecting = 1;
    s_conn->connected = CONN_TEARD;
    if (rdma_disconnect(s_conn->id)) {
      manager_->log(VERB_WARN,"Warning: connection was not fit for rdma_disconnect() in disconnect()\n");
    }

  } else if (s_conn->connected == CONN_TEARD && s_conn->disconnecting == 1) {
    s_conn->disconnecting = 0;
    s_conn->connected = CONN_FINIS;
  } else if (s_conn->connected == CONN_FINIS) {
    manager_->log(VERB_INFO,"Notice: connection already in state CONN_FINIS in disconnect()\n");
    return;
  } else {
    manager_->log(VERB_WARN,"Warning: Invalid state (c=%d, d=%d) in disconnect()\n",
                  s_conn->connected,s_conn->disconnecting);
    s_conn->connected = CONN_FINIS;
    s_conn->disconnecting = 0;
  }

}

void IBConn::reject(rdma_cm_id* id, const char* imm = NULL) {
  if (rdma_reject(id, imm, (NULL==imm)?0:1+strlen(imm))) {
    diewithcode("rdma_reject failed",errno);
  }
}

void IBConn::on_connect() {
  s_conn->connected = CONN_SETUP;
}

void IBConn::post_receives(struct connection *conn) {
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  while(!recv_pend_post.empty()) {

    struct recv_buf* thispend = recv_pend_post.front();

    wr.wr_id = (uintptr_t)thispend;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t)thispend->msg;
    sge.length = recv_buf_size;
    sge.lkey = thispend->mr->lkey;

    int rv = ibv_post_recv(conn->qp, &wr, &bad_wr);
    if (rv) diewithcode("Failed to ibv_post_recv",rv);

    recv_pend_post.pop();
  }
}

void IBConn::register_memory(struct connection *conn) {


  for (int i=0; i<RECV_BUFS; i++) {
    struct recv_buf* thisbuf = (struct recv_buf*)malloc(sizeof(struct recv_buf));
    struct message* recv_msg = (struct message*)malloc(recv_buf_size);
    struct ibv_mr* recv_mr = NULL;

    if (thisbuf == NULL || recv_msg == NULL) {
      manager_->log(VERB_ERROR,"Ran out of memory registering receive buffers\n");
      die("");
    }

    if (NULL == (recv_mr = ibv_reg_mr(
      manager_->gpd,
      recv_msg,
      recv_buf_size,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ)))
    {
      manager_->log(VERB_ERROR,"Failed to register recv_mr memory region at %p, size %zu\n",
                    recv_msg, recv_buf_size);
      diewithcode("Failed with code",errno);
    }

    thisbuf->msg = recv_msg;
    thisbuf->mr = recv_mr;
    thisbuf->instance = this;

    recv_bufs.push_back(thisbuf);
    manager_->log(VERB_DEBUG,"Created recv buffer %3d/%3d with msg=%p "
                  "and mr=%p in conn this=%p\n",i+1,RECV_BUFS,recv_msg,
                  recv_mr, this);
  }


  send_buf = (struct recv_buf*)malloc(sizeof(struct recv_buf));
  conn->send_msg = (struct message*)malloc(send_buf_size);

  if (NULL == (conn->send_mr = ibv_reg_mr(
    manager_->gpd,
    conn->send_msg,
    send_buf_size,
    IBV_ACCESS_LOCAL_WRITE)))
  {
    manager_->log(VERB_ERROR,"Failed to register send_mr memory region at %p, size %zu\n",
                  conn->send_msg, send_buf_size);
    diewithcode("Failed with code",errno);
  }

  send_buf->msg = conn->send_msg;
  send_buf->mr = conn->send_mr;
  send_buf->instance = this;

}

void IBConn::send_message(struct connection *conn, int msg_size) {
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  int rval;

  post_receives(conn);

  if (conn->connected < CONN_SETUP || conn->connected > CONN_READY) {
    manager_->log(VERB_ERROR,"Error: connection cannot send_message in state %d\n",
                  conn->connected);
  }

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)(send_buf);
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  if (msg_size < MAX_INLINE_SEND) {
    wr.send_flags |= IBV_SEND_INLINE;
  }

  sge.addr = (uintptr_t)conn->send_msg;
  sge.length = msg_size;
  sge.lkey = conn->send_mr->lkey;

  while (!conn->connected);

  if (rval = (ibv_post_send(conn->qp, &wr, &bad_wr))) {
    manager_->log(VERB_ERROR,"Error: unable to ibv_post_send() with error %d and bad_wr %p\n",errno,bad_wr);
    die("");
  }
}

int IBConn::send_head_mr() {
  struct mr_chain_node* node = s_conn->sending_mrs;
  struct mrmessage* mrmsg = (struct mrmessage*)(s_conn->send_msg);

  mrmsg->type = MSG_MR;
  mrmsg->scope = node->scope;
  mrmsg->location = node->location;
  mrmsg->mr_id = node->mr_id;
  memcpy(&(mrmsg->mr),node->mr,sizeof(struct ibv_mr));
  mrmsg->last = (node->next == NULL);

  manager_->log(VERB_DEBUG,"Sending MR id=%d, loc=%s, scope=%s, len %zu @ %p\n",
                node->mr_id,(node->location==MR_LOC_LOCAL)?"local":"remote",
                (node->scope==MR_SCOPE_LOCAL)?"local":"global",
                node->mr->length,node->mr->addr);

  s_conn->sending_mrs = node->next;
  free(node);

  send_message(s_conn,sizeof(struct mrmessage));
}

// Tell peer about my memory region
int IBConn::send_mr() {

  IBC_INCREF();

  struct mr_chain_node* node;
  struct mr_chain_node* tmpnode = NULL;
  int sendcount = 0;
  int allcount = 0;

  if (s_conn->connected == CONN_DISCO)
    s_conn->connected = CONN_SETUP;

  // Send over all the global MRs
  node = manager_->global_mrs;
  while (node != NULL) {
    if (node->location == MR_LOC_LOCAL) {

      manager_->log(VERB_INFO,"Queueing global MR @ %p size %zu to peer\n",
                    node->mr->addr,node->mr->length);
      sendcount++;

      tmpnode = (struct mr_chain_node*)malloc(sizeof(struct mr_chain_node));
      if (tmpnode == NULL)
        die("Memory exhausted reserving chain_node in global portion of send_mr");

      // Copy data into tmpnode and insert tmpnode as head node of sending_mrs chain
      memcpy(tmpnode,node,sizeof(struct mr_chain_node));
      tmpnode->next = s_conn->sending_mrs;
      s_conn->sending_mrs = tmpnode;
    }
    node = node->next;
  }

  // Send over all the local MRs
  node = s_conn->local_mrs;
  while (node != NULL) {
    if (node->location == MR_LOC_LOCAL) {

      manager_->log(VERB_INFO,"Queueing local MR @ %p size %zu to peer\n",
                    node->mr->addr,node->mr->length);
      sendcount++;

      tmpnode = (struct mr_chain_node*)malloc(sizeof(struct mr_chain_node));
      if (tmpnode == NULL)
        die("Memory exhausted reserving chain_node in global portion of send_mr");

      // Copy data into tmpnode and insert tmpnode as head node of sending_mrs chain
      memcpy(tmpnode,node,sizeof(struct mr_chain_node));
      tmpnode->next = s_conn->sending_mrs;
      s_conn->sending_mrs = tmpnode;
    }
    node = node->next;
  }

  s_conn->send_state++;
  manager_->log(VERB_INFO,"Will be sending %d MRs to peer\n",sendcount);

  // Corner case for zero MRs
  if (sendcount == 0) {
    manager_->log(VERB_WARN,"Warning: telling peer about zero MRs. Problem?\n");

    // Tell the peer this is a flag MR
    struct ibv_mr tempmr;
    tempmr.addr = NULL;
    tempmr.length = 0;

    struct mrmessage* mrmsg = (struct mrmessage*)(s_conn->send_msg);
    mrmsg->type = MSG_MR;
    mrmsg->scope = MR_SCOPE_LOCAL;
    mrmsg->location = MR_LOC_LOCAL;
    mrmsg->mr_id = -1;
    memcpy(&(mrmsg->mr),&tempmr,sizeof(struct ibv_mr));
    mrmsg->last = true;

    send_message(s_conn,sizeof(struct mrmessage));

  } else {
    // Normal kick-start to sending
    send_head_mr();
  }

  IBC_DECREF();
  return 0;

}

void IBConn::set_recv_hook(void(*recv_hook)(int,struct message*,size_t,IBConn*,void*), void* recv_context) {
  on_recv_hook = recv_hook;
  recv_hook_context = recv_context;
}

void IBConn::set_ready_hook(void(*ready_hook)( void*), void* ready_context) {
  on_ready_hook = ready_hook;
  ready_hook_context = ready_context;
}

void IBConn::set_rdma_recv_hook(void(*rdma_recv_hook)(int,IBConn*,void*),
  void* rdma_recv_context)
{
  on_rdma_recv_hook = rdma_recv_hook;
  rdma_recv_hook_context = rdma_recv_context;
}

void IBConn::set_mr_hook(void(*mr_hook)(int* status, struct ibv_mr* mr, int* mr_id, void*),
  void* mr_context)
{
  on_mr_hook = mr_hook;
  mr_hook_context = mr_context;
}

void IBConn::set_send_complete_hook(void(*send_complete_hook)(void*),
  void* send_complete_context)
{
  on_send_complete_hook = send_complete_hook;
  send_complete_hook_context = send_complete_context;
}

void IBConn::set_role(enum role r) {
  s_role = r;
}

const char* role_to_str(int s_role) {
  return (s_role == R_CLIENT)?"client":"server";
}


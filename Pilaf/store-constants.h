
// Moved out of the IB code
enum app_msg_type {

  MSG_DHT_PUT = MSG_USER_FIRST,
  MSG_DHT_PUT_DONE,
  MSG_DHT_DELETE,
  MSG_DHT_DELETE_DONE,

  // Following for server-mediated reads/contains
  MSG_DHT_GET,
  MSG_DHT_GET_DONE_MISSING,
  MSG_DHT_GET_DONE,
  MSG_DHT_CONTAINS,
  MSG_DHT_CONTAINS_DONE,

  // Other types of messages
  MSG_DHT_CAPACITY
};

struct kv_req {
  KEY_TYPE key;
  VAL_TYPE value;
};

#pragma pack(push)
#pragma pack(4)
struct dht_message {
  int type;

  union mdata {
    struct kv_req req;
#if KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP
    struct {
      size_t key_len; 
      size_t val_len;
      char body;
    } put;               // get or put request
    char valresp;        // value response
#elif KEY_VAL_PAIRTYPE==KVPT_SIZET_DOUBLE //added for server-mediated reads
    struct {
      double body;
    } put;
#endif
    char statusval;
  } data;
};
#pragma pack(pop)

enum mr_types {
  MR_TYPE_RECV_BUF,
  MR_TYPE_SEND_BUF,
  MR_TYPE_DHT_TABLE,
  MR_TYPE_DHT_EXTENTS,
  MR_TYPE_RDMA_BUF_TABLE,
  MR_TYPE_RDMA_BUF_EXTENTS
};

#if KEY_VAL_PAIRTYPE==KVPT_SIZET_DOUBLE
const int MSG_BUF_SIZE = sizeof(dht_message);			//It will actually be the max of the sizes of the message and mrmessage structs

#else
const int RECV_EXT_SIZE = (1<<20); /* 16 MB (aka freaking massive, to fit key-val for giant puts) */
const int MSG_BUF_SIZE = MAX(RECV_EXT_SIZE,sizeof(dht_message));

#endif

#define RECONN_ITERS_TIMEOUT (10000000)
#define CONNECT_TIMEOUT (1000000*10) // us
#define CONNECT_RETRY_SLEEP 1000 // us


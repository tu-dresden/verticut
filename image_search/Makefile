.PHONY: clean

PILAF_PATH := ../Pilaf
REDIS_PATH := ../Redis
LDFLAGS := $(LDFLAGS) -lmsgpack -lmsgpack-rpc -lmpio -lrdmacm -libverbs -lrt -lpthread -lprotobuf -lmemcached -lboost_thread-mt
CFLAGS  := -Wno-write-strings -Ofast -rdynamic -I${REDIS_PATH} -I${PILAF_PATH} 
CC      := mpiCC.openmpi

COMMON_SRC := memcached_proxy.h pilaf_proxy.h base_proxy.h image_search_constants.h 
OBJS_PILAF := $(PILAF_PATH)/ib.o $(PILAF_PATH)/ibman.o $(PILAF_PATH)/store-client.o 
OBJS_REDIS := $(REDIS_PATH)/anet.o
COMMON_OBJS := image_search.pb.o args_config.o mpi_coordinator.o $(OBJS_PILAF) $(OBJS_REDIS)

OBJS_IMAGE_BUILD := $(COMMON_OBJS) build_hash_tables.o 
OBJS_IMAGE_LINEAR_SEARCH := $(COMMON_OBJS) linear_search.o 
OBJS_DISTRIBUTED_IMAGE_SEARCH := $(COMMON_OBJS) distributed_image_search.o search_worker.o 
OBJS_INTEGRITY_CHECK := $(COMMON_OBJS) integrity_check.o 
OBJS_IMAGE_SERVER := image_search_server.o image_server_main.o
OBJS_IMAGE_TEST := $(COMMON_OBJS) image_search_client.o image_search_test.o
APPS := build-tables linear-search distributed-image-search integrity-check image-server image-search-test

all: $(APPS) 

%.pb.h %.pb.cc : %.proto
	protoc -I=. $< --cpp_out=./

.cc.o:
	$(CC) $(CFLAGS) $< -c -o $@


build-tables: $(OBJS_IMAGE_BUILD) $(COMMON_SRC)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS) 

linear-search: $(OBJS_IMAGE_LINEAR_SEARCH) $(COMMON_SRC)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

distributed-image-search: $(OBJS_DISTRIBUTED_IMAGE_SEARCH) $(COMMON_SRC)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

integrity-check: $(OBJS_INTEGRITY_CHECK) $(COMMON_SRC)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

image-server: $(OBJS_IMAGE_SERVER) 
	${CC} -o $@ $^ $(LDFLAGS) -lmsgpack -lmsgpack-rpc -lmpio 

image-search-test: $(OBJS_IMAGE_TEST) $(COMMON_SRC)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS) 

clean:
	@rm -f *.o ${APPS}


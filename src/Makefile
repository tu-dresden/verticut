.PHONY: clean

PILAF_PATH := ../Pilaf
REDIS_PATH := ../Redis
LDFLAGS := $(LDFLAGS) -lrt -lmsgpack -lmsgpack-rpc -lmpio -lrdmacm -libverbs -lrt -lpthread -lprotobuf -lmemcached -lboost_system 
CFLAGS  := -Wno-write-strings -Ofast -rdynamic -I${REDIS_PATH} -I${PILAF_PATH} 
CC      := mpiCC.openmpi

COMMON_SRC := memcached_proxy.h pilaf_proxy.h base_proxy.h image_search_constants.h
OBJS_PILAF := $(PILAF_PATH)/ib.o $(PILAF_PATH)/ibman.o $(PILAF_PATH)/store-client.o 
OBJS_REDIS := $(REDIS_PATH)/anet.o
COMMON_OBJS := image_search.pb.o args_config.o mpi_coordinator.o $(OBJS_PILAF) $(OBJS_REDIS)

OBJS_IMAGE_BUILD := $(COMMON_OBJS) build_hash_tables.o 
OBJS_IMAGE_LINEAR_SEARCH := $(COMMON_OBJS) linear_search.o timer.o 
OBJS_DISTRIBUTED_IMAGE_SEARCH := $(COMMON_OBJS) bitmap.o distributed_image_search.o search_worker.o timer.o
OBJS_ACCURACY_TEST := $(COMMON_OBJS) bitmap.o accuracy_test.o search_worker.o timer.o 
OBJS_INTEGRITY_CHECK := $(COMMON_OBJS) integrity_check.o 
OBJS_IMAGE_SERVER := image_search_server.o image_server_main.o
OBJS_IMAGE_TEST := image_search_client.o image_search_test.o
APPS := build-tables linear-search distributed-image-search integrity-check image-server image-search-test generate-bitmap bitmap-deamon accuracy-test

all: $(APPS) 

%.pb.h %.pb.cc : %.proto
	protoc -I=. $< --cpp_out=./

.cc.o:
	$(CC) $(CFLAGS) $< -c -o $@


build-tables: $(OBJS_IMAGE_BUILD) $(COMMON_SRC)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS) 

linear-search: $(OBJS_IMAGE_LINEAR_SEARCH) $(COMMON_SRC)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

distributed-image-search: $(OBJS_DISTRIBUTED_IMAGE_SEARCH) 
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

accuracy-test: $(OBJS_ACCURACY_TEST) 
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

integrity-check: $(OBJS_INTEGRITY_CHECK) $(COMMON_SRC)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

image-server: $(OBJS_IMAGE_SERVER) 
	${CC} -o $@ $^ $(LDFLAGS) -lmsgpack -lmsgpack-rpc -lmpio 

image-search-test: $(OBJS_IMAGE_TEST) 
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS) 

generate-bitmap: generate_bitmap.cc
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS) 

bitmap-deamon: bitmap_deamon.cc
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS) 

clean:
	@rm -f *.o ${APPS}

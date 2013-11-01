.PHONY: clean

PILAF_PATH := ../Pilaf
LDFLAGS := $(LDFLAGS) -lrdmacm -libverbs -lrt -lpthread -lprotobuf -lmemcached
CFLAGS  := -Wno-write-strings -Ofast -rdynamic -I${PILAF_PATH}
CC      := mpiCC.openmpi

PROXY_SRC := memcached_proxy.h pilaf_proxy.h base_proxy.h 
OBJS_PILAF := $(PILAF_PATH)/ib.o $(PILAF_PATH)/ibman.o $(PILAF_PATH)/store-client.o 
OBJS_IMAGE_BUILD :=  $(OBJS_PILAF) image_search.pb.o build_hash_tables.o args_config.o
OBJS_IMAGE_LINEAR_SEARCH := $(OBJS_PILAF) image_search.pb.o linear_search.o args_config.o
OBJS_DISTRIBUTED_IMAGE_SEARCH := $(OBJS_PILAF) image_search.pb.o distributed_image_search.o mpi_coordinator.o search_worker.o 
APPS := image-build-test image-linear-search-test distributed-image-search

all: $(APPS) 

%.pb.h %.pb.cc : %.proto
	protoc -I=. $< --cpp_out=./

.cc.o:
	$(CC) $(CFLAGS) $< -c -o $@

image-build-test: $(OBJS_IMAGE_BUILD) $(PROXY_SRC)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS) 

image-linear-search-test: $(OBJS_IMAGE_LINEAR_SEARCH) $(PROXY_SRC)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

distributed-image-search: $(OBJS_DISTRIBUTED_IMAGE_SEARCH) $(PROXY_SRC)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

clean:
	@rm -f *.o ${APPS}


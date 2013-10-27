.PHONY: clean

PILAF_PATH := ../Pilaf
LDFLAGS := $(LDFLAGS) -lrdmacm -libverbs -lrt -lpthread -lprotobuf
CFLAGS  := -Wno-write-strings -Ofast -g -rdynamic
CC      := g++

OBJS_IMAGE_BUILD := $(PILAF_PATH)/ib.o $(PILAF_PATH)/ibman.o $(PILAF_PATH)/store-client.o image_search.pb.o build_hash_tables.o
OBJS_IMAGE_SEARCH := $(PILAF_PATH)/ib.o $(PILAF_PATH)/ibman.o image_search.pb.o $(PILAF_PATH)/store-client.o search_image.o
OBJS_IMAGE_LINEAR_SEARCH := $(PILAF_PATH)/ib.o $(PILAF_PATH)/ibman.o image_search.pb.o $(PILAF_PATH)/store-client.o linear_search.o
OBJS_IMAGE_GENERATE := generate_binarycode.o
APPS    := image-build-test image-search-test generate_binarycode
#IMAGE_APP := image-build-test image-search-test image-linear-search-test generate_binarycode

all: $(APPS) 

.cc.o:
	$(CC) $(CFLAGS) $< -c -o $@

image-build-test: $(OBJS_IMAGE_BUILD)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

image-search-test: $(OBJS_IMAGE_SEARCH)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

generate_binarycode: $(OBJS_IMAGE_GENERATE)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)


clean:
	@rm -f *.o ${APPS}


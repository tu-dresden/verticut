.PHONY: clean

PILAF_PATH := ../Pilaf
LDFLAGS := $(LDFLAGS) -lrdmacm -libverbs -lrt -lpthread -lprotobuf
CFLAGS  := -Wno-write-strings -Ofast -g -rdynamic
CC      := g++

OBJS_PILAF := $(PILAF_PATH)/ib.o $(PILAF_PATH)/ibman.o $(PILAF_PATH)/store-client.o
OBJS_IMAGE_BUILD :=  $(OBJS_PILAF) image_search.pb.o build_hash_tables.o
OBJS_IMAGE_SEARCH := $(OBJS_PILAF) image_search.pb.o search_image.o
OBJS_IMAGE_LINEAR_SEARCH := $(OBJS_PILAF) image_search.pb.o linear_search.o
OBJS_IMAGE_GENERATE := generate_binarycode.o
APPS    := image-build-test image-search-test generate_binarycode image-linear-search-test

all: $(APPS) 

.cc.o:
	$(CC) $(CFLAGS) $< -c -o $@

image-build-test: $(OBJS_IMAGE_BUILD)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

image-search-test: $(OBJS_IMAGE_SEARCH)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

generate_binarycode: $(OBJS_IMAGE_GENERATE)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

image-linear-search-test: $(OBJS_IMAGE_LINEAR_SEARCH)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

clean:
	@rm -f *.o ${APPS}


.PHONY: clean

#IMAGE_SOURCE_DIR := image_search
CFLAGS  := -Wno-write-strings -Ofast -g -rdynamic
CC      := g++
LDFLAGS := $(LDFLAGS) -lrdmacm -libverbs -lrt -lpthread
OBJS_DT := ib.o ibman.o dht-test.o store-server.o store-client.o
OBJS_LT := ib.o ibman.o loadtest.o store-client.o
OBJS_IB := ib.o ibman.o ib-test.o ib-server.o ib-client.o
OBJS_HT := ht-test.o time/get_clock.o
#OBJS_IMAGE_BUILD := ib.o ibman.o $(IMAGE_SOURCE_DIR)/image_search.pb.o store-client.o $(IMAGE_SOURCE_DIR)/build_hash_tables.o
#OBJS_IMAGE_SEARCH := ib.o ibman.o $(IMAGE_SOURCE_DIR)/image_search.pb.o store-client.o $(IMAGE_SOURCE_DIR)/search_image.o
#OBJS_IMAGE_LINEAR_SEARCH := ib.o ibman.o $(IMAGE_SOURCE_DIR)/image_search.pb.o store-client.o $(IMAGE_SOURCE_DIR)/linear_search.o
#OBJS_IMAGE_GENERATE := $(IMAGE_SOURCE_DIR)/generate_binarycode.o
APPS    := dht-test loadtest ib-test ht-test
#IMAGE_APP := image-build-test image-search-test image-linear-search-test generate_binarycode

all: $(APPS)

.cc.o:
	$(CC) $(CFLAGS) $< -c -o $@

dht-test: $(OBJS_DT)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

loadtest: $(OBJS_LT)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

ib-test: $(OBJS_IB)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

ht-test: $(OBJS_HT)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)


clean:
	#rm -f $(IMAGE_SOURCE_DIR)/*.o ${IMAGE_APP}
	rm -f *.o ${APPS}


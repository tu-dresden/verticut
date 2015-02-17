.PHONY: clean

CFLAGS  := -std=c++11 -Wno-write-strings -Ofast -rdynamic
CC      := g++
LDFLAGS := $(LDFLAGS) -lrdmacm -libverbs -lrt -lpthread
OBJS_DT := ib.o ibman.o dht-test.o store-server.o store-client.o
OBJS_LT := ib.o ibman.o loadtest.o store-client.o
OBJS_IB := ib.o ibman.o ib-test.o ib-server.o ib-client.o
OBJS_HT := ht-test.o time/get_clock.o
APPS    := dht-test loadtest ib-test ht-test

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


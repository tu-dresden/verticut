.PHONY: clean

PILAF_PATH := ../Pilaf
LDFLAGS := $(LDFLAGS) -lrdmacm -libverbs -lrt -lpthread -lprotobuf
CFLAGS  := -Wno-write-strings -Ofast -g -rdynamic
CC      := g++

OBJS_TEST := $(PILAF_PATH)/ib.o $(PILAF_PATH)/ibman.o $(PILAF_PATH)/store-client.o pilaf_test.o
APPS    := pilaf_test

all: $(APPS) 

.cc.o:
	$(CC) $(CFLAGS) $< -c -o $@

pilaf_test: $(OBJS_TEST)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

clean:
	@rm -f *.o ${APPS}


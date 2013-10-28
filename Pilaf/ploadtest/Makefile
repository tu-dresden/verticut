.PHONY: clean

MEMCACHED_HOME=/home/yifeng/lib/libmemcached
REDIS_HOME=/home/yifeng/hiredis

CFLAGS  :=-Wno-write-strings -Ofast 
CXXFLAGS := -Wno-write-strings -Ofast -fPIC
CC      := g++
LDFLAGS := $(LDFLAGS) -lrdmacm -libverbs -lprofiler -lrt
OBJS_PLT := ../ib.o ../ibman.o ploadtest.o ../store-client.o

APPS    := ploadtest
ifeq ($(REDIS),1)
	CXXFLAGS += -I$(REDIS_HOME) -DREDIS=1
	LDFLAGS += -L$(REDIS_HOME) -lhiredis -Wl,-rpath=$(REDIS_HOME) 
else ifeq ($(MEMCACHED),1)
	CXXFLAGS += -I$(MEMCACHED_HOME)/include -DMEMCACHED=1
	LDFLAGS += -L$(MEMCACHED_HOME)/lib -lmemcached -Wl,-rpath=$(MEMCACHED_HOME)/lib 
endif

all: $(APPS)

%.o: %.c
	$(CC) -c -o $@ $< $(CFLAGS)

ploadtest: $(OBJS_PLT)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS) -lrt 

clean:
	rm -f *.o ${APPS}


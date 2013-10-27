.PHONY: clean

PILAF_PATH := ../Pilaf
LDFLAGS := $(LDFLAGS) -lrdmacm -libverbs -lrt -lpthread -lprotobuf
CFLAGS  := -Wno-write-strings -Ofast -g -rdynamic
CC      := mpiCC

OBJS_PILAF := $(PILAF_PATH)/ib.o $(PILAF_PATH)/ibman.o $(PILAF_PATH)/store-client.o
OBJS_TEST :=  $(OBJS_PILAF) pilaf_test.o
OBJS_MPI_TEST := $(OBJS_PILAF) mpi_pilaf_test.o
APPS    := pilaf_test mpi_pilaf_test

all: $(APPS) 

.cc.o:
	$(CC) $(CFLAGS) $< -c -o $@

pilaf_test: $(OBJS_TEST)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

mpi_pilaf_test: $(OBJS_MPI_TEST)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

clean:
	@rm -f *.o ${APPS}


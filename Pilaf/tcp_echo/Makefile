.PHONY: clean

CFLAGS  := -std=c++11 -Wno-write-strings -fpermissive -O3
CC      := g++
LDFLAGS := $(LDFLAGS) -lrt
OBJS_CL := client.o helper.o
OBJS_SR := multi.o helper.o

APPS    := client multi

all: $(APPS)

.cc.o:
	$(CC) $(CFLAGS) $< -c -o $@

multi: $(OBJS_SR)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

client: $(OBJS_CL)
	${CC} -o $@ $^ $(LDFLAGS) $(CFLAGS)

clean:
	rm -f *.o ${APPS}


.PHONY: clean
CFLAGS := -pedantic -O0 -W -DDEBUG -g
CC := g++

anet.o : anet.h anet.c
	$(CC) -c $(CFLAGS) anet.c

clean:
	@rm anet.o

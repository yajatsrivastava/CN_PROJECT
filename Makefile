# Makefile for KTP (KGP Transport Protocol) Assignment
CC = gcc
CFLAGS = -Wall -Wextra -pthread -g
AR = ar
ARFLAGS = rcs

LIB = libksocket.a
INIT_BIN = initksocket
USER1_BIN = user1
USER2_BIN = user2

.PHONY: all clean tar

all: $(LIB) $(INIT_BIN) $(USER1_BIN) $(USER2_BIN)

$(LIB): ksocket.o
	$(AR) $(ARFLAGS) $@ $^

ksocket.o: ksocket.c ksocket.h
	$(CC) $(CFLAGS) -c ksocket.c -o ksocket.o

$(INIT_BIN): initksocket.c $(LIB) ksocket.h
	$(CC) $(CFLAGS) initksocket.c -L. -lksocket -lpthread -o $(INIT_BIN)

$(USER1_BIN): user1.c $(LIB) ksocket.h
	$(CC) $(CFLAGS) user1.c -L. -lksocket -lpthread -o $(USER1_BIN)

$(USER2_BIN): user2.c $(LIB) ksocket.h
	$(CC) $(CFLAGS) user2.c -L. -lksocket -lpthread -o $(USER2_BIN)

clean:
	rm -f *.o $(LIB) $(INIT_BIN) $(USER1_BIN) $(USER2_BIN)
	rm -f test_output.txt test_input.txt ktp_submission.tar.gz

tar: all
	tar -czvf ktp_submission.tar.gz ksocket.h ksocket.c initksocket.c user1.c user2.c makefile documentation.txt
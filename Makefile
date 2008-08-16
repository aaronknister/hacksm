CC=gcc
CFLAGS=-Wall -g -m32
LIBS=-ldmapi

all: hacksmd hacksm_migrate hacksm_ls

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

hacksmd: hacksmd.o common.o
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

hacksm_migrate: hacksm_migrate.o common.o
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

hacksm_ls: hacksm_ls.o common.o
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

clean: 
	rm -f *.o hacksmd hacksm_migrate hacksm_ls

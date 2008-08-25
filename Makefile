CC=gcc
CFLAGS=-Wall -g 
LIBS=-ldmapi

all: hacksmd hacksm_migrate hacksm_ls

COMMON=store_file.o common.o

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

hacksmd: hacksmd.o $(COMMON)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

hacksm_migrate: hacksm_migrate.o $(COMMON)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

hacksm_ls: hacksm_ls.o $(COMMON)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

clean: 
	rm -f *.o hacksmd hacksm_migrate hacksm_ls

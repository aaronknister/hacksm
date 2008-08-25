#define _XOPEN_SOURCE 500
#define _GNU_SOURCE 

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <getopt.h>
#include <signal.h>
#include <utime.h>
#include <stdbool.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>
#include <aio.h>
#include <dmapi.h>
#include <stdint.h>

#define discard_const(ptr) ((void *)((intptr_t)(ptr)))

const char *dmapi_event_string(dm_eventtype_t ev);
void hsm_recover_session(const char *name, dm_sessid_t *sid);
int hsm_store_open(dev_t device, ino_t inode, int flags);
int hsm_store_unlink(dev_t device, ino_t inode);
void msleep(int t);
void hsm_cleanup_tokens(dm_sessid_t sid, dm_response_t response, int retcode);
const char *timestring(void);


enum hsm_migrate_state {
	HSM_STATE_START     = 0,
	HSM_STATE_MIGRATED  = 1,
	HSM_STATE_RECALL    = 2};

struct hsm_attr {
	char magic[4];
	time_t migrate_time;
	uint64_t size;
	uint64_t device;
	uint64_t inode;
	enum hsm_migrate_state state;
};

#define HSM_MAGIC "HSM1"
#define HSM_ATTRNAME "hacksm"

#include "store.h"

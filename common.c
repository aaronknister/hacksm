/*
  a test implementation of a HSM daemon

  Andrew Tridgell August 2008

 */

#include "hacksm.h"

static const struct {
	dm_eventtype_t ev;
	const char *name;
} dmapi_event_strings[] = {
#define EVENT_STRING(x) { x, #x }
	EVENT_STRING(DM_EVENT_INVALID),
	EVENT_STRING(DM_EVENT_CLOSE),
	EVENT_STRING(DM_EVENT_MOUNT),
	EVENT_STRING(DM_EVENT_PREUNMOUNT),
	EVENT_STRING(DM_EVENT_UNMOUNT),
	EVENT_STRING(DM_EVENT_NOSPACE),
	EVENT_STRING(DM_EVENT_DEBUT),
	EVENT_STRING(DM_EVENT_CREATE),
	EVENT_STRING(DM_EVENT_POSTCREATE),
	EVENT_STRING(DM_EVENT_REMOVE),
	EVENT_STRING(DM_EVENT_POSTREMOVE),
	EVENT_STRING(DM_EVENT_RENAME),
	EVENT_STRING(DM_EVENT_POSTRENAME),
	EVENT_STRING(DM_EVENT_SYMLINK),
	EVENT_STRING(DM_EVENT_POSTSYMLINK),
	EVENT_STRING(DM_EVENT_LINK),
	EVENT_STRING(DM_EVENT_POSTLINK),
	EVENT_STRING(DM_EVENT_READ),
	EVENT_STRING(DM_EVENT_WRITE),
	EVENT_STRING(DM_EVENT_TRUNCATE),
	EVENT_STRING(DM_EVENT_ATTRIBUTE),
	EVENT_STRING(DM_EVENT_CANCEL),
	EVENT_STRING(DM_EVENT_DESTROY),
	EVENT_STRING(DM_EVENT_USER),
	EVENT_STRING(DM_EVENT_PREPERMCHANGE),
	EVENT_STRING(DM_EVENT_POSTPERMCHANGE),
	EVENT_STRING(DM_EVENT_MAX),
};

const char *dmapi_event_string(dm_eventtype_t ev)
{
	int i;
	for (i=0;i<sizeof(dmapi_event_strings)/sizeof(dmapi_event_strings[0]);i++) {
		if (dmapi_event_strings[i].ev == ev) {
			return dmapi_event_strings[i].name;
		}
	}
	return "UNKNOWN";
}

void hsm_recover_session(const char *name, dm_sessid_t *sid)
{
	int ret, i;
	u_int n = 0;
	dm_sessid_t *sess = NULL;
	dm_sessid_t oldsid = DM_NO_SESSION;

	ret = dm_getall_sessions(0, NULL, &n);
	if (ret == 0) {
		goto new_session;
	}
	if (errno != E2BIG) {
		printf("Bad error code %s from dm_getall_sessions\n", strerror(errno));
		exit(1);
	}

	sess = (dm_sessid_t *)calloc(sizeof(dm_sessid_t), n);
	if (sess == NULL) {
		printf("No memory for %u sessions\n", n);
		exit(1);
	}

	ret = dm_getall_sessions(n, sess, &n);
	if (ret != 0) {
		printf("dm_getall_sessions failed\n");
		exit(1);
	}

	for (i=0;i<n;i++) {
		char buf[DM_SESSION_INFO_LEN+1] = "";
		size_t len = 0;

		ret = dm_query_session(sess[i], DM_SESSION_INFO_LEN, buf, &len);
		if (ret != 0) {
			continue;
		}
		buf[len] = 0;
		if (strcmp(buf, name) == 0) {
			printf("Recovered existing session\n");
			oldsid = sess[i];
			break;
		}
	}
	free(sess);

new_session:
	ret = dm_create_session(oldsid, discard_const(name), sid);
	if (ret != 0) {
		printf("Failed to create session\n");
		exit(1);
	}
}


int hsm_store_open(dev_t device, ino_t inode, int flags)
{
	char *fname = NULL;
	asprintf(&fname, HSM_STORE "/0x%llx:0x%llx",
		 (unsigned long long)device, (unsigned long long)inode);
	int fd = open(fname, flags, 0600);
	free(fname);
	return fd;
}

int hsm_store_unlink(dev_t device, ino_t inode)
{
	char *fname = NULL;
	int ret;
	asprintf(&fname, HSM_STORE "/0x%llx:0x%llx",
		 (unsigned long long)device, (unsigned long long)inode);
	ret = unlink(fname);
	free(fname);
	return ret;
}

void msleep(int t)
{
	struct timeval tval;  

	tval.tv_sec = 0;
	tval.tv_usec = t;
 
	select(0,NULL,NULL, NULL, &tval);
}

void hsm_cleanup_tokens(dm_sessid_t sid, dm_response_t response, int retcode)
{
	dm_token_t *tok = NULL;
	u_int n = 0;
	int ret, i, total=0;

	while (1) {
		u_int n2;
		ret = dm_getall_tokens(sid, n, tok, &n2);
		if (ret == -1 && errno == E2BIG) {
			n = n2;
			tok = realloc(tok, sizeof(dm_token_t)*n);
			continue;
		}
		if (ret == -1) {
			printf("dm_getall_tokens - %s\n", strerror(errno));
			return;
		}
		if (ret == 0 && n2 == 0) {
			break;
		}
		printf("Cleaning up %u tokens\n", n2);
		for (i=0;i<n2;i++) {
			dm_respond_event(sid, tok[i], 
					 response, retcode, 0, NULL);
			total++;
		}
	}
	if (tok) free(tok);
}

const char *timestring(void)
{
	time_t t = time(NULL);
	struct tm *tm = localtime(&t);
	static char TimeBuf[100];

	strftime(TimeBuf,sizeof(TimeBuf)-1,"%Y/%m/%d %T",tm);
	return TimeBuf;
}

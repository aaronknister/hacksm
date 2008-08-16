/*
  a test implementation of a HSM daemon

  Andrew Tridgell August 2008

 */

#include "hacksm.h"

static struct {
	dm_sessid_t sid;
} dmapi = {
	.sid = DM_NO_SESSION
};

#define SESSION_NAME "hacksmd"

static void hsm_term_handler(int signal)
{
	printf("Got signal %d - exiting\n", signal);
	exit(1);
}


static void hsm_init(void)
{
	char *dmapi_version = NULL;
	dm_eventset_t eventSet;
	int ret;

	ret = dm_init_service(&dmapi_version);
	if (ret != 0) {
		printf("Failed to init dmapi\n");
		exit(1);
	}

	printf("Initialised DMAPI version '%s'\n", dmapi_version);	

	hsm_recover_session(SESSION_NAME, &dmapi.sid);

	/* we want mount events only initially */
	DMEV_ZERO(eventSet);
	DMEV_SET(DM_EVENT_MOUNT, eventSet);
	ret = dm_set_disp(dmapi.sid, DM_GLOBAL_HANP, DM_GLOBAL_HLEN, DM_NO_TOKEN,
			  &eventSet, DM_EVENT_MAX);
	if (ret != 0) {
		printf("Failed to setup events\n");
		exit(1);
	}
}


static void hsm_handle_mount(dm_eventmsg_t *msg)
{
	dm_mount_event_t *mount;
	void *hand1;
	size_t hand1len;
	dm_eventset_t eventSet;
	int ret;
	
	mount = DM_GET_VALUE(msg, ev_data, dm_mount_event_t*);
	hand1 = DM_GET_VALUE(mount , me_handle1, void *);
	hand1len = DM_GET_LEN(mount, me_handle1);
	
	DMEV_ZERO(eventSet);
	DMEV_SET(DM_EVENT_READ, eventSet);
	DMEV_SET(DM_EVENT_WRITE, eventSet);
	DMEV_SET(DM_EVENT_TRUNCATE, eventSet);
	DMEV_SET(DM_EVENT_DESTROY, eventSet);
	ret = dm_set_eventlist(dmapi.sid, hand1, hand1len,
			       DM_NO_TOKEN, &eventSet, DM_EVENT_MAX);
	if (ret != 0) {
		printf("Failed to setup all event handler\n");
		exit(1);
	}
	
	ret = dm_set_disp(dmapi.sid, hand1, hand1len, DM_NO_TOKEN,
			  &eventSet, DM_EVENT_MAX);
	if (ret != 0) {
		printf("Failed to setup disposition for all events\n");
		exit(1);
	}
	
	ret = dm_respond_event(dmapi.sid, msg->ev_token, 
			       DM_RESP_CONTINUE, 0, 0, NULL);
	if (ret != 0) {
		printf("Failed to respond to mount event\n");
		exit(1);
	}
}


static void hsm_handle_recall(dm_eventmsg_t *msg)
{
	dm_data_event_t *ev;
	void *hanp;
	size_t hlen, rlen;
	int ret;
	dm_attrname_t attrname;
	dm_token_t token = msg->ev_token;
	struct hsm_attr h;
	int retcode = -1;
	dm_boolean_t exactFlag;
	int fd;
	char buf[0x10000];
	off_t ofs;
	int have_right = 0;

        ev = DM_GET_VALUE(msg, ev_data, dm_data_event_t *);
        hanp = DM_GET_VALUE(ev, de_handle, void *);
        hlen = DM_GET_LEN(ev, de_handle);

        memset(attrname.an_chars, 0, DM_ATTR_NAME_SIZE);
        strncpy((char*)attrname.an_chars, HSM_ATTRNAME, DM_ATTR_NAME_SIZE);

	ret = dm_request_right(dmapi.sid, hanp, hlen, token, DM_RR_WAIT, DM_RIGHT_EXCL);
	if (ret != 0) {
		printf("dm_request_right failed - %s\n", strerror(errno));
		goto done;
	}

	have_right = 1;

	ret = dm_get_dmattr(dmapi.sid, hanp, hlen, token, &attrname, 
			    sizeof(h), &h, &rlen);
	if (ret != 0) {
		if (errno == ENOENT) {
			printf("File already recalled (no attribute)\n");
			goto done;
		}
		printf("dm_get_dmattr failed - %s\n", strerror(errno));
		goto done;
	}

	if (rlen != sizeof(h)) {
		printf("hsm_handle_read - bad attribute size %d\n", (int)rlen);
		goto done;
	}

	if (strncmp(h.magic, HSM_MAGIC, sizeof(h.magic)) != 0) {
		printf("Bad magic '%*.*s'\n", (int)sizeof(h.magic), (int)sizeof(h.magic), h.magic);
		goto done;
	}

	h.state = HSM_STATE_RECALL;
	ret = dm_set_dmattr(dmapi.sid, hanp, hlen, token, &attrname, 0, sizeof(h), (void*)&h);
	if (ret != 0) {
		printf("dm_set_dmattr failed - %s\n", strerror(errno));
		goto done;
	}

	fd = hsm_store_open(h.device, h.inode, O_RDONLY);
	if (fd == -1) {
		printf("Failed to open store file for file 0x%llx:0x%llx\n",
		       (unsigned long long)h.device, (unsigned long long)h.inode);
		goto done;
	}

	printf("Recalling file %llx:%llx of size %d\n", 
	       (unsigned long long)h.device, (unsigned long long)h.inode,
	       (int)h.size);

	ofs = 0;
	while ((ret = read(fd, buf, sizeof(buf))) > 0) {
		int ret2 = dm_write_invis(dmapi.sid, hanp, hlen, token, DM_WRITE_SYNC, ofs, ret, buf);
		if (ret2 != ret) {
			printf("dm_write_invis failed - %s\n", strerror(errno));
			goto done;
		}
		ofs += ret;
	}
	close(fd);

	ret = dm_remove_dmattr(dmapi.sid, hanp, hlen, token, 0, &attrname);
	if (ret != 0) {
		printf("dm_remove_dmattr failed - %s\n", strerror(errno));
		goto done;
	}

	ret = hsm_store_unlink(h.device, h.inode);
	if (ret != 0) {
		printf("Failed to unlink store file\n");
		goto done;
	}

	ret = dm_set_region(dmapi.sid, hanp, hlen, token, 0, NULL, &exactFlag);
	if (ret == -1) {
		printf("failed dm_set_region - %s\n", strerror(errno));
		exit(1);
	}

done:
	ret = dm_respond_event(dmapi.sid, msg->ev_token, 
			       DM_RESP_CONTINUE, retcode, 0, NULL);
	if (ret != 0) {
		printf("Failed to respond to read event\n");
		exit(1);
	}

	if (have_right) {
		ret = dm_release_right(dmapi.sid, hanp, hlen, token);
		if (ret == -1) {
			printf("failed dm_release_right on %s\n", strerror(errno));
		}
	}
}


static void hsm_handle_destroy(dm_eventmsg_t *msg)
{
	dm_destroy_event_t *ev;
	void *hanp;
	size_t hlen, rlen;
	int ret;
	dm_attrname_t attrname;
	dm_token_t token = msg->ev_token;
	struct hsm_attr h;
	int retcode = -1;
	dm_boolean_t exactFlag;

        ev = DM_GET_VALUE(msg, ev_data, dm_destroy_event_t *);
        hanp = DM_GET_VALUE(ev, ds_handle, void *);
        hlen = DM_GET_LEN(ev, ds_handle);

	if (DM_TOKEN_EQ(token, DM_INVALID_TOKEN)) {
		goto done;
	}

        memset(attrname.an_chars, 0, DM_ATTR_NAME_SIZE);
        strncpy((char*)attrname.an_chars, HSM_ATTRNAME, DM_ATTR_NAME_SIZE);

	ret = dm_get_dmattr(dmapi.sid, hanp, hlen, token, &attrname, 
			    sizeof(h), &h, &rlen);
	if (ret != 0) {
		printf("dm_get_dmattr failed - %s\n", strerror(errno));
		goto done;
	}

	if (rlen != sizeof(h)) {
		printf("hsm_handle_read - bad attribute size %d\n", (int)rlen);
		goto done;
	}

	if (strncmp(h.magic, HSM_MAGIC, sizeof(h.magic)) != 0) {
		printf("Bad magic '%*.*s'\n", (int)sizeof(h.magic), (int)sizeof(h.magic), h.magic);
		goto done;
	}

	ret = hsm_store_unlink(h.device, h.inode);
	if (ret == -1) {
		printf("Failed to unlink store file for file 0x%llx:0x%llx\n",
		       (unsigned long long)h.device, (unsigned long long)h.inode);
		goto done;
	}

	ret = hsm_store_unlink(h.device, h.inode);
	if (ret != 0) {
		printf("Failed to unlink store file\n");
		goto done;
	}

	ret = dm_remove_dmattr(dmapi.sid, hanp, hlen, token, 0, &attrname);
	if (ret != 0) {
		printf("dm_remove_dmattr failed - %s\n", strerror(errno));
		goto done;
	}

	ret = dm_set_region(dmapi.sid, hanp, hlen, token, 0, NULL, &exactFlag);
	if (ret == -1) {
		printf("failed dm_set_region - %s\n", strerror(errno));
		exit(1);
	}

done:
	if (!DM_TOKEN_EQ(msg->ev_token,DM_NO_TOKEN) &&
	    !DM_TOKEN_EQ(msg->ev_token, DM_INVALID_TOKEN)) {
		ret = dm_respond_event(dmapi.sid, msg->ev_token, 
				       DM_RESP_CONTINUE, retcode, 0, NULL);
		if (ret != 0) {
			printf("Failed to respond to destroy event\n");
			exit(1);
		}
	}
}


static void hsm_handle_message(dm_eventmsg_t *msg)
{
	printf("Got event %s from node 0x%x\n",
	       dmapi_event_string(msg->ev_type), msg->ev_nodeid);

	switch (msg->ev_type) {
	case DM_EVENT_MOUNT:
		hsm_handle_mount(msg);
		break;
	case DM_EVENT_READ:
	case DM_EVENT_WRITE:
		hsm_handle_recall(msg);
		break;
	case DM_EVENT_DESTROY:
		hsm_handle_destroy(msg);
		break;
	default:
		if (!DM_TOKEN_EQ(msg->ev_token,DM_NO_TOKEN) &&
		    !DM_TOKEN_EQ(msg->ev_token, DM_INVALID_TOKEN)) {
			printf("Giving default response\n");
			int ret = dm_respond_event(dmapi.sid, msg->ev_token, 
					       DM_RESP_CONTINUE, 0, 0, NULL);
			if (ret != 0) {
				printf("Failed to respond to mount event\n");
				exit(1);
			}
		}
		break;
	}
}

static void hsm_wait_events(void)
{
	int ret;
	char buf[0x10000];
	size_t rlen;

	printf("Waiting for events\n");
	
	while (1) {
		dm_eventmsg_t *msg;
		/* we don't use DM_RR_WAIT to ensure that the daemon can be killed */
		msleep(10);
		ret = dm_get_events(dmapi.sid, 0, 0, sizeof(buf), buf, &rlen);
		if (ret < 0) {
			if (errno == EAGAIN) continue;
			printf("Failed to get event (%s)\n", strerror(errno));
			exit(1);
		}
		
		for (msg=(dm_eventmsg_t *)buf; msg; msg = DM_STEP_TO_NEXT(msg, dm_eventmsg_t *)) {
			hsm_handle_message(msg);
		}
	}
}

static void usage(void)
{
	printf("Usage: hacksmd <options>\n");
	printf("\n\tOptions:\n");
	printf("\t\t -c                 cleanup lost tokens\n");
	exit(0);
}

int main(int argc, char * const argv[])
{
	int opt;
	bool cleanup = false;

	/* parse command-line options */
	while ((opt = getopt(argc, argv, "ch")) != -1) {
		switch (opt) {
		case 'c':
			cleanup = true;
			break;
		case 'h':
		default:
			usage();
			break;
		}
	}

	setlinebuf(stdout);	

	argv += optind;
	argc -= optind;

	signal(SIGTERM, hsm_term_handler);
	signal(SIGINT, hsm_term_handler);

	hsm_init();

	hsm_cleanup_tokens(dmapi.sid);

	if (cleanup) {
		return 0;
	}

	hsm_wait_events();

	return 0;
}

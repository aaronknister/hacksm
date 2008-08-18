/*
  a test implementation of a HSM daemon

  Andrew Tridgell August 2008

 */

#include "hacksm.h"

static struct {
	bool blocking_wait;
	unsigned debug;
	bool use_fork;
	unsigned recall_delay;
} options = {
	.blocking_wait = true,
	.debug = 2,
	.use_fork = false,
	.recall_delay = 0,
};

static struct {
	dm_sessid_t sid;
} dmapi = {
	.sid = DM_NO_SESSION
};

#define SESSION_NAME "hacksmd"

/* no special handling on terminate in hacksmd, as we want existing
   events to stay around so we can continue them on restart */
static void hsm_term_handler(int signal)
{
	printf("Got signal %d - exiting\n", signal);
	exit(1);
}


/*
  initialise DMAPI, possibly recovering an existing session. The
  hacksmd session is never destroyed, to allow for recovery of
  partially completed events
 */
static void hsm_init(void)
{
	char *dmapi_version = NULL;
	dm_eventset_t eventSet;
	int ret;
	int errcode = 0;

	while ((ret = dm_init_service(&dmapi_version)) == -1) {
		if (errno != errcode) {
			errcode = errno;
			printf("Waiting for DMAPI to initialise (%d: %s)\n", 
			       errno, strerror(errno));
		}
		sleep(1);
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


/*
  called on a DM_EVENT_MOUNT event . This just needs to acknowledge
  the mount. We don't have any sort of 'setup' step before running
  hacksmd on a filesystem, so it just accepts mount events from any
  filesystem that supports DMAPI
 */
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

/*
  called on a data event from DMAPI. Check the files attribute, and if
  it is migrated then do a recall
 */
static void hsm_handle_recall(dm_eventmsg_t *msg)
{
	dm_data_event_t *ev;
	void *hanp;
	size_t hlen, rlen;
	int ret;
	dm_attrname_t attrname;
	dm_token_t token = msg->ev_token;
	struct hsm_attr h;
	dm_boolean_t exactFlag;
	int fd;
	char buf[0x10000];
	off_t ofs;
	dm_right_t right;
	dm_response_t response = DM_RESP_CONTINUE;
	int retcode = 0;

        ev = DM_GET_VALUE(msg, ev_data, dm_data_event_t *);
        hanp = DM_GET_VALUE(ev, de_handle, void *);
        hlen = DM_GET_LEN(ev, de_handle);

        memset(attrname.an_chars, 0, DM_ATTR_NAME_SIZE);
        strncpy((char*)attrname.an_chars, HSM_ATTRNAME, DM_ATTR_NAME_SIZE);

	/* make sure we have an exclusive right on the file */
	ret = dm_query_right(dmapi.sid, hanp, hlen, token, &right);
	if (ret != 0 && errno != ENOENT) {
		printf("dm_query_right failed - %s\n", strerror(errno));
		retcode = EIO;
		response = DM_RESP_ABORT;
		goto done;
	}
	
	if (right != DM_RIGHT_EXCL || errno == ENOENT) {
		ret = dm_request_right(dmapi.sid, hanp, hlen, token, DM_RR_WAIT, DM_RIGHT_EXCL);
		if (ret != 0) {
			printf("dm_request_right failed - %s\n", strerror(errno));
			retcode = EIO;
			response = DM_RESP_ABORT;
			goto done;
		}
	}

	/* get the attribute from the file, and make sure it is
	   valid */
	ret = dm_get_dmattr(dmapi.sid, hanp, hlen, token, &attrname, 
			    sizeof(h), &h, &rlen);
	if (ret != 0) {
		if (errno == ENOENT) {
			if (options.debug > 2) {
				printf("File already recalled (no attribute)\n");
			}
			goto done;
		}
		printf("dm_get_dmattr failed - %s\n", strerror(errno));
		retcode = EIO;
		response = DM_RESP_ABORT;
		goto done;
	}

	if (rlen != sizeof(h)) {
		printf("hsm_handle_read - bad attribute size %d\n", (int)rlen);
		retcode = EIO;
		response = DM_RESP_ABORT;
		goto done;
	}

	if (strncmp(h.magic, HSM_MAGIC, sizeof(h.magic)) != 0) {
		printf("Bad magic '%*.*s'\n", (int)sizeof(h.magic), (int)sizeof(h.magic),
		       h.magic);
		retcode = EIO;
		response = DM_RESP_ABORT;
		goto done;
	}

	/* mark the file as being recalled. This ensures that if
	   hacksmd dies part way through the recall that another
	   migrate won't happen until the recall is completed by a
	   restarted hacksmd */
	h.state = HSM_STATE_RECALL;
	ret = dm_set_dmattr(dmapi.sid, hanp, hlen, token, &attrname, 0, sizeof(h), (void*)&h);
	if (ret != 0) {
		printf("dm_set_dmattr failed - %s\n", strerror(errno));
		retcode = EIO;
		response = DM_RESP_ABORT;
		goto done;
	}

	/* get the migrated data from the store, and put it in the
	   file with invisible writes */
	fd = hsm_store_open(h.device, h.inode, O_RDONLY);
	if (fd == -1) {
		printf("Failed to open store file for file 0x%llx:0x%llx\n",
		       (unsigned long long)h.device, (unsigned long long)h.inode);
		retcode = EIO;
		response = DM_RESP_ABORT;
		goto done;
	}

	if (options.debug > 1) {
		printf("%s %s: Recalling file %llx:%llx of size %d\n", 
		       timestring(),
		       dmapi_event_string(msg->ev_type),
		       (unsigned long long)h.device, (unsigned long long)h.inode,
		       (int)h.size);
	}

	if (options.recall_delay) {
		sleep(random() % options.recall_delay);
	}

	ofs = 0;
	while ((ret = read(fd, buf, sizeof(buf))) > 0) {
		int ret2 = dm_write_invis(dmapi.sid, hanp, hlen, token, DM_WRITE_SYNC, ofs, ret, buf);
		if (ret2 != ret) {
			printf("dm_write_invis failed - %s\n", strerror(errno));
			retcode = EIO;
			response = DM_RESP_ABORT;
			goto done;
		}
		ofs += ret;
	}
	close(fd);

	/* remove the attribute from the file - it is now fully recalled */
	ret = dm_remove_dmattr(dmapi.sid, hanp, hlen, token, 0, &attrname);
	if (ret != 0) {
		printf("dm_remove_dmattr failed - %s\n", strerror(errno));
		retcode = EIO;
		response = DM_RESP_ABORT;
		goto done;
	}

	/* remove the store file */
	ret = hsm_store_unlink(h.device, h.inode);
	if (ret != 0) {
		printf("WARNING: Failed to unlink store file\n");
	}

	/* remove the managed region from the file */
	ret = dm_set_region(dmapi.sid, hanp, hlen, token, 0, NULL, &exactFlag);
	if (ret == -1) {
		printf("failed dm_set_region - %s\n", strerror(errno));
		retcode = EIO;
		response = DM_RESP_ABORT;
		goto done;
	}

done:
	/* tell the kernel that the event has been handled */
	ret = dm_respond_event(dmapi.sid, msg->ev_token, 
			       response, retcode, 0, NULL);
	if (ret != 0) {
		printf("Failed to respond to read event\n");
		exit(1);
	}
}


/*
  called on a DM_EVENT_DESTROY event, when a file is being deleted
 */
static void hsm_handle_destroy(dm_eventmsg_t *msg)
{
	dm_destroy_event_t *ev;
	void *hanp;
	size_t hlen, rlen;
	int ret;
	dm_attrname_t attrname;
	dm_token_t token = msg->ev_token;
	struct hsm_attr h;
	dm_right_t right;
	dm_response_t response = DM_RESP_CONTINUE;
	int retcode = 0;
	dm_boolean_t exactFlag;

        ev = DM_GET_VALUE(msg, ev_data, dm_destroy_event_t *);
        hanp = DM_GET_VALUE(ev, ds_handle, void *);
        hlen = DM_GET_LEN(ev, ds_handle);

	if (DM_TOKEN_EQ(token, DM_INVALID_TOKEN)) {
		goto done;
	}

	/* make sure we have an exclusive lock on the file */
	ret = dm_query_right(dmapi.sid, hanp, hlen, token, &right);
	if (ret != 0 && errno != ENOENT) {
		printf("dm_query_right failed - %s\n", strerror(errno));
		retcode = EIO;
		response = DM_RESP_ABORT;
		goto done;
	}

	if (right != DM_RIGHT_EXCL || errno == ENOENT) {
		ret = dm_request_right(dmapi.sid, hanp, hlen, token, DM_RR_WAIT, DM_RIGHT_EXCL);
		if (ret != 0) {
			printf("dm_request_right failed - %s\n", strerror(errno));
			retcode = EIO;
			response = DM_RESP_ABORT;
			goto done;
		}
	}

        memset(attrname.an_chars, 0, DM_ATTR_NAME_SIZE);
        strncpy((char*)attrname.an_chars, HSM_ATTRNAME, DM_ATTR_NAME_SIZE);

	/* get the attribute and check it is valid. This is just
	   paranoia really, as the file is going away */
	ret = dm_get_dmattr(dmapi.sid, hanp, hlen, token, &attrname, 
			    sizeof(h), &h, &rlen);
	if (ret != 0) {
		printf("WARNING: dm_get_dmattr failed - %s\n", strerror(errno));
		goto done;
	}

	if (rlen != sizeof(h)) {
		printf("hsm_handle_read - bad attribute size %d\n", (int)rlen);
		retcode = EIO;
		response = DM_RESP_ABORT;
		goto done;
	}

	if (strncmp(h.magic, HSM_MAGIC, sizeof(h.magic)) != 0) {
		printf("Bad magic '%*.*s'\n", (int)sizeof(h.magic), (int)sizeof(h.magic), h.magic);
		retcode = EIO;
		response = DM_RESP_ABORT;
		goto done;
	}

	if (options.debug > 1) {
		printf("%s: Destroying file %llx:%llx of size %d\n", 
		       dmapi_event_string(msg->ev_type),
		       (unsigned long long)h.device, (unsigned long long)h.inode,
		       (int)h.size);
	}

	/* remove the store file */
	ret = hsm_store_unlink(h.device, h.inode);
	if (ret == -1) {
		printf("WARNING: Failed to unlink store file for file 0x%llx:0x%llx\n",
		       (unsigned long long)h.device, (unsigned long long)h.inode);
	}

	/* remove the attribute */
	ret = dm_remove_dmattr(dmapi.sid, hanp, hlen, token, 0, &attrname);
	if (ret != 0) {
		printf("dm_remove_dmattr failed - %s\n", strerror(errno));
		retcode = EIO;
		response = DM_RESP_ABORT;
		goto done;
	}

	/* and clear the managed region */
	ret = dm_set_region(dmapi.sid, hanp, hlen, token, 0, NULL, &exactFlag);
	if (ret == -1) {
		printf("WARNING: failed dm_set_region - %s\n", strerror(errno));
	}

done:
	/* only respond if the token is real */
	if (!DM_TOKEN_EQ(msg->ev_token,DM_NO_TOKEN) &&
	    !DM_TOKEN_EQ(msg->ev_token, DM_INVALID_TOKEN)) {
		ret = dm_respond_event(dmapi.sid, msg->ev_token, 
				       response, retcode, 0, NULL);
		if (ret != 0) {
			printf("Failed to respond to destroy event\n");
			exit(1);
		}
	}
}

/*
  main switch for DMAPI messages
 */
static void hsm_handle_message(dm_eventmsg_t *msg)
{
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

/*
  wait for DMAPI events to come in and dispatch them
 */
static void hsm_wait_events(void)
{
	int ret;
	char buf[0x10000];
	size_t rlen;

	printf("Waiting for events\n");
	
	while (1) {
		dm_eventmsg_t *msg;
		if (options.blocking_wait) {
			ret = dm_get_events(dmapi.sid, 0, DM_EV_WAIT, sizeof(buf), buf, &rlen);
		} else {
			/* optionally don't use DM_RR_WAIT to ensure
			   that the daemon can be killed. This is only
			   needed because GPFS uses an uninterruptible
			   sleep for dm_get_events with DM_EV_WAIT. It
			   should be an interruptible sleep */
			msleep(10);
			ret = dm_get_events(dmapi.sid, 0, 0, sizeof(buf), buf, &rlen);
		}
		if (ret < 0) {
			if (errno == EAGAIN) continue;
			if (errno == ESTALE) {
				printf("DMAPI service has shutdown - restarting\n");
				hsm_init();
				continue;
			}
			printf("Failed to get event (%s)\n", strerror(errno));
			exit(1);
		}

		/* loop over all the messages we received */
		for (msg=(dm_eventmsg_t *)buf; 
		     msg; 
		     msg = DM_STEP_TO_NEXT(msg, dm_eventmsg_t *)) {
			/* optionally fork on each message, thus
			   giving parallelism and allowing us to delay
			   recalls, simulating slow tape speeds */
			if (options.use_fork) {
				if (fork() != 0) continue;
				srandom(getpid() ^ time(NULL));
				hsm_handle_message(msg);
				_exit(0);
			} else {
				hsm_handle_message(msg);
			}
		}
	}
}

/*
  on startup we look for partially completed events from an earlier
  instance of hacksmd, and continue them if we can
 */
static void hsm_cleanup_events(void)
{
	char buf[0x1000];
	size_t rlen;
	dm_token_t *tok = NULL;
	u_int n = 0;
	int ret, i;

	while (1) {
		u_int n2;
		ret = dm_getall_tokens(dmapi.sid, n, tok, &n2);
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
			dm_eventmsg_t *msg;
			/* get the message associated with this token
			   back from the kernel */
			ret = dm_find_eventmsg(dmapi.sid, tok[i], sizeof(buf), buf, &rlen);
			if (ret == -1) {
				printf("Unable to find message for token in cleanup\n");
				continue;
			}
			msg = (dm_eventmsg_t *)buf;
			/* there seems to be a bug where GPFS
			   sometimes gives us a garbage token here */
			if (!DM_TOKEN_EQ(tok[i], msg->ev_token)) {
				printf("Message token mismatch in cleanup\n");
				dm_respond_event(dmapi.sid, tok[i], 
						 DM_RESP_ABORT, EINTR, 0, NULL);
			} else {
				unsigned saved_delay = options.recall_delay;
				options.recall_delay = 0;
				hsm_handle_message(msg);
				options.recall_delay = saved_delay;
			}
		}
	}
	if (tok) free(tok);
}

/*
  show program usage
 */
static void usage(void)
{
	printf("Usage: hacksmd <options>\n");
	printf("\n\tOptions:\n");
	printf("\t\t -c                 cleanup lost tokens\n");
	printf("\t\t -N                 use a non-blocking event wait\n");
	printf("\t\t -d level           choose debug level\n");
	printf("\t\t -F                 fork to handle each event\n");
	printf("\t\t -R delay           set a random delay on recall up to 'delay' seconds\n");
	exit(0);
}

/* main code */
int main(int argc, char * const argv[])
{
	int opt;
	bool cleanup = false;

	/* parse command-line options */
	while ((opt = getopt(argc, argv, "chNd:FR:")) != -1) {
		switch (opt) {
		case 'c':
			cleanup = true;
			break;
		case 'd':
			options.debug = strtoul(optarg, NULL, 0);
			break;
		case 'R':
			options.recall_delay = strtoul(optarg, NULL, 0);
			break;
		case 'N':
			options.blocking_wait = false;
			break;
		case 'F':
			options.use_fork = true;
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

	signal(SIGCHLD, SIG_IGN);

	signal(SIGTERM, hsm_term_handler);
	signal(SIGINT, hsm_term_handler);

	hsm_init();

	if (cleanup) {
		hsm_cleanup_tokens(dmapi.sid, DM_RESP_ABORT, EINTR);
		return 0;
	}

	hsm_cleanup_events();

	hsm_wait_events();

	return 0;
}

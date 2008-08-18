/*
  a test implementation of a HSM migrate tool

  Andrew Tridgell August 2008

 */

#include "hacksm.h"

#define SESSION_NAME "hacksm_migrate"

static struct {
	dm_sessid_t sid;
	dm_token_t token;
} dmapi = {
	.sid = DM_NO_SESSION
};


/*
  if we exit unexpectedly then we need to cleanup any rights we held
  by reponding to our userevent
 */
static void hsm_term_handler(int signal)
{
	if (!DM_TOKEN_EQ(dmapi.token,DM_NO_TOKEN)) {
		dm_respond_event(dmapi.sid, dmapi.token, DM_RESP_CONTINUE, 0, 0, NULL);		
		dmapi.token = DM_NO_TOKEN;
	}
	printf("Got signal %d - exiting\n", signal);
	exit(1);
}


/*
  initialise the DMAPI connection
 */
static void hsm_init(void)
{
	char *dmapi_version = NULL;
	int ret;

	ret = dm_init_service(&dmapi_version);
	if (ret != 0) {
		printf("Failed to init dmapi\n");
		exit(1);
	}

	printf("Initialised DMAPI version '%s'\n", dmapi_version);	

	hsm_recover_session(SESSION_NAME, &dmapi.sid);
}

/*
  migrate one file
 */
static int hsm_migrate(const char *path)
{
	int ret;
	void *hanp = NULL;
	size_t hlen = 0;
	dm_attrname_t attrname;
	char buf[0x1000];
	size_t rlen;
	struct stat st;
	struct hsm_attr h;
	dm_region_t region;
	dm_boolean_t exactFlag;
	off_t ofs;
	int fd;
	int retval = 1;

	dmapi.token = DM_NO_TOKEN;

	ret = dm_path_to_handle(discard_const(path), &hanp, &hlen);
	if (ret != 0) {
		printf("dm_path_to_handle failed for %s - %s\n", path, strerror(errno));
		exit(1);
	}

	/* we create a user event which we use to gain exclusive
	   rights on the file */
	ret = dm_create_userevent(dmapi.sid, 0, NULL, &dmapi.token);
	if (ret != 0) {
		printf("dm_create_userevent failed for %s - %s\n", path, strerror(errno));
		exit(1);
	}

	/* getting an exclusive right first guarantees that two
	   migrate commands don't happen at the same time on the same
	   file, and also guarantees that a recall isn't happening at
	   the same time. We then downgrade to a shared right
	   immediately, which still gives the same guarantee, but
	   means that any reads on the file can proceeed while we are
	   saving away the data during the migrate */
	ret = dm_request_right(dmapi.sid, hanp, hlen, dmapi.token, DM_RR_WAIT, DM_RIGHT_EXCL);
	if (ret != 0) {
		printf("dm_request_right failed for %s - %s\n", path, strerror(errno));
		goto respond;
	}

	/* now downgrade the right - reads on the file can then proceed during the
	   expensive migration step */
	ret = dm_downgrade_right(dmapi.sid, hanp, hlen, dmapi.token);
	if (ret != 0) {
		printf("dm_downgrade_right failed for %s - %s\n", path, strerror(errno));
		goto respond;
	}

        memset(attrname.an_chars, 0, DM_ATTR_NAME_SIZE);
        strncpy((char*)attrname.an_chars, HSM_ATTRNAME, DM_ATTR_NAME_SIZE);

	/* get any existing attribute on the file */
	ret = dm_get_dmattr(dmapi.sid, hanp, hlen, dmapi.token, &attrname, 
			    sizeof(h), &h, &rlen);
	if (ret != 0 && errno != ENOENT) {
		printf("dm_get_dmattr failed for %s - %s\n", path, strerror(errno));
		goto respond;
	}

	/* check it is valid */
	if (ret == 0) {
		if (strncmp(h.magic, HSM_MAGIC, sizeof(h.magic)) != 0) {
			printf("Bad magic '%*.*s'\n", (int)sizeof(h.magic), (int)sizeof(h.magic), h.magic);
			exit(1);
		}
		if (h.state == HSM_STATE_START) {
			/* a migration has died on this file */
			printf("Continuing migration of partly migrated file\n");
			hsm_store_unlink(h.device, h.inode);
		} else {
			/* it is either fully migrated, or waiting recall */
			printf("Not migrating already migrated file %s\n", path);
			goto respond;
		}
	}

	if (lstat(path, &st) != 0) {
		printf("failed to stat %s - %s\n", path, strerror(errno));
		goto respond;
	}

	if (!S_ISREG(st.st_mode)) {
		printf("Not migrating non-regular file %s\n", path);
		goto respond;
	}

	if (st.st_size == 0) {
		printf("Not migrating file '%s' of size 0\n", path);
		goto respond;
	}

	/* open up the store file */
	fd = hsm_store_open(st.st_dev, st.st_ino, O_CREAT|O_TRUNC|O_WRONLY);
	if (fd == -1) {
		printf("Failed to open store file for %s - %s\n", path, strerror(errno));
		goto respond;
	}

	/* read the file data and store it away */
	ofs = 0;
	while ((ret = dm_read_invis(dmapi.sid, hanp, hlen, dmapi.token, ofs, sizeof(buf), buf)) > 0) {
		if (write(fd, buf, ret) != ret) {
			printf("Failed to write to store for %s - %s\n", path, strerror(errno));
			hsm_store_unlink(st.st_dev, st.st_ino);
			goto respond;
		}
		ofs += ret;
	}
	if (ret == -1) {
		printf("failed dm_read_invis on %s - %s\n", path, strerror(errno));
		hsm_store_unlink(st.st_dev, st.st_ino);
		goto respond;
	}
	fsync(fd);
	close(fd);

	/* now upgrade to a exclusive right on the file before we
	   change the dmattr and punch holes in the file. */
	ret = dm_upgrade_right(dmapi.sid, hanp, hlen, dmapi.token);
	if (ret != 0) {
		printf("dm_upgrade_right failed for %s - %s\n", path, strerror(errno));
		goto respond;
	}

	strncpy(h.magic, HSM_MAGIC, sizeof(h.magic));
	h.size = st.st_size;
	h.migrate_time = time(NULL);
	h.device = st.st_dev;
	h.inode = st.st_ino;
	h.state = HSM_STATE_START;

	/* mark the file as starting to migrate */
	ret = dm_set_dmattr(dmapi.sid, hanp, hlen, dmapi.token, &attrname, 0, 
			    sizeof(h), (void*)&h);
	if (ret == -1) {
		printf("failed dm_set_dmattr on %s - %s\n", path, strerror(errno));
		hsm_store_unlink(st.st_dev, st.st_ino);
		goto respond;
	}

	/* mark the whole file as offline, including parts beyond EOF */
	region.rg_offset = 0;
	region.rg_size   = 0; /* zero means the whole file */
	region.rg_flags  = DM_REGION_WRITE | DM_REGION_READ;

	ret = dm_set_region(dmapi.sid, hanp, hlen, dmapi.token, 1, &region, &exactFlag);
	if (ret == -1) {
		printf("failed dm_set_region on %s - %s\n", path, strerror(errno));
		hsm_store_unlink(st.st_dev, st.st_ino);
		goto respond;
	}

	/* this dm_get_dmattr() is not strictly necessary - it is just
	   paranoia */
	ret = dm_get_dmattr(dmapi.sid, hanp, hlen, dmapi.token, &attrname, 
			    sizeof(h), &h, &rlen);
	if (ret != 0) {
		printf("ERROR: Abandoning partial migrate - attribute gone!?\n");
		goto respond;
	}

	if (h.state != HSM_STATE_START) {
		printf("ERROR: Abandoning partial migrate - state=%d\n", h.state);
		goto respond;
	}

	ret = dm_punch_hole(dmapi.sid, hanp, hlen, dmapi.token, 0, st.st_size);
	if (ret == -1) {
		printf("failed dm_punch_hole on %s - %s\n", path, strerror(errno));
		hsm_store_unlink(st.st_dev, st.st_ino);
		goto respond;
	}

	h.state = HSM_STATE_MIGRATED;

	/* mark the file as fully migrated */
	ret = dm_set_dmattr(dmapi.sid, hanp, hlen, dmapi.token, &attrname, 
			    0, sizeof(h), (void*)&h);
	if (ret == -1) {
		printf("failed dm_set_dmattr on %s - %s\n", path, strerror(errno));
		hsm_store_unlink(st.st_dev, st.st_ino);
		goto respond;
	}

	printf("Migrated file '%s' of size %d\n", path, (int)st.st_size);

	retval = 0;

respond:
	/* destroy our userevent */
	ret = dm_respond_event(dmapi.sid, dmapi.token, DM_RESP_CONTINUE, 0, 0, NULL);
	if (ret == -1) {
		printf("failed dm_respond_event on %s - %s\n", path, strerror(errno));
		exit(1);
	}
	
	dmapi.token = DM_NO_TOKEN;

	dm_handle_free(hanp, hlen);
	return retval;
}

static void usage(void)
{
	printf("Usage: hacksm_migrate <options> PATH..\n");
	printf("\n\tOptions:\n");
	printf("\t\t -c                 cleanup lost tokens\n");
	exit(0);
}

int main(int argc, char * const argv[])
{
	int opt, i, ret=0;
	bool cleanup = false;

	/* parse command-line options */
	while ((opt = getopt(argc, argv, "hc")) != -1) {
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

	hsm_init();

	if (cleanup) {
		hsm_cleanup_tokens(dmapi.sid, DM_RESP_CONTINUE, 0);
		if (argc == 0) {
			return 0;
		}
	}

	signal(SIGTERM, hsm_term_handler);
	signal(SIGINT, hsm_term_handler);

	if (argc == 0) {
		usage();
	}

	for (i=0;i<argc;i++) {
		ret |= hsm_migrate(argv[i]);
	}

	return ret;
}

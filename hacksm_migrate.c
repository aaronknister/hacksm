/*
  a test implementation of a HSM migrate tool

  Andrew Tridgell August 2008

 */

#include "hacksm.h"

#define SESSION_NAME "hacksm_migrate"

static struct {
	unsigned wait_time;
} options = {
	.wait_time = 100,
};

static struct {
	dm_sessid_t sid;
	dm_token_t token;
} dmapi = {
	.sid = DM_NO_SESSION
};

static void hsm_term_handler(int signal)
{
	if (!DM_TOKEN_EQ(dmapi.token,DM_NO_TOKEN)) {
		dm_respond_event(dmapi.sid, dmapi.token, DM_RESP_CONTINUE, 0, 0, NULL);		
		dmapi.token = DM_NO_TOKEN;
	}
	printf("Got signal %d - exiting\n", signal);
	exit(1);
}


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

	ret = dm_create_userevent(dmapi.sid, 0, NULL, &dmapi.token);
	if (ret != 0) {
		printf("dm_create_userevent failed for %s - %s\n", path, strerror(errno));
		exit(1);
	}

	/* getting an exclusive right then downgrading seems to be much more reliable
	   then going straight to a shared right */
	ret = dm_request_right(dmapi.sid, hanp, hlen, dmapi.token, DM_RR_WAIT, DM_RIGHT_EXCL);
	if (ret != 0) {
		printf("dm_request_right failed for %s - %s\n", path, strerror(errno));
		goto respond;
	}

	ret = dm_downgrade_right(dmapi.sid, hanp, hlen, dmapi.token);
	if (ret != 0) {
		printf("dm_downgrade_right failed for %s - %s\n", path, strerror(errno));
		goto respond;
	}

        memset(attrname.an_chars, 0, DM_ATTR_NAME_SIZE);
        strncpy((char*)attrname.an_chars, HSM_ATTRNAME, DM_ATTR_NAME_SIZE);

	ret = dm_get_dmattr(dmapi.sid, hanp, hlen, dmapi.token, &attrname, 
			    sizeof(h), &h, &rlen);
	if (ret != 0 && errno != ENOENT) {
		printf("dm_get_dmattr failed for %s - %s\n", path, strerror(errno));
		goto respond;
	}

	if (ret == 0) {
		if (strncmp(h.magic, HSM_MAGIC, sizeof(h.magic)) != 0) {
			printf("Bad magic '%*.*s'\n", (int)sizeof(h.magic), (int)sizeof(h.magic), h.magic);
			exit(1);
		}
		if (h.state == HSM_STATE_START) {
			if (h.migrate_time + 60 > time(NULL)) {
				printf("Not migrating recent partially migrated file\n");
				goto respond;
			}
			printf("Continuing migration of partly migrated file\n");
			hsm_store_unlink(h.device, h.inode);
		} else {
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

	fd = hsm_store_open(st.st_dev, st.st_ino, O_CREAT|O_TRUNC|O_WRONLY);
	if (fd == -1) {
		printf("Failed to open store file for %s - %s\n", path, strerror(errno));
		goto respond;
	}

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

	/* this sleep is to work around a race in dmapi on GPFS. A read might have started
	   before we setup the managed region. We need the read to complete before
	   we can punch holes in the file. There must be a better way .... */
	if (options.wait_time) {
		msleep(options.wait_time);
	}

	/* now upgrade to a exclusive right on the file */
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

	ret = dm_set_dmattr(dmapi.sid, hanp, hlen, dmapi.token, &attrname, 0, sizeof(h), (void*)&h);
	if (ret == -1) {
		printf("failed dm_set_dmattr on %s - %s\n", path, strerror(errno));
		hsm_store_unlink(st.st_dev, st.st_ino);
		goto respond;
	}

	region.rg_offset = 0;
	region.rg_size   = st.st_size;
	region.rg_flags  = DM_REGION_WRITE | DM_REGION_READ;

	ret = dm_set_region(dmapi.sid, hanp, hlen, dmapi.token, 1, &region, &exactFlag);
	if (ret == -1) {
		printf("failed dm_set_region on %s - %s\n", path, strerror(errno));
		hsm_store_unlink(st.st_dev, st.st_ino);
		goto respond;
	}

	/* give those pesky reads another chance */
	ret = dm_downgrade_right(dmapi.sid, hanp, hlen, dmapi.token);
	if (ret != 0) {
		printf("dm_downgrade_right failed for %s - %s\n", path, strerror(errno));
		goto respond;
	}
	
	if (options.wait_time) {
		msleep(options.wait_time);
	}

	ret = dm_upgrade_right(dmapi.sid, hanp, hlen, dmapi.token);
	if (ret != 0) {
		printf("dm_downgrade_right failed for %s - %s\n", path, strerror(errno));
		goto respond;
	}


	ret = dm_get_dmattr(dmapi.sid, hanp, hlen, dmapi.token, &attrname, 
			    sizeof(h), &h, &rlen);
	if (ret != 0) {
		printf("Abandoning partial migrate - attribute gone\n");
		goto respond;
	}

	if (h.state != HSM_STATE_START) {
		printf("Abandoning partial migrate - state=%d\n", h.state);
		goto respond;
	}

	ret = dm_punch_hole(dmapi.sid, hanp, hlen, dmapi.token, 0, st.st_size);
	if (ret == -1) {
		printf("failed dm_punch_hole on %s - %s\n", path, strerror(errno));
		hsm_store_unlink(st.st_dev, st.st_ino);
		goto respond;
	}

	h.state = HSM_STATE_MIGRATED;

	ret = dm_set_dmattr(dmapi.sid, hanp, hlen, dmapi.token, &attrname, 0, sizeof(h), (void*)&h);
	if (ret == -1) {
		printf("failed dm_set_dmattr on %s - %s\n", path, strerror(errno));
		hsm_store_unlink(st.st_dev, st.st_ino);
		goto respond;
	}

	printf("Migrated file '%s' of size %d\n", path, (int)st.st_size);

	retval = 0;

respond:
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
	printf("\t\t -w WAITTIME        time to wait in migrate (milliseconds)\n");
	printf("\t\t -c                 cleanup lost tokens\n");
	exit(0);
}

int main(int argc, char * const argv[])
{
	int opt, i, ret=0;
	bool cleanup = false;

	/* parse command-line options */
	while ((opt = getopt(argc, argv, "hw:c")) != -1) {
		switch (opt) {
		case 'w':
			options.wait_time = strtoul(optarg, NULL, 0);
			break;
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

	if (cleanup) {
		hsm_cleanup_tokens(dmapi.sid, DM_RESP_CONTINUE, 0);
		if (argc == 0) {
			return 0;
		}
	}

	signal(SIGTERM, hsm_term_handler);
	signal(SIGINT, hsm_term_handler);

	hsm_init();

	if (argc == 0) {
		usage();
	}

	for (i=0;i<argc;i++) {
		ret |= hsm_migrate(argv[i]);
	}

	return ret;
}

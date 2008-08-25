/*
  a test implementation of a HSM listing tool

  Andrew Tridgell August 2008

 */

#include "hacksm.h"
#include <dirent.h>

#define SESSION_NAME "hacksm_ls"

static struct {
	bool dmapi_info;
} options;

static struct {
	dm_sessid_t sid;
	dm_token_t token;
} dmapi = {
	.sid = DM_NO_SESSION
};

static struct hsm_store_context *store_ctx;

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
  connect to DMAPI
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

	store_ctx = hsm_store_init(void);
	if (store_ctx == NULL) {
		printf("Unable to open HSM store - %s\n", strerror(errno));
		exit(1);
	}
}



/*
  show detailed DMAPI information on a file
 */
static void hsm_show_dmapi_info(const char *path, void *hanp, size_t hlen)
{
	int ret, i;
	dm_region_t *regions = NULL;
	u_int nregions = 0;
	char *buf = NULL;
	size_t buflen = 0;
	dm_stat_t st;

	while ((ret = dm_get_region(dmapi.sid, hanp, hlen, dmapi.token, 
				    nregions, regions, &nregions)) == -1 && 
	       errno == E2BIG) {
		regions = realloc(regions, sizeof(dm_region_t)*nregions);
	}
	if (ret == 0) {
		printf("Managed regions:\n");
		for (i=0;i<nregions;i++) {
			printf("\trg_offset=0x%llx rg_size=0x%llx rg_flags=0x%x rg_opaque=0x%x\n",
			       (unsigned long long)regions[i].rg_offset,
			       (unsigned long long)regions[i].rg_size,
			       (unsigned)regions[i].rg_flags,
			       (unsigned)regions[i].rg_opaque);
		}
	}
	if (regions) free(regions);

	while ((ret = dm_getall_dmattr(dmapi.sid, hanp, hlen, dmapi.token, 
				       buflen, buf, &buflen)) == -1 && 
	       errno == E2BIG) {
		buf = realloc(buf, buflen);
	}
	if (ret == 0) {
		dm_attrlist_t *attr;
		printf("DMAPI Attributes:\n");
		for (attr=(dm_attrlist_t *)buf; 
		     attr; 
		     attr = DM_STEP_TO_NEXT(attr, dm_attrlist_t *)) {
			printf("\t'%*.*s' length=0x%x\n", 
			       DM_ATTR_NAME_SIZE, DM_ATTR_NAME_SIZE,
			       attr->al_name.an_chars, attr->al_data.vd_length);
		}		
	}
	if (buf) free(buf);

	if ((ret = dm_get_fileattr(dmapi.sid, hanp, hlen, dmapi.token, 0, &st)) == 0) {
		printf("DMAPI fileattr ok\n");
	}
}


/*
  list one file
 */
static void hsm_ls(const char *path)
{
	int ret;
	void *hanp = NULL;
	size_t hlen = 0;
	dm_attrname_t attrname;
	size_t rlen;
	struct hsm_attr h;
	int fd;

	dmapi.token = DM_NO_TOKEN;

	ret = dm_path_to_handle(discard_const(path), &hanp, &hlen);
	if (ret != 0) {
		printf("dm_path_to_handle failed for %s - %s\n", path, strerror(errno));
		return;
	}

	/* create a user event to hold a lock on the file while listing */
	ret = dm_create_userevent(dmapi.sid, 0, NULL, &dmapi.token);
	if (ret != 0) {
		printf("dm_create_userevent failed for %s - %s\n", path, strerror(errno));
		dm_handle_free(hanp, hlen);
		return;
	}

	/* we only need a shared right */
	ret = dm_request_right(dmapi.sid, hanp, hlen, dmapi.token, 
			       DM_RR_WAIT, DM_RIGHT_SHARED);
	if (ret != 0) {
		printf("dm_request_right failed for %s - %s\n", path, strerror(errno));
		goto done;
	}

	if (options.dmapi_info) {
		hsm_show_dmapi_info(path, hanp, hlen);
	}

        memset(attrname.an_chars, 0, DM_ATTR_NAME_SIZE);
        strncpy((char*)attrname.an_chars, HSM_ATTRNAME, DM_ATTR_NAME_SIZE);

	/* get the attribute on the file */
	ret = dm_get_dmattr(dmapi.sid, hanp, hlen, dmapi.token, &attrname, 
			    sizeof(h), &h, &rlen);
	if (ret != 0 && errno != ENOENT) {
		printf("dm_get_dmattr failed for %s - %s\n", path, strerror(errno));
		goto done;
	}

	if (ret != 0) {
		printf("p            %s\n", path);
		goto done;
	}
	if (strncmp(h.magic, HSM_MAGIC, sizeof(h.magic)) != 0) {
		printf("Bad magic '%*.*s'\n", (int)sizeof(h.magic), (int)sizeof(h.magic), h.magic);
		goto done;
	}

	/* if it is migrated then also check the store file is OK */
	if (h.state == HSM_STATE_MIGRATED) {
		struct hsm_store_handle *h;
		h = hsm_store_open(store_ctx, h.device, h.inode, true);
		if (h == NULL) {
			printf("Failed to open store file for %s - %s (0x%llx:0x%llx)\n", 
			       path, strerror(errno), 
			       (unsigned long long)h.device, (unsigned long long)h.inode);
		}
		hsm_store_close(h);
	}

	printf("m %7u %d  %s\n", (unsigned)h.size, (int)h.state, path);

done:
	ret = dm_respond_event(dmapi.sid, dmapi.token, DM_RESP_CONTINUE, 0, 0, NULL);
	if (ret == -1) {
		printf("failed dm_respond_event on %s - %s\n", path, strerror(errno));
		exit(1);
	}

	dmapi.token = DM_NO_TOKEN;

	dm_handle_free(hanp, hlen);
}

/*
  list all files in a directory
 */
static void hsm_lsdir(const char *path)
{
	DIR *d;
	struct dirent *de;

	d = opendir(path);
	if (d == NULL) {
		return;
	}

	while ((de = readdir(d))) {
		struct stat st;
		char *name = NULL;
		asprintf(&name, "%s/%s", path, de->d_name);
		if (stat(name, &st) == 0 && S_ISREG(st.st_mode)) {
			hsm_ls(name);
		}
		free(name);
	}

	closedir(d);
}

static void usage(void)
{
	printf("Usage: hacksm_ls <options> PATH..\n");
	printf("\n\tOptions:\n");
	printf("\t\t -D                 show detailed DMAPI info for each file\n");
	exit(0);
}

int main(int argc, char * const argv[])
{
	int opt, i;

	/* parse command-line options */
	while ((opt = getopt(argc, argv, "hD")) != -1) {
		switch (opt) {
		case 'D':
			options.dmapi_info = true;
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

	if (argc == 0) {
		usage();
	}

	signal(SIGTERM, hsm_term_handler);
	signal(SIGINT, hsm_term_handler);

	hsm_init();

	hsm_cleanup_tokens(dmapi.sid, DM_RESP_ABORT, EIO);

	for (i=0;i<argc;i++) {
		struct stat st;
		if (lstat(argv[i], &st) != 0) continue;
		if (S_ISDIR(st.st_mode)) {
			hsm_lsdir(argv[i]);
		} else if (S_ISREG(st.st_mode)) {
			hsm_ls(argv[i]);
		}
	}

	return 0;
}

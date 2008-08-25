/*
  HSM store backend

  Andrew Tridgell August 2008

 */

#include "hacksm.h"

#define HSM_STORE_PATH "/hacksm_store"

struct hsm_store_context {
	const char *basepath;
};

struct hsm_store_handle {
	struct hsm_store_context *ctx;
	int fd;
	bool readonly;
};

/*
  initialise the link to the store
 */
struct hsm_store_context *hsm_store_init(void)
{
	struct hsm_store_context *ctx;
	struct stat st;

	ctx = malloc(sizeof(struct hsm_store_context));
	if (ctx == NULL) {
		errno = ENOMEM;
		return NULL;
	}

	ctx->basepath = HSM_STORE;
	if (stat(ctx->basepath, &st) != 0 ||
	    !S_ISDIR(st.st_mode)) {
		errno = EINVAL;
		free(ctx);
		return NULL;
	}

	return ctx;
}

/*
  shutdown the link to the store
 */
void hsm_store_shutdown(struct hsm_store_context *ctx)
{
	ctx->basepath = NULL;
	free(ctx);
}

/*
  return a filename in the store
 */
static char *store_fname(struct hsm_store_context *ctx, dev_t device, ino_t inode)
{
	char *fname = NULL;
	asprintf(&fname, "%s/0x%llx:0x%llx",
		 ctx->basepath,
		 (unsigned long long)device, (unsigned long long)inode);
	if (fname == NULL) {
		errno = ENOMEM;
		return NULL;
	}
	return fname;
}

/*
  open a file in the store
 */
struct hsm_store_handle *hsm_store_open(struct hsm_store_context *ctx,
					dev_t device, ino_t inode, bool readonly)
{
	struct hsm_store_handle *h;
	char *fname = NULL;

	fname = store_fname(ctx, device, inode);
	if (fname == NULL) {
		return NULL;
	}

	h = malloc(sizeof(struct hsm_store_handle));
	if (h == NULL) {
		errno = ENOMEM;
		free(fname);
		return NULL;
	}

	h->ctx = ctx;
	h->readonly = readonly;

	if (readonly) {
		h->fd = open(fname, O_RDONLY);
	} else {
		h->fd = open(fname, O_WRONLY|O_CREAT|O_TRUNC, 0600);
	}

	free(fname);

	if (h->fd == -1) {
		free(h);
		return NULL;
	}

	return h;
}

/*
  remove a file from the store
 */
int hsm_store_remove(struct hsm_store_context *ctx,
		     dev_t device, ino_t inode)
{
	char *fname;
	int ret;

	fname = store_fname(ctx, device, inode);
	if (fname == NULL) {
		return -1;
	}
	ret = unlink(fname);
	free(fname);
	return ret;
}


/*
  read from a stored file
 */
size_t hsm_store_read(struct hsm_store_handle *h, uint8_t *buf, size_t n)
{
	return read(h->fd, buf, n);
}

/*
  write to a stored file
 */
size_t hsm_store_write(struct hsm_store_handle *h, uint8_t *buf, size_t n)
{
	return write(h->fd, buf, n);
}

/*
  close a store file
 */
int hsm_store_close(struct hsm_store_handle *h)
{
	int ret;
	
	if (!h->readonly) {
		fsync(h->fd);
	}
	ret = close(h->fd);
	h->fd = -1;
	free(h);
	return ret;
}

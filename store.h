/*
  header for HSM store backends
 */

/*
  initialise the link to the HSM store
 */
struct hsm_store_context *hsm_store_init(void);

/* 
   open a file handle in the HSM store
 */
struct hsm_store_handle *hsm_store_open(struct hsm_store_context *,
					dev_t device, ino_t inode, bool readonly);

/*
  read from an open handle
 */
size_t hsm_store_read(struct hsm_store_handle *, uint8_t *buf, size_t n);

/* 
   write to an open handle
 */
size_t hsm_store_write(struct hsm_store_handle *, uint8_t *buf, size_t n);

/* 
   close a handle
 */
int hsm_store_close(struct hsm_store_handle *);

/* 
   shutdown the link to the store
 */
void hsm_store_shutdown(struct hsm_store_context *);

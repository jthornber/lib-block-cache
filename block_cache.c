#include "block_cache.h"

#include "./list.h"

#include <assert.h>
#include <aio.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

// FIXME: get from linux headers
#define SECTOR_SHIFT 9

#if 0
/*----------------------------------------------------------------
 * Allocator
 *--------------------------------------------------------------*/
struct allocator {

};

struct allocator *create_allocator(size_t mem)
{

}

void destroy_allocator(struct allocator *a)
{

}

/* In practise s is drawn from a very small set */
void *alloc(struct allocator *a, size_t s)
{

}
#endif
/*----------------------------------------------------------------
 * Structures
 *--------------------------------------------------------------*/
enum block_flags {
	READ_PENDING = (1 << 0),
	WRITE_PENDING = (1 << 1),
	DIRTY = (1 << 2),
};

struct block_cache;

struct block {
	struct list_head list;
	struct list_head hash_list;

	struct block_cache *bc;	/* up pointer */
	uint64_t index;

	int error;
	unsigned flags;

	struct aiocb control_block;

	pthread_mutex_t lock;
	pthread_cond_t cond;

	struct bc_block b;
};

struct block_cache {
	struct allocator *allocator;
	int fd;
	sector_t block_size;
	uint64_t nr_blocks;

	pthread_mutex_t lock;
	struct list_head errored;
	struct list_head dirty;
	struct list_head clean;
	struct list_head io_pending;

	/*
	 * Hash table fields.
	 */
	unsigned nr_buckets;
	unsigned mask;
	struct list_head buckets[0];
};

/*----------------------------------------------------------------
 * Allocation
 *--------------------------------------------------------------*/
static void release_block(struct block *b)
{
	// FIXME: implement
}

/*----------------------------------------------------------------
 * Logging
 *--------------------------------------------------------------*/
static void info(struct block_cache *bc, const char *format, ...)
	__attribute__ ((format (printf, 2, 3)));

static void info(struct block_cache *bc, const char *format, ...)
{
	va_list ap;

	va_start(ap, format);
	vfprintf(stderr, format, ap);
	va_end(ap);
}

/*----------------------------------------------------------------
 * Locking
 *--------------------------------------------------------------*/
static void block_lock(struct block *b)
{
	pthread_mutex_lock(&b->lock);
}

static void block_unlock(struct block *b)
{
	pthread_mutex_unlock(&b->lock);
}

/* FIXME: get sparse to check b->lock is held */
static void block_wake(struct block *b)
{
	pthread_cond_broadcast(&b->cond);
}

/*
 * Must be called with the bc->lock held.
 * FIXME: can we get sparse to check this?
 */
static int __block_wait(struct block *b)
{
	int r = pthread_cond_wait(&b->cond, &b->lock);
	if (r)
		info(b->bc, "pthread_cond_wait() failed: %d\n", r);

	return r;
}

static void cache_lock(struct block_cache *bc)
{
	pthread_mutex_lock(&bc->lock);
}

static void cache_unlock(struct block_cache *bc)
{
	pthread_mutex_unlock(&bc->lock);
}

/*----------------------------------------------------------------
 * Low level IO handling
 *--------------------------------------------------------------*/

 /* FIXME: can we have two concurrent writes?
  * eg, background writeback, put with dirty, flush?
  *
  * To avoid this we need to introduce some restrictions:
  * i)  A held block can never be written back.
  * ii) You cannot get a block until writeback has completed.
  *
  * FIXME: audit this
  */

static void complete(union sigval v)
{
	struct block *b = v.sival_ptr;

	b->error = aio_error(&b->control_block);

	/*
	 * We can't remove from the io_pending list due to race with
	 * wait_for_all().
	 */
	block_lock(b);
	b->flags &= ~(READ_PENDING | WRITE_PENDING);
	block_wake(b);
	block_unlock(b);
}

static void setup_callback(struct block *b)
{
	struct aiocb *cb = &b->control_block;

	cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
	cb->aio_sigevent.sigev_value.sival_ptr = b;
	cb->aio_sigevent.sigev_notify_function = complete;
	cb->aio_sigevent.sigev_notify_attributes = NULL; /* default attributes */
}

static void setup_control_block(struct block *b, int fd)
{
	struct aiocb *cb = &b->control_block;

	memset(&b->control_block, 0, sizeof(b->control_block));
	cb->aio_fildes = fd;
	cb->aio_offset = (b->bc->block_size << SECTOR_SHIFT) * b->b.index;
	cb->aio_buf = b->b.data;
	cb->aio_nbytes = b->bc->block_size;
	setup_callback(b);
}

static int __io_pending(struct block *b)
{
	return b->flags & (READ_PENDING | WRITE_PENDING);
}

static int issue_read(struct block *b)
{
	int r;
	struct block_cache *bc = b->bc;

	setup_control_block(b, bc->fd);

	block_lock(b);
	assert(!__io_pending(b));
	b->flags |= READ_PENDING;
	block_unlock(b);

	cache_lock(bc);
	list_del(&b->list);
	list_add_tail(&b->list, &bc->io_pending);
	cache_unlock(bc);

	r = aio_read(&b->control_block);
	if (r) {
		info(bc, "aio_read failed: %d\n", r);
		release_block(b);
	}

	return r;
}

static int issue_write(struct block *b)
{
	int r;
	struct block_cache *bc = b->bc;

	setup_control_block(b, bc->fd);

	block_lock(b);
	assert(!__io_pending(b));
	b->flags |= WRITE_PENDING;
	block_unlock(b);

	cache_lock(bc);
	list_del(&b->list);
	list_add_tail(&b->list, &bc->io_pending);
	cache_unlock(bc);

	r = aio_write(&b->control_block);
	if (r) {
		info(bc, "aio_write failed: %d\n", r);
		release_block(b);
	}

	return r;
}

static int wait_until_clear(struct block *b, unsigned test_bits)
{
	int r = 0;

	block_lock(b);
	while (b->flags & test_bits) {
		r = __block_wait(b);
		if (r)
			break;
	}
	block_unlock(b);

	return r;
}

static int wait_for_read(struct block *b)
{
	return wait_until_clear(b, READ_PENDING);
}

static int wait_for_write(struct block *b)
{
	return wait_until_clear(b, WRITE_PENDING);
}

static int wait_for_io(struct block *b)
{
	return wait_until_clear(b, READ_PENDING | WRITE_PENDING);
}

/*----------------------------------------------------------------
 * Clean/dirty list management
 *--------------------------------------------------------------*/

/*
 * FIXME: we're using lru lists atm, but I think it would be worth
 * experimenting with a multiqueue approach.
 */
static struct list_head *__categorise(struct block *b)
{
	if (b->error)
		return &b->bc->errored;

	return (b->flags & DIRTY) ? &b->bc->dirty : &b->bc->clean;
}

static void __add(struct block *b)
{
	list_add_tail(&b->list, __categorise(b));
}

static void __del(struct block *b)
{
	list_del(&b->list);
}

static void hit(struct block *b)
{
	cache_lock(b->bc);
	block_lock(b);
	list_move_tail(&b->list, __categorise(b));
	block_unlock(b);
	cache_unlock(b->bc);
}

/*----------------------------------------------------------------
 * High level IO handling
 *--------------------------------------------------------------*/
static void wait_for_all(struct block_cache *bc)
{
	int r;
	struct block *b, *tmp;
	struct list_head *dest;

	cache_lock(bc);
	list_for_each_entry_safe (b, tmp, &bc->io_pending, list) {
		r = wait_for_io(b);

		/*
		 * Once set b->error is never cleared, so we can access
		 * this safely without the block lock.
		 */
		dest = (r || b->error) ? &bc->errored : &bc->clean;

		list_move_tail(&b->list, dest);
	}
	cache_unlock(bc);
}

/*
 * Caller should check there is pending io.
 */
static void wait_for_one(struct block_cache *bc)
{
	int r;
	struct block *b, *tmp;

	cache_lock(bc);

	if (list_empty(&bc->io_pending))
		return;

	/*
	 * First we run through the list on the off chance that an IO is
	 * already complete.
	 */
	list_for_each_entry_safe (b, tmp, &bc->io_pending, list) {
		block_lock(b);
		if (!__io_pending(b)) {
			list_del(&b->list);
			list_add_tail(&b->list, &bc->clean);
		}
		block_unlock(b);
	}

	if (!list_empty(&bc->clean))
		goto out;

	/*
	 * Then we wait for the oldest IO.
	 */
	b = list_first_entry(&bc->io_pending, struct block, list);

	r = wait_for_io(b);
	if (r) {
		b->error = r;
		list_move_tail(&b->list, &bc->errored);
	}

	list_move_tail(&b->list, &bc->clean);

out:
	cache_unlock(bc);
}

static unsigned writeback(struct block_cache *bc, unsigned count)
{
	int r;
	struct block *b, *tmp;
	unsigned actual = 0;

	cache_lock(bc);
	list_for_each_entry_safe (b, tmp, &bc->dirty, list) {
		r = issue_write(b);
		if (r) {
			b->error = r;
			list_move_tail(&b->list, &bc->errored);
		}

		if (++actual >= count)
			break;
	}
	cache_unlock(bc);

	return actual;
}

/*----------------------------------------------------------------
 * Hash table
 *---------------------------------------------------------------*/

/*
 * |nr_buckets| must be a power of two.
 */
void hash_init(struct block_cache *bc, unsigned nr_buckets)
{
	unsigned i;

	bc->nr_buckets = nr_buckets;
	bc->mask = nr_buckets - 1;

	for (i = 0; i < nr_buckets; i++)
		INIT_LIST_HEAD(bc->buckets + i);
}

unsigned hash(uint64_t index)
{
	/* FIXME: finish */
	return 0;
}

struct block *__hash_lookup(struct block_cache *bc, block_index index)
{
	struct block *b;
	unsigned bucket = hash(index);

	list_for_each_entry (b, bc->buckets + bucket, hash_list) {
		if (b->index == index)
			return b;
	}

	return NULL;
}

struct block *hash_lookup(struct block_cache *bc, block_index index)
{
	struct block *b;

	cache_lock(bc);
	b = __hash_lookup(bc, index);
	cache_unlock(bc);

	return b;
}

void hash_insert(struct block *b)
{
	unsigned bucket = hash(b->index);

	list_add_tail(&b->hash_list, b->bc->buckets + bucket);
}

void hash_remove(struct block *b)
{
	list_del(&b->hash_list);
}

/*----------------------------------------------------------------
 * Public interface
 *--------------------------------------------------------------*/
struct block_cache *
block_cache_create(int fd, sector_t block_size, uint64_t max_nr_blocks, size_t mem)
{
	struct block_cache *bc;


	bc = malloc(sizeof(*bc));
	if (bc) {
#if 0
		if (!a)
			return NULL;

		bc = alloc(a, sizeof(*bc));
		if (!bc)
			return NULL;

		bc->allocator = a;
		bc->fd = fd;
		bc->block_size = block_size;
		bc->nr_blocks = nr_blocks;
#endif
	}

	return bc;
}

void
block_cache_destroy(struct block_cache *bc)
{
	/* FIXME: cancel all pending io */
}

static void zero_block(struct block *b)
{
	memset(b->b.data, 0, b->bc->block_size << SECTOR_SHIFT);
}

struct bc_block *
block_cache_get(struct block_cache *bc, block_index index, unsigned flags)
{
	struct block *b = hash_lookup(bc, index);

	if (b) {
		if (flags & GF_ZERO) {
			zero_block(b);
			mark_dirty(bc, b);
		}

		return b;
	} else
		if (flags & GF_CAN_BLOCK)
			return sync_load_block(bc, index);

	return NULL;
}

#if 0
void
block_cache_put(struct block_cache *bc, struct bc_block *b, unsigned flags)
{
	if (flags & PF_FORGET) {
		// FIXME: implement
	}

	if (flags & PF_DIRTY)
		mark_dirty(bc, b);

	dec(bc, b);
}

int
block_cache_flush(struct block_cache *bc)
{
	int r;
	struct block *b;

	cache_lock(bc);
	list_for_each (b, bc->dirty_blocks, list)
		issue_write(b);
	cache_unlock(bc);

	wait_for_all(bc);

	cache_lock(bc);
	r = list_empty(bc->write_failed);
	cache_unlock(bc);

	return r;
}

void
block_cache_prefetch(struct block_cache *bc, block_index index)
{
	struct block *b = lookup(bc, index);

	if (b)
		hit(b);
	else {
		b = block_alloc(index);
		issue_read(b);
		hash_insert(b);
	}
}

/*----------------------------------------------------------------*/
#endif

#include "block_cache.h"

#include "./list.h"

#include <assert.h>
#include <aio.h>
#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

// FIXME: get from linux headers
#define SECTOR_SHIFT 9
#define MIN_BLOCKS 16
#define PAGE_SIZE 4096

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

	struct block_cache *bc;
	unsigned ref_count;

	/*
	 * The lock is for signalling from the aio thread that an io is
	 * complete.  It only protects the |error| and |flags| fields.
	 */
	pthread_mutex_t lock;
	pthread_cond_t cond;

	int error;
	unsigned flags;
	struct aiocb control_block;

	struct bc_block b;
};

struct block_cache {
	int fd;
	sector_t block_size;
	uint64_t nr_blocks;

	/*
	 * Blocks on the free list are not initialised, apart from the
	 * b.data field.
	 */
	struct list_head free;
	struct list_head errored;
	struct list_head dirty;
	struct list_head clean;
	struct list_head io_pending;

	unsigned nr_dirty;

	/*
	 * Hash table fields.
	 */
	unsigned nr_buckets;
	unsigned mask;
	struct list_head buckets[0];
};

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
 * Allocation
 *--------------------------------------------------------------*/
static void *alloc_aligned(size_t len, size_t alignment)
{
	void *result = NULL;
	int r = posix_memalign(&result, alignment, len);
	if (r)
		return NULL;

	return result;
}

static int init_free_list(struct block_cache *bc, unsigned count)
{
	size_t len;
	struct block *blocks;
	size_t block_size = bc->block_size << SECTOR_SHIFT;
	void *data;
	unsigned i;

	/* Allocate the block structures */
	len = sizeof(struct block) * count;
	blocks = malloc(len);
	if (!blocks)
		return -ENOMEM;

	/* Allocate the data for each block.  We page align the data. */
	data = alloc_aligned(count * block_size, PAGE_SIZE);
	if (!data) {
		free(blocks);
		return -ENOMEM;
	}

	for (i = 0; i < count; i++) {
		struct block *b = blocks + i;
		INIT_LIST_HEAD(&b->list);
		b->b.data = data + block_size * i;

		list_add(&b->list, &bc->free);
	}

	return 0;
}

static struct block *__alloc_block(struct block_cache *bc)
{
	struct block *b;

	if (list_empty(&bc->free))
		return NULL;

	b = list_first_entry(&bc->free, struct block, list);
	list_del(&b->list);

	return b;
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

static void __block_wake(struct block *b)
{
	pthread_cond_broadcast(&b->cond);
}

/*
 * Must be called with the bc->lock held.
 */
static void __block_wait(struct block *b)
{
	int r = pthread_cond_wait(&b->cond, &b->lock);
	if (r) {
		info(b->bc, "pthread_cond_wait() failed: %d\n", r);

		/*
		 * This can only happen if the arguments are invalid, in
		 * which case all bets are off.  We bring the system down.
		 */
		abort();
	}
}

/*----------------------------------------------------------------
 * Low level IO handling
 *
 * We cannot have two concurrent writes on the same block.
 * eg, background writeback, put with dirty, flush?
 *
 * To avoid this we introduce some restrictions:
 *
 * i)  A held block can never be written back.
 * ii) You cannot get a block until writeback has completed.
 *
 * FIXME: audit this
 *--------------------------------------------------------------*/

/*
 * This can be called from the context of the aio thread.  So we have a
 * separate 'top half' complete function that we know is only called by the
 * main cache thread.
 */
static void complete_bottom_half(struct block *b, int error)
{
	/*
	 * We can't remove from the io_pending list due to race with
	 * wait_for_all().
	 */
	block_lock(b);
	b->error = error;
	b->flags &= ~(READ_PENDING | WRITE_PENDING);
	__block_wake(b);
	block_unlock(b);
}

static void complete_top_half(struct block *b)
{
	if (b->error)
		list_move_tail(&b->list, &b->bc->errored);
	else {
		block_lock(b);
		if (b->flags & DIRTY) {
			b->flags |= ~DIRTY;
			b->bc->nr_dirty--;
		}
		block_unlock(b);

		list_move_tail(&b->list, &b->bc->clean);
	}
}

static void complete(union sigval v)
{
	struct block *b = v.sival_ptr;
	int err = aio_error(&b->control_block);

	complete_bottom_half(b, err);
}

static int __io_pending(struct block *b)
{
	return b->flags & (READ_PENDING | WRITE_PENDING);
}

/*
 * |b->list| should be valid (either pointing to itself, on one of the other
 * lists.
 */
static int issue_low_level(struct block *b, int (*issue_fn)(struct aiocb *),
			   const char *desc, unsigned flag)
{
	int r;
	struct block_cache *bc = b->bc;

	block_lock(b);
	assert(!__io_pending(b));
	b->flags |= flag;
	block_unlock(b);

	list_move_tail(&b->list, &bc->io_pending);

	r = issue_fn(&b->control_block);
	if (r) {
		info(bc, "%s failed: %d\n", desc, r);
		complete_bottom_half(b, -EIO);
	}

	return r;
}

static int issue_read(struct block *b)
{
	return issue_low_level(b, aio_read, "aio_read", READ_PENDING);
}

static int issue_write(struct block *b)
{
	return issue_low_level(b, aio_write, "aio_write", WRITE_PENDING);
}

static void wait_until_clear(struct block *b, unsigned test_bits)
{
	block_lock(b);
	while (b->flags & test_bits)
		__block_wait(b);
	block_unlock(b);
}

static void wait_for_io(struct block *b)
{
	wait_until_clear(b, READ_PENDING | WRITE_PENDING);
	complete_top_half(b);
}

/*----------------------------------------------------------------
 * Clean/dirty list management
 *--------------------------------------------------------------*/
#if 0
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

static void hit(struct block *b)
{
	list_move_tail(&b->list, __categorise(b));
}
#endif
/*----------------------------------------------------------------
 * High level IO handling
 *--------------------------------------------------------------*/
static void wait_all(struct block_cache *bc)
{
	struct block *b, *tmp;

	list_for_each_entry_safe (b, tmp, &bc->io_pending, list)
		wait_for_io(b);
}

/*
 * Caller should check there is pending io.
 */
static void wait_one(struct block_cache *bc)
{
	struct block *b, *tmp;

retry:
	if (list_empty(&bc->io_pending))
		return;

	/*
	 * First we run through the list on the off chance that an IO is
	 * already complete.
	 */
	list_for_each_entry_safe (b, tmp, &bc->io_pending, list) {
		block_lock(b);
		if (!__io_pending(b))
			complete_top_half(b);
		block_unlock(b);
	}

	if (!list_empty(&bc->clean))
		return;

	/*
	 * Then we wait for the oldest IO.
	 */
	b = list_first_entry(&bc->io_pending, struct block, list);

	wait_for_io(b);
	if (b->error)
		goto retry;
}

static unsigned writeback(struct block_cache *bc, unsigned count)
{
	int r;
	struct block *b, *tmp;
	unsigned actual = 0;

	list_for_each_entry_safe (b, tmp, &bc->dirty, list) {
		if (actual == count)
			break;

		block_lock(b);
		if (__io_pending(b)) {
			block_unlock(b);
			continue;
		}
		block_unlock(b);

		if (b->ref_count)
			continue;

		r = issue_write(b);
		if (r) {
			b->error = r;
			list_move_tail(&b->list, &bc->errored);
		}

		actual++;
	}

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

struct block *hash_lookup(struct block_cache *bc, block_index index)
{
	struct block *b;
	unsigned bucket = hash(index);

	list_for_each_entry (b, bc->buckets + bucket, hash_list) {
		if (b->b.index == index)
			return b;
	}

	return NULL;
}

void hash_insert(struct block *b)
{
	unsigned bucket = hash(b->b.index);

	list_move_tail(&b->hash_list, b->bc->buckets + bucket);
}

void hash_remove(struct block *b)
{
	list_del_init(&b->hash_list);
}

/*----------------------------------------------------------------
 * High level allocation
 *--------------------------------------------------------------*/
static void setup_callback(struct block *b)
{
	struct aiocb *cb = &b->control_block;

	cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
	cb->aio_sigevent.sigev_value.sival_ptr = b;
	cb->aio_sigevent.sigev_notify_function = complete;
	cb->aio_sigevent.sigev_notify_attributes = NULL; /* default attributes */
}

static void setup_control_block(struct block *b)
{
	struct aiocb *cb = &b->control_block;
	size_t block_size_bytes = b->bc->block_size << SECTOR_SHIFT;

	memset(&b->control_block, 0, sizeof(b->control_block));
	cb->aio_fildes = b->bc->fd;
	cb->aio_offset = block_size_bytes * b->b.index;
	cb->aio_buf = b->b.data;
	cb->aio_nbytes = block_size_bytes;
	setup_callback(b);
}

static void release_block(struct block *b)
{
	hash_remove(b);
	list_move(&b->list, &b->bc->free);
}

static struct block *new_block(struct block_cache *bc,
			       block_index index)
{
	struct block *b;

	b = __alloc_block(bc);

	if (!b) {
		if (list_empty(&bc->clean)) {
			writeback(bc, 80); /* FIXME: magic number */
			wait_one(bc);
		}

		if (!list_empty(&bc->clean)) {
			release_block(list_first_entry(&bc->clean, struct block, list));
			b = __alloc_block(bc);
		}
	}

	if (b) {
		INIT_LIST_HEAD(&b->list);
		INIT_LIST_HEAD(&b->hash_list);
		b->bc = bc;
		b->ref_count = 0;

		pthread_mutex_init(&b->lock, NULL);
		pthread_cond_init(&b->cond, NULL);

		b->error = 0;
		b->flags = 0;

		b->b.index = index;
		setup_control_block(b);

		hash_insert(b);
	}

	return b;
}

/*----------------------------------------------------------------
 * Block reference counting
 *--------------------------------------------------------------*/
static void get_block(struct block *b)
{
	b->ref_count++;
}

static void put_block(struct block *b)
{
	assert(b->ref_count);
	b->ref_count--;
}

static int __is_dirty(struct block *b)
{
	return b->flags & DIRTY;
}

// FIXME: remove this
#define WRITEBACK_THRESHOLD 256

static void mark_dirty(struct block *b)
{
	struct block_cache *bc = b->bc;

	// FIXME: split flags up so we don't need to do this locking
	block_lock(b);		/* because we access b->flags */
	if (!__is_dirty(b)) {
		b->flags |= DIRTY;
		list_move_tail(&b->list, &b->bc->dirty);
		bc->nr_dirty++;
	}
	block_unlock(b);
#if 0
	if (bc->nr_dirty > WRITEBACK_THRESHOLD)
		writeback(bc, WRITEBACK_THRESHOLD / 2);
#endif
}

/*----------------------------------------------------------------
 * Public interface
 *--------------------------------------------------------------*/
unsigned calc_nr_blocks(size_t mem, sector_t block_size)
{
	size_t space_per_block = (block_size << SECTOR_SHIFT) + sizeof(struct block);
	unsigned r = mem / space_per_block;

	return (r < MIN_BLOCKS) ? MIN_BLOCKS : r;
}

unsigned calc_nr_buckets(unsigned nr_blocks)
{
	unsigned r = 8;
	unsigned n = nr_blocks / 4;

	if (n < 8)
		n = 8;

	while (r < n)
		r <<= 1;

	return r;
}

struct block_cache *
block_cache_create(int fd, sector_t block_size, uint64_t max_nr_blocks, size_t mem)
{
	int r;
	struct block_cache *bc;
	unsigned nr_blocks = calc_nr_blocks(mem, block_size);
	unsigned nr_buckets = calc_nr_buckets(nr_blocks);

	bc = malloc(sizeof(*bc) + sizeof(*bc->buckets) * nr_buckets);
	if (bc) {
		bc->fd = fd;
		bc->block_size = block_size;
		bc->nr_blocks = max_nr_blocks;

		hash_init(bc, nr_buckets);
		INIT_LIST_HEAD(&bc->free);
		INIT_LIST_HEAD(&bc->errored);
		INIT_LIST_HEAD(&bc->dirty);
		INIT_LIST_HEAD(&bc->clean);
		INIT_LIST_HEAD(&bc->io_pending);

		r = init_free_list(bc, nr_blocks);
		if (r) {
			info(bc, "couldn't allocate blocks: %d\n", r);
			free(bc);
			return NULL;
		}
	}

	info(bc, "created block cache with %u entries\n", nr_blocks);

	return bc;
}

void
block_cache_destroy(struct block_cache *bc)
{
	wait_all(bc);
	// FIXME: finish
}

static void zero_block(struct block *b)
{
	memset(b->b.data, 0, b->bc->block_size << SECTOR_SHIFT);
	mark_dirty(b);
}

static struct block *read_block(struct block_cache *bc, block_index index, unsigned flags)
{
	struct block *b = hash_lookup(bc, index);

	if (b) {
		// FIXME: IO pending?


		if (flags & GF_ZERO)
			zero_block(b);

	} else {
		if (flags & GF_CAN_BLOCK) {
			b = new_block(bc, index);

			if (b) {
				if (flags & GF_ZERO)
					zero_block(b);
				else {
					issue_read(b);
					wait_for_io(b);

					if (b->error)
						b = NULL;
				}
			}
		}
	}

	return b;
}

struct bc_block *
block_cache_get(struct block_cache *bc, block_index index, unsigned flags)
{
	struct block *b = read_block(bc, index, flags);

	if (b) {
		get_block(b);
		return &b->b;
	}

	return NULL;
}

void
block_cache_put(struct block_cache *bc, struct bc_block *bcb, unsigned flags)
{
	struct block *b = container_of(bcb, struct block, b);

	if (flags & PF_DIRTY)
		mark_dirty(b);

	put_block(b);
}

int
block_cache_flush(struct block_cache *bc)
{
	struct block *b;

	list_for_each_entry (b, &bc->dirty, list)
		issue_write(b);

	wait_all(bc);

	return list_empty(&bc->errored) ? 0 : -EIO;
}

void
block_cache_prefetch(struct block_cache *bc, block_index index)
{
	struct block *b = hash_lookup(bc, index);

	if (!b) {
		b = new_block(bc, index);
		if (b)
			issue_read(b);
	}
}

/*----------------------------------------------------------------*/


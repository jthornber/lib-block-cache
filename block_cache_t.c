#include "block_cache.h"

#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

/*----------------------------------------------------------------*/

#define NR_BLOCKS (64 * 10240)

static void sequential_zero(struct block_cache *bc)
{
	uint64_t i;

	/* Check GF_ZERO works */
	for (i = 0; i < NR_BLOCKS; i++) {
		struct bc_block *b = block_cache_get(bc, i, GF_ZERO | GF_CAN_BLOCK);

		if (!b) {
			fprintf(stderr, "unable to get a block\n");
			exit(1);
		}

		block_cache_put(bc, b, PF_DIRTY);
	}
}

static void sequential_read(struct block_cache *bc)
{
	uint64_t i;

	for (i = 0; i < NR_BLOCKS; i++) {
		struct bc_block *b = block_cache_get(bc, i, GF_CAN_BLOCK);

		if (!b) {
			fprintf(stderr, "unable to get a block\n");
			exit(1);
		}

		// FIXME: check blocks are full of zeroes

		block_cache_put(bc, b, 0);
	}
}

static void sequential_read_with_precache(struct block_cache *bc)
{
	uint64_t i, precache_count = 1000;

	for (i = 0; i < precache_count; i++)
		block_cache_prefetch(bc, i);

	for (i = 0; i < NR_BLOCKS; i++) {
		struct bc_block *b = block_cache_get(bc, i, GF_CAN_BLOCK);

		if (!b) {
			fprintf(stderr, "unable to get a block\n");
			exit(1);
		}

		// FIXME: check blocks are full of zeroes

		block_cache_put(bc, b, 0);

		if (i + precache_count < NR_BLOCKS)
			block_cache_prefetch(bc, i + precache_count);
	}
}

int main(int argc, char **argv)
{
	int fd;
	struct block_cache *bc;

	if (argc != 2) {
		fprintf(stderr, "Usage: block_cache_t <file or dev>\n");
		exit(1);
	}

	fd = open(argv[1],  O_RDWR | O_CREAT | O_DIRECT | O_SYNC, 0666);
	if (fd < 0) {
		perror("couldn't open data file\n");
		exit(1);
	}

	bc = block_cache_create(fd, 8, NR_BLOCKS, 4096 * 10240);

	sequential_zero(bc);
	//sequential_read(bc);
	//sequential_read_with_precache(bc);

	block_cache_destroy(bc);

	return 0;
}

/*----------------------------------------------------------------*/

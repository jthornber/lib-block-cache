.PHONEY: all
all: block_cache_t

CFLAGS=\
	-g \
	-Wall \
	-D_GNU_SOURCE

LIBS=\
	-laio

block_cache.o: block_cache.c
	gcc -c $(CFLAGS) $+ -o $@

block_cache_t: block_cache_t.c block_cache.o
	gcc $(CFLAGS) $+ -o $@ $(LIBS)

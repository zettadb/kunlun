#include "postgres.h"

#include "utils/palloc.h"
#include "utils/memutils.h"
#include "sharding/mat_cache.h"
#include <unistd.h>
#include <sys/types.h>
#include <unistd.h>

MatCache *matcache_create()
{
    MemoryContext mem_ctx = MemoryContextSwitchTo(TopMemoryContext);
    MatCache *cache = (MatCache *)palloc0(sizeof(MatCache));
    cache->file = BufFileCreateTemp(true);
    cache->mode = WRITE_MODE;
    MemoryContextSwitchTo(mem_ctx);

    return cache;
}

void matcache_close(MatCache *cache)
{
    BufFileClose(cache->file);
    pfree(cache);
}

void matcache_reset(MatCache *cache)
{
    memset(&cache->read_pos, 0, sizeof(MatCachePos));
    memset(&cache->write_pos, 0, sizeof(MatCachePos));
    BufFileSeek(cache->file, 0, 0, SEEK_SET);
}

bool matcache_eof(MatCache *cache)
{
    if (cache->mode == READ_MODE)
        BufFileTell(cache->file, &cache->read_pos.fileno, &cache->read_pos.offset);
    else
        BufFileTell(cache->file, &cache->write_pos.fileno, &cache->write_pos.offset);

    return memcmp(&cache->read_pos, &cache->write_pos, sizeof(MatCachePos)) == 0;
}

void matcache_write(MatCache *cache, uchar *data, size_t len)
{
    MemoryContext mem_ctx = MemoryContextSwitchTo(TopMemoryContext);

    if (cache->mode == READ_MODE)
    {
        /* Save the read pos */
        BufFileTell(cache->file, &cache->read_pos.fileno, &cache->read_pos.offset);

        BufFileSeek(cache->file, cache->write_pos.fileno, cache->write_pos.offset, SEEK_SET);

        cache->mode = WRITE_MODE;
    }

    BufFileWrite(cache->file, data, len);

    MemoryContextSwitchTo(mem_ctx);
}

size_t matcache_read(MatCache *cache, uchar *buff, size_t len)
{
    MemoryContext mem_ctx = MemoryContextSwitchTo(TopMemoryContext);

    if (cache->mode == WRITE_MODE)
    {
        /* Save the write pos */
        BufFileTell(cache->file, &cache->write_pos.fileno, &cache->write_pos.offset);

        /* Switch to read mode, and set the proper read pos */
        BufFileSeek(cache->file, cache->read_pos.fileno, cache->read_pos.offset, SEEK_SET);

        cache->mode = READ_MODE;
    }

    size_t size = BufFileRead(cache->file, buff, len);

    MemoryContextSwitchTo(mem_ctx);

    return size;
}

void matcache_get_read_pos(MatCache *cache, MatCachePos *pos)
{
    if (cache->mode == WRITE_MODE)
        *pos = cache->read_pos;
    else
        BufFileTell(cache->file, &pos->fileno, &pos->offset);
}

void matcache_get_write_pos(MatCache *cache, MatCachePos *pos)
{
    if (cache->mode == READ_MODE)
        *pos = cache->write_pos;
    else
        BufFileTell(cache->file, &pos->fileno, &pos->offset);
}

void matcache_set_read_pos(MatCache *cache, MatCachePos pos)
{
    if (cache->mode == WRITE_MODE)
        cache->read_pos = pos;
    else
        BufFileSeek(cache->file, pos.fileno, pos.offset, SEEK_SET);
}

void matcache_set_write_pos(MatCache *cache, MatCachePos pos)
{
    if (cache->mode == READ_MODE)
        cache->write_pos = pos;
    else
        BufFileSeek(cache->file, pos.fileno, pos.offset, SEEK_SET);
}

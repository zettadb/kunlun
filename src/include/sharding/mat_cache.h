#ifndef MAT_CACHE_H
#define MAT_CACHE_H

#include "postgres.h"

#include "storage/buffile.h"

#define BUFF_SIZE 1024

typedef unsigned char uchar;

#define READ_MODE 'r'
#define WRITE_MODE 'w'
typedef struct MatCachePos
{
        int fileno;
        off_t offset;
} MatCachePos;
typedef struct MatCache
{
        MatCachePos read_pos;
        MatCachePos write_pos;
        BufFile *file;
        char mode;
} MatCache;

extern MatCache *matcache_create(void);
extern void matcache_close(MatCache *cache);
extern void matcache_reset(MatCache *cache);
extern bool matcache_eof(MatCache *cache);
static inline char matcache_mode(MatCache *cache) { return cache->mode; }
extern void matcache_write(MatCache *cache, uchar *data, size_t len);
extern size_t matcache_read(MatCache *cache, uchar *buff, size_t len);

extern void matcache_get_read_pos(MatCache *cache, MatCachePos *pos);
extern void matcache_set_read_pos(MatCache *cache, MatCachePos pos);
extern void matcache_get_write_pos(MatCache *cache, MatCachePos *pos);
extern void matcache_set_write_pos(MatCache *cache, MatCachePos pos);
#endif
/* ******************************************************************
 * f2sz
 * Copyright (c) 2023, Anton Korobeynikov
 *
 * This source code is licensed under the GPLv3 (found in the LICENSE
 * file in the root directory of this source tree).
 ****************************************************************** */

#include <memory.h>
#include <cstddef>
#include <cstdint>

bool strEndsWith(const char *str, const char *suf);
size_t decodeMultiplier(char *arg);

static uint32_t readLE32(uint8_t *buf) {
    uint32_t result;
    memcpy(&result, buf, sizeof(result));

#if defined(F2SZ_BIG_ENDIAN)
    result = ((result & 0xFF000000) >> 24) | ((result & 0x00FF0000) >> 8) |
             ((result & 0x0000FF00) << 8)  | ((result & 0x000000FF) << 24);
#endif

    return result;
}

static uint64_t readLE64(uint8_t *buf) {
    uint64_t result;
    memcpy(&result, buf, sizeof(result));

#if defined(F2SZ_BIG_ENDIAN)
    result =
            ((result << 56) & 0xFF00000000000000UL) |
            ((result << 40) & 0x00FF000000000000UL) |
            ((result << 24) & 0x0000FF0000000000UL) |
            ((result <<  8) & 0x000000FF00000000UL) |
            ((result >>  8) & 0x00000000FF000000UL) |
            ((result >> 24) & 0x0000000000FF0000UL) |
            ((result >> 40) & 0x000000000000FF00UL) |
            ((result >> 56) & 0x00000000000000FFUL);
#endif
    return result;
}

static uint32_t readLE24(uint8_t *buf) {
    return buf[0] |
          (buf[1] << 8) |
          (buf[2] << 16);
}

static void writeLE32(void *dst, uint32_t data) {
#if defined(F2SZ_BIG_ENDIAN)
    uint32_t swap = ((data & 0xFF000000) >> 24) | ((data & 0x00FF0000) >> 8) |
                    ((data & 0x0000FF00) << 8) | ((data & 0x000000FF) << 24);
    memcpy(dst, &swap, sizeof(swap));
#else
    memcpy(dst, &data, sizeof(data));
#endif
}

static void writeLE64(void *dst, uint64_t data) {
#if defined(F2SZ_BIG_ENDIAN)
    uint64_t swap =
            ((x << 56) & 0xFF00000000000000UL) |
            ((x << 40) & 0x00FF000000000000UL) |
            ((x << 24) & 0x0000FF0000000000UL) |
            ((x <<  8) & 0x000000FF00000000UL) |
            ((x >>  8) & 0x00000000FF000000UL) |
            ((x >> 24) & 0x0000000000FF0000UL) |
            ((x >> 40) & 0x000000000000FF00UL) |
            ((x >> 56) & 0x00000000000000FFUL);
    memcpy(dst, &swap, sizeof(swap));
#else
    memcpy(dst, &data, sizeof(data));
#endif
}

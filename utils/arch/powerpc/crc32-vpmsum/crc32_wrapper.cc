/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#if defined(__PPC64__)
#define CRC_TABLE
#include "crc32_constants.h"

#define VMX_ALIGN       16
#define VMX_ALIGN_MASK  (VMX_ALIGN-1)

#ifdef REFLECT
static uint32_t crc32_align(uint32_t crc, const uint8_t* p, size_t len)
{
    while (len--) {
        crc = crc_table[(crc ^ *p++) & 0xff] ^ (crc >> 8);
    }
    return crc;
}
#else
static uint32_t crc32_align(uint32_t crc, const uint8_t* p, size_t len)
{
    while (len--) {
        crc = crc_table[((crc >> 24) ^ *p++) & 0xff] ^ (crc << 8);
    }
    return crc;
}
#endif

uint32_t __crc32_vpmsum(uint32_t crc, const uint8_t* p, size_t len);

uint32_t crc32_vpmsum(uint32_t crc, const uint8_t* p, size_t len)
{
    uint32_t prealign;
    uint32_t tail;

#ifdef CRC_XOR
    crc ^= 0xffffffff;
#endif

    if (len < VMX_ALIGN + VMX_ALIGN_MASK) {
        crc = crc32_align(crc, p, len);
        goto out;
    }

    if ((unsigned long)p & VMX_ALIGN_MASK) {
        prealign = VMX_ALIGN - ((unsigned long)p & VMX_ALIGN_MASK);
        crc = crc32_align(crc, p, prealign);
        len -= prealign;
        p += prealign;
    }

    crc = __crc32_vpmsum(crc, p, len & ~VMX_ALIGN_MASK);

    tail = len & VMX_ALIGN_MASK;
    if (tail) {
        p += len & ~VMX_ALIGN_MASK;
        crc = crc32_align(crc, p, tail);
    }

out:
#ifdef CRC_XOR
    crc ^= 0xffffffff;
#endif

    return crc;
}
#endif
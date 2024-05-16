/* ******************************************************************
 * f2sz
 * Copyright (c) 2024, Anton Korobeynikov
 *
 * This source code is licensed under the GPLv3 (found in the LICENSE
 * file in the root directory of this source tree).
 ****************************************************************** */

#include "index.h"
#include "utils.h"

#include <string>

#include <cstdio>
#include <cstdint>

#include <string_view>
#include <zstd.h>

void RecordIndex::print(FILE *outFile) const {
    fprintf(outFile, "\n---- index ----\n");
    fprintf(outFile, "name\tindex\tinput offset\n");

    for (const auto &entry : indexEntries) {
        fwrite(entry.name.data(), entry.name.size(), 1, outFile);
        fputc('\t', outFile);

        fprintf(outFile, "%zu\t", entry.idx);
        fprintf(outFile, "%zu\n", entry.offset);
    }
}

bool RecordIndex::read(FILE *inFile, size_t frameSize, bool verbose) {
    uint8_t footerBuffer[ZSTD_indexTableFooterSize];

    if (frameSize < ZSTD_indexTableFooterSize) {
        if (verbose)
            fprintf(stderr, "ERROR: too small index table frame\n");
        return false;
    }

    if (fseek(inFile, frameSize -  ZSTD_indexTableFooterSize, SEEK_CUR) != 0) {
        if (verbose)
            fprintf(stderr, "ERROR: failed to read index table\n");
        return false;
    }

    size_t numFooterBytesRead = fread(footerBuffer, 1, sizeof(footerBuffer), inFile);
    if (numFooterBytesRead != ZSTD_indexTableFooterSize) {
        if (verbose)
            fprintf(stderr, "ERROR: failed to read index table footer");
        return false;
    }

    if (footerBuffer[4] & 0x1) {
        uint32_t fframeSize = readLE32(footerBuffer);
        if (fframeSize != frameSize) {
            if (verbose)
                fprintf(stderr, "ERROR: invalid index table size");
            return false;
        }
    }

    if (fseek(inFile, -(long)frameSize, SEEK_CUR) != 0) {
        if (verbose)
            fprintf(stderr, "ERROR: failed to read index table\n");
        return false;
    }

    size_t framePos = 0;
    while (framePos < frameSize - ZSTD_indexTableFooterSize) {
        uint8_t buf[16];
        std::string entryName;
        while (true) {
            // Read a single entry. It is always safe to read 16 bytes as each entry
            // is at least 17 bytes long
            size_t numBytesRead = fread(buf, 1, sizeof(buf), inFile);
            if (numBytesRead != sizeof(buf)) {
                if (verbose)
                    fprintf(stderr, "ERROR: failed to read index table\n");
                return false;
            }
            framePos += numBytesRead;

            uint8_t *nullPos = (uint8_t*)memchr(buf, 0, sizeof(buf));
            if (nullPos == NULL) {
                // No null terminator, just append to name
                entryName.append((char*)buf, sizeof(buf));
                // Read next chunk
                continue;
            }

            // null terminator found in buffer, append chunk and read the
            // remaining pieces
            entryName.append((char*)buf, nullPos - buf);
            size_t chunkLen = buf + sizeof(buf) - nullPos - 1;

            memmove(buf, nullPos + 1, chunkLen);
            numBytesRead = fread(buf + chunkLen, 1, sizeof(buf) - chunkLen, inFile);
            if (numBytesRead != sizeof(buf) - chunkLen) {
                if (verbose)
                    fprintf(stderr, "ERROR: failed to read index table entry\n");
                return false;
            }
            framePos += numBytesRead;

            uint64_t idx = readLE64(buf);
            uint64_t off = readLE64(buf + 8);
            char *str = strdup(entryName.c_str());
            stringCache.emplace_back(str, free);

            add({str, entryName.size()}, idx, off);
            break;
        }
    }

    if (fseek(inFile, ZSTD_indexTableFooterSize, SEEK_CUR) != 0) {
        if (verbose)
            fprintf(stderr, "ERROR: failed to read record index\n");
        return false;
    }

    if (verbose)
        print(stderr);

    return true;
}

void RecordIndex::write(FILE *outFile, bool verbose) {
    uint8_t buf[sizeof(size_t)];

    if (verbose)
        print(stderr);

    // Skippable_Magic_Number
    writeLE32(buf, ZSTD_MAGIC_SKIPPABLE_START | 0xF);
    fwrite(buf, 4, 1, outFile);

    // Determine frame size
    uint32_t frameSize = 0;
    for (const auto &entry : indexEntries) {
        frameSize += entry.name.size() + 1; // Zero terminated
        frameSize += 8 + 8; // idx, offset
    }
    frameSize += ZSTD_indexTableFooterSize; // footer: number of index entries, reserved byte, magic

    // Frame_Size
    writeLE32(buf, frameSize);
    fwrite(buf, 4, 1, outFile);

    // Index_Table_Entries
    for (const auto &entry : indexEntries) {
        fwrite(entry.name.data(), entry.name.size(), 1, outFile);
        fputc(0, outFile);

        writeLE64(buf, entry.idx);
        fwrite(buf, 8, 1, outFile);

        writeLE64(buf, entry.offset);
        fwrite(buf, 8, 1, outFile);
    }

    // Index_Table_Footer
    // Frame_Size
    writeLE32(buf, frameSize);
    fwrite(buf, 4, 1, outFile);

    // Index_Table_Descriptor
    buf[0] = 0 | 1;
    fwrite(buf, 1, 1, outFile);

    // Index_Magic_Number
    writeLE32(buf, ZSTD_FIDX_MAGICNUMBER); // 'FIDX'
    fwrite(buf, 4, 1, outFile);
}

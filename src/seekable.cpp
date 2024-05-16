/* ******************************************************************
 * f2sz
 * Copyright (c) 2024, Anton Korobeynikov
 *
 * This source code is licensed under the GPLv3 (found in the LICENSE
 * file in the root directory of this source tree).
 ********************************************************************/

#include "seekable.h"
#include "utils.h"
#include <cstdio>
#include <cstdint>
#include <zstd.h>

bool SeekTable::read(FILE *inFile, size_t frameSize, bool verbose) {
    uint8_t footerBuffer[ZSTD_seekTableFooterSize];

    if (frameSize < ZSTD_seekTableFooterSize) {
        if (verbose)
            fprintf(stderr, "ERROR: too small seek table frame\n");
        return false;
    }

    // Determine the entry size
    int res = fseek(inFile, frameSize - ZSTD_seekTableFooterSize, SEEK_CUR);
    if (res != 0) {
        if (verbose)
            fprintf(stderr, "ERROR: failed to read seek table entry");
        return false;
    }

    size_t numFooterBytesRead = fread(footerBuffer, 1, sizeof(footerBuffer), inFile);
    if (numFooterBytesRead != ZSTD_seekTableFooterSize) {
        if (verbose)
            fprintf(stderr, "ERROR: failed to read seek table footer");
        return false;
    }

    unsigned entrySize = footerBuffer[4] & 0x80 ? 12 : 8;
    uint32_t numEntries = readLE32(footerBuffer);
    size_t expectedFrameSize = entrySize * numEntries + ZSTD_seekTableFooterSize;

    if (frameSize != expectedFrameSize) {
        if (verbose)
            fprintf(stderr, "ERROR: seek table frame size invalid\n");
        return false;
    }

    res = fseek(inFile, -(long)frameSize, SEEK_CUR);
    if (res != 0) {
        if (verbose)
            fprintf(stderr, "ERROR: failed to read seek table entry");
        return false;
    }

    uint8_t buf[12];
    for (size_t i = 0; i < numEntries; ++i) {
        int res = fread(buf, 1, entrySize, inFile);
        if (res != entrySize) {
            if (verbose)
                fprintf(stderr, "ERROR: failed to read seek table entry");
            return false;
        }
        add(readLE32(buf), readLE32(buf + 4));
    }

    res = fread(buf, 1, 4, inFile);
    if (res != 4) {
        if (verbose)
            fprintf(stderr, "ERROR: failed to read seek table entry");
        return false;
    }
    if (readLE32(buf) != numEntries) {
        if (verbose)
            fprintf(stderr, "ERROR: invalid number of seek table entries");
        return false;
    }

    res = fseek(inFile, 5, SEEK_CUR);
    if (res != 0) {
        if (verbose)
            fprintf(stderr, "ERROR: failed to read seek table entry");
        return false;
    }

    if (verbose)
        print(stderr);

    return true;
}

void SeekTable::print(FILE *outFile) const {
    fprintf(outFile, "\n---- seek table ----\n");
    fprintf(outFile, "decompressed\tcompressed\n");

    for (const auto &e : tableEntries) {
        fprintf(stderr, "%u\t%u\n", e.decompressedSize, e.compressedSize);
    }
}

void SeekTable::write(FILE *outFile, bool verbose) {
    uint8_t buf[4];

    // Skippable_Magic_Number
    writeLE32(buf, ZSTD_MAGIC_SKIPPABLE_START | 0xE);
    fwrite(buf, 4, 1, outFile);

    // Frame_Size
    writeLE32(buf, tableEntries.size() * 8 + ZSTD_seekTableFooterSize);
    fwrite(buf, 4, 1, outFile);

    if (verbose)
        print(stderr);

    // Seek_Table_Entries
    for (const auto &e : tableEntries) {
        // Compressed_Size
        writeLE32(buf, e.compressedSize);
        fwrite(buf, 4, 1, outFile);

        // Decompressed_Size
        writeLE32(buf, e.decompressedSize);
        fwrite(buf, 4, 1, outFile);
    }

    // Seek_Table_Footer
    // Number_Of_Frames
    writeLE32(buf, tableEntries.size());
    fwrite(buf, 4, 1, outFile);

    // Seek_Table_Descriptor
    buf[0] = 0;
    fwrite(buf, 1, 1, outFile);

    // Seekable_Magic_Number
    writeLE32(buf, ZSTD_SEEKABLE_MAGICNUMBER);
    fwrite(buf, 4, 1, outFile);
}

/* ******************************************************************
 * f2sz
 * Copyright (c) 2024, Anton Korobeynikov
 *
 * This source code is licensed under the GPLv3 (found in the LICENSE
 * file in the root directory of this source tree).
 ****************************************************************** */

#include "seekable.h"
#include "utils.h"
#include <zstd.h>
#include <cstdint>

void SeekTable::write(FILE *outFile, bool verbose) {
    uint8_t buf[4];

    // Skippable_Magic_Number
    writeLE32(buf, ZSTD_MAGIC_SKIPPABLE_START | 0xE);
    fwrite(buf, 4, 1, outFile);

    // Frame_Size
    writeLE32(buf, tableEntries.size() * 8 + ZSTD_seekTableFooterSize);
    fwrite(buf, 4, 1, outFile);

    if (verbose) {
        fprintf(stderr, "\n---- seek table ----\n");
        fprintf(stderr, "decompressed\tcompressed\n");
    }

    // Seek_Table_Entries
    for (const auto &e : tableEntries) {
        // Compressed_Size
        writeLE32(buf, e.compressedSize);
        fwrite(buf, 4, 1, outFile);

        // Decompressed_Size
        writeLE32(buf, e.decompressedSize);
        fwrite(buf, 4, 1, outFile);

        if (verbose)
            fprintf(stderr, "%u\t%u\n", e.decompressedSize, e.compressedSize);
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

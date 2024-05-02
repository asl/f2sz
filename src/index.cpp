/* ******************************************************************
 * f2sz
 * Copyright (c) 2024, Anton Korobeynikov
 *
 * This source code is licensed under the GPLv3 (found in the LICENSE
 * file in the root directory of this source tree).
 ****************************************************************** */

#include "index.h"
#include "utils.h"
#include <cstdio>
#include <zstd.h>
#include <cstdint>

void RecordIndex::write(FILE *outFile, bool verbose) {
    uint8_t buf[sizeof(size_t)];

    if (verbose) {
        fprintf(stderr, "\n---- index ----\n");
        fprintf(stderr, "name\tindex\tinput offset\n");
    }

    // Add index frame

    // Skippable_Magic_Number
    writeLE32(buf, ZSTD_MAGIC_SKIPPABLE_START | 0xF);
    fwrite(buf, 4, 1, outFile);

    // Determine frame size
    uint32_t frameSize = 0;
    for (const auto &entry : indexEntries) {
        frameSize += entry.name.size() + 1; // Zero terminated
        frameSize += 8 + 8; // idx, offset
    }
    frameSize += ZSTD_indexTableFooterSize; // footer: number of index entriees, reserved byte, magic

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

        if (verbose) {
            fwrite(entry.name.data(), entry.name.size(), 1, stderr);
            fputc('\t', stderr);

            fprintf(stderr, "%zu\t", entry.idx);
            fprintf(stderr, "%zu\n", entry.offset);
        }
    }

    // Index_Table_Footer
    // Number_Of_Entries
    writeLE32(buf, indexEntries.size());
    fwrite(buf, 4, 1, outFile);

    // Index_Table_Descriptor (reserved for later)
    buf[0] = 0;
    fwrite(buf, 1, 1, outFile);

    // Index_Magic_Number
    writeLE32(buf, ZSTD_FIDX_MAGICNUMBER); // 'FIDX'
    fwrite(buf, 4, 1, outFile);
}

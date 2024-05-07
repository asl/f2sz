/* ******************************************************************
 * f2sz
 * Copyright (c) 2024, Anton Korobeynikov
 *
 * This source code is licensed under the GPLv3 (found in the LICENSE
 * file in the root directory of this source tree).
 ****************************************************************** */

#ifndef _F2SZ_SEEKABLE_H_
#define _F2SZ_SEEKABLE_H_

#include <vector>
#include <cstdint>
#include <cstdio>

static constexpr unsigned ZSTD_seekTableFooterSize = 9;
static constexpr uint32_t ZSTD_SEEKABLE_MAGICNUMBER = 0x8F92EAB1;
static constexpr unsigned ZSTD_SEEKABLE_MAXFRAMES = 0x8000000U;
static constexpr size_t ZSTD_SEEKABLE_MAX_FRAME_DECOMPRESSED_SIZE = 0x40000000U;

struct SeekTableEntry {
    uint32_t compressedSize;
    uint32_t decompressedSize;
};

class SeekTable {
  public:
    void add(uint32_t compressedSize, uint32_t decompressedSize) {
        tableEntries.emplace_back(SeekTableEntry{compressedSize, decompressedSize});
    }

    size_t size() const { return tableEntries.size(); }
    bool read(FILE *inFile, size_t frameSize, bool verbose = false);
    void write(FILE *outFile, bool verbose = false);
    void print(FILE *outFile) const;

    auto &operator[](size_t idx) {  return tableEntries[idx]; }
    const auto &operator[](size_t idx) const {  return tableEntries[idx]; }

    auto &entries() { return tableEntries; };
    const auto &entries() const { return tableEntries; }

    size_t decompressedFrameSize(size_t targetFrame) const {
        return tableEntries.at(targetFrame).decompressedSize;
    }
    size_t compressedFrameSize(size_t targetFrame) const {
        return tableEntries.at(targetFrame).compressedSize;
    }


  private:
    std::vector<SeekTableEntry> tableEntries;
};


#endif // _F2SZ_SEEKABLE_H_

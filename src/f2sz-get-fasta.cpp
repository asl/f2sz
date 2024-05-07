/* ******************************************************************
 * f2sz
 * Copyright (c) 2024, Anton Korobeynikov
 *
 * This source code is licensed under the GPLv3 (found in the LICENSE
 * file in the root directory of this source tree).
 ****************************************************************** */

#include "index.h"
#include "seekable.h"
#include "utils.h"

#include <algorithm>
#include <memory.h>
#include <memory>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <zstd_errors.h>

enum class FrameError {
    success=0,
    frame_error=1,
    not_zstd=2,
    file_error=3,
    truncated_input=4
};

#define ERROR_IF(c,n,...) {            \
    if (c) {                           \
        fprintf(stderr, __VA_ARGS__);  \
        fprintf(stderr, " \n");        \
        return n;                      \
    }                                  \
}

struct Context {
    SeekTable table;
    RecordIndex index;
    bool verbose = false;
    size_t numActualFrames = 0;
    size_t numSkippableFrames = 0;
    std::vector<size_t> frameCompressedOffsets;
    std::vector<size_t> frameDecompressedOffsets;

    ZSTD_DStream* dstream;

    Context() {
        dstream = ZSTD_createDStream();
        ZSTD_initDStream(dstream);
    }

    ~Context() {
        ZSTD_freeDStream(dstream);
    }
};

#undef ERROR
#define ERROR(name) ((size_t)-ZSTD_error_##name)

size_t offsetToFrameNum(const Context &ctx, size_t offset) {
    fprintf(stderr, "off: %zu\n", offset);


    auto it = std::upper_bound(ctx.frameDecompressedOffsets.begin(), ctx.frameDecompressedOffsets.end(), offset);
    if (it == ctx.frameDecompressedOffsets.end())
        return size_t(-1);

    return it - ctx.frameDecompressedOffsets.begin() - 1;
}

size_t recordToFrameNum(const Context &ctx, size_t recordNum) {
    size_t frameNum = 0;
    const auto &entries = ctx.index.entries();
    for (size_t i = 0; i < entries.size(); ++i) {
        if (entries[i].idx > recordNum)
            break;
        frameNum = i;
    }

    return frameNum;
}

size_t decompressFrame(FILE *srcFile, Context &ctx, uint8_t *dst, unsigned targetFrame) {
    size_t decompressedFrameSize = ctx.table.decompressedFrameSize(targetFrame);
    std::vector<uint8_t> inBuf(ZSTD_BLOCKSIZE_MAX, 0);
    ZSTD_inBuffer in{inBuf.data(), 0, 0};
    ZSTD_outBuffer out{dst, decompressedFrameSize, 0};

    size_t res = ZSTD_DCtx_reset(ctx.dstream, ZSTD_reset_session_only);
    if (ZSTD_isError(res))
        return res;

    if (fseek(srcFile,
              ctx.frameCompressedOffsets[targetFrame],
              SEEK_SET) != 0)
        return ERROR(seekableIO);


    size_t decompressedSize = 0, noOutputProgressCount = 0;
    while (decompressedSize < decompressedFrameSize) {
        size_t prevOutPos = out.pos, prevInPos = in.pos;
        size_t toRead = ZSTD_decompressStream(ctx.dstream, &out, &in);
        if (ZSTD_isError(toRead))
            return toRead;

        size_t forwardProgress = out.pos - prevOutPos;
        if (forwardProgress == 0) {
            if (noOutputProgressCount++ > 16)
                return ERROR(seekableIO);
        } else {
            noOutputProgressCount = 0;
        }

        decompressedSize += forwardProgress;
        if (toRead == 0) // Frame complete
            break;

        // Read in more data if we're done with this buffer
        if (in.pos == in.size) {
            if (toRead > ZSTD_BLOCKSIZE_MAX)
                toRead = ZSTD_BLOCKSIZE_MAX;
            if (toRead != fread(inBuf.data(), 1, toRead, srcFile))
                return ERROR(seekableIO);
            in.size = toRead;
            in.pos = 0;
        }
    }

    if (decompressedSize != decompressedFrameSize)
        return ERROR(seekableIO);

    return decompressedSize;
}

FrameError enumFrames(FILE *srcFile, Context &ctx) {
    ctx.numActualFrames = 0, ctx.numSkippableFrames = 0;

    for ( ; ; ) {
        uint8_t headerBuffer[ZSTD_FRAMEHEADERSIZE_MAX];
        memset(headerBuffer, 0, sizeof(headerBuffer));

        const size_t numBytesRead = fread(headerBuffer, 1, sizeof(headerBuffer), srcFile);
        if (numBytesRead < ZSTD_FRAMEHEADERSIZE_MIN(ZSTD_f_zstd1)) {
            if (feof(srcFile) && numBytesRead == 0)
                break;  // correct end of file
            int res = feof(srcFile);
            ERROR_IF(res, FrameError::not_zstd, "ERROR: reached end of file with incomplete frame");
            ERROR_IF(true, FrameError::frame_error, "ERROR: did not reach end of file but ran out of frames");
        }

        {
            const uint32_t magicNumber = readLE32(headerBuffer);

            // Zstandard frame
            if (magicNumber == ZSTD_MAGICNUMBER) {
                ZSTD_frameHeader header;

                {
                    size_t res = ZSTD_getFrameHeader(&header, headerBuffer, numBytesRead);
                    ERROR_IF(res != 0, FrameError::frame_error, "ERROR: could not decode frame header");
                }

                // Move to the end of the frame header
                {
                    const size_t headerSize = ZSTD_frameHeaderSize(headerBuffer, numBytesRead);
                    ERROR_IF(ZSTD_isError(headerSize), FrameError::frame_error, "ERROR: could not determine frame header size");
                    int res = fseek(srcFile, ((long)headerSize)-((long)numBytesRead), SEEK_CUR);
                    ERROR_IF(res != 0, FrameError::frame_error, "ERROR: could not move to end of frame header");
                }

                // Skip all blocks in the frame
                {
                    bool lastBlock = false;
                    do {
                        uint8_t blockHeaderBuffer[3];
                        size_t numBlockBytesRead = fread(blockHeaderBuffer, 1, 3, srcFile);

                        ERROR_IF(numBlockBytesRead != 3,
                                 FrameError::frame_error, "Error while reading block header");
                        {   const uint32_t blockHeader = readLE24(blockHeaderBuffer);
                            const uint32_t blockTypeID = (blockHeader >> 1) & 3;
                            const uint32_t isRLE = (blockTypeID == 1);
                            const uint32_t isWrongBlock = (blockTypeID == 3);
                            const long blockSize = isRLE ? 1 : (long)(blockHeader >> 3);
                            ERROR_IF(isWrongBlock, FrameError::frame_error, "ERROR: unsupported block type");
                            lastBlock = blockHeader & 1;
                            int res = fseek(srcFile, blockSize, SEEK_CUR);
                            ERROR_IF(res != 0, FrameError::frame_error, "ERROR: could not skip to end of block");
                        }
                    } while (lastBlock != 1);
                }

                // Check if checksum is used
                {
                    const uint8_t frameHeaderDescriptor = headerBuffer[4];
                    const bool contentChecksumFlag = (frameHeaderDescriptor & (1 << 2)) >> 2;
                    uint32_t checksum;
                    if (contentChecksumFlag) {
                        ERROR_IF(fread(&checksum, 1, 4, srcFile) != 4,
                                 FrameError::frame_error, "ERROR: could not read checksum");
                    }
                }
                ctx.numActualFrames += 1;
            } else if ((magicNumber & ZSTD_MAGIC_SKIPPABLE_MASK) == ZSTD_MAGIC_SKIPPABLE_START) {  // Skippable frame
                const uint32_t frameSize = readLE32(headerBuffer + 4);
                const long seek = (long)(8 + frameSize - numBytesRead);
                int res = fseek(srcFile, seek, SEEK_CUR);
                ERROR_IF(res != 0, FrameError::frame_error, "ERROR: could not find end of skippable frame");
                ctx.numSkippableFrames += 1;

                // Seek back to magic
                if (frameSize > 4) {
                    res = fseek(srcFile, -4, SEEK_CUR);
                    ERROR_IF(res != 0, FrameError::frame_error, "ERROR: could not seek to read frame magic value");
                    uint8_t magicBuffer[4];
                    size_t numMagicBytesRead = fread(magicBuffer, 1, 4, srcFile);
                    ERROR_IF(numMagicBytesRead != 4,
                             FrameError::frame_error, "Error while reading frame magic value");
                    const uint32_t frameMagic = readLE32(magicBuffer);
                    if (frameMagic == ZSTD_SEEKABLE_MAGICNUMBER) {
                        res = fseek(srcFile, -(long)frameSize, SEEK_CUR);
                        ERROR_IF(res != 0, FrameError::frame_error, "ERROR: could not seek to read frame");

                        bool seekRead = ctx.table.read(srcFile, frameSize, ctx.verbose);
                        ERROR_IF(!seekRead, FrameError::frame_error, "ERROR: invalid seek table format");
                    } else if (frameMagic == ZSTD_FIDX_MAGICNUMBER) {
                        res = fseek(srcFile, -(long)frameSize, SEEK_CUR);
                        ERROR_IF(res != 0, FrameError::frame_error, "ERROR: could not seek to read frame");

                        bool seekRead = ctx.index.read(srcFile, frameSize, ctx.verbose);
                        ERROR_IF(!seekRead, FrameError::frame_error, "ERROR: invalid record index format");
                    } else {
                        if (ctx.verbose)
                            fprintf(stderr, "WARN: unknown magic value %08x:\n", magicNumber);
                    }
                }
            }  // something unknown
            else {
                ERROR_IF(true, FrameError::not_zstd, "ERROR: not a zstd frame");
            }
        }
    }

    // Final sanity checks:
    //  - Both record index and seek table filled in
    //  - Both have same sizes and the size corresponds # of actual frames
    ERROR_IF(ctx.table.size() != ctx.index.size(), FrameError::file_error,
             "ERROR: sizes of record index and seek table do not match");
    ERROR_IF(ctx.table.size() != ctx.numActualFrames, FrameError::file_error,
             "ERROR: number of frames in file do not match number of index entries");

    if (ctx.verbose)
        fprintf(stderr, "Total frames: %zu, skippable frames: %zu\n", ctx.numActualFrames, ctx.numSkippableFrames);

    // Transform frame sizes to frame offsets
    ctx.frameCompressedOffsets.reserve(ctx.table.size() + 1);
    ctx.frameCompressedOffsets.push_back(0);
    for (const auto &entry : ctx.table.entries())
        ctx.frameCompressedOffsets.push_back(ctx.frameCompressedOffsets.back() + entry.compressedSize);

    ctx.frameDecompressedOffsets.reserve(ctx.table.size() + 1);
    ctx.frameDecompressedOffsets.push_back(0);
    for (const auto &entry : ctx.table.entries())
        ctx.frameDecompressedOffsets.push_back(ctx.frameDecompressedOffsets.back() + entry.decompressedSize);

    return FrameError::success;
}

std::string_view getRecord(uint8_t *buf, size_t len, size_t recordNum) {
    size_t remaining = len;
    size_t idx = 0;
    while (remaining > 0) {
        // Advance over input buffer, we know there is at least 1 byte to check
        // Find FASTA header. Usually it should be just current symbol.
        uint8_t *hpos = (uint8_t*)memchr(buf, '>', remaining);
        // No more FASTA headers until the end of the input buffer, nothing to do
        if (hpos == NULL)
            return {};

        // Advance over '>'
        buf += 1; remaining -= 1;

        uint8_t *eol = (uint8_t*)memchr(buf, '\n', remaining);
        // No more newlines until EOF - likely a malformed entry
        // but we'd simply grab the whole chunk then
        if (eol == NULL) {
            if (idx == recordNum)
                return { (char*)hpos, size_t(buf + remaining - hpos) };

            break;
        }

        // Find the next header, if any
        uint8_t *hposNext = (uint8_t*)memchr(buf, '>', remaining);

        // See, if we are at desired record
        if (idx == recordNum) {
            // No more FASTA headers until the end of the input buffer, grab the
            // whole chunk
            if (hposNext == NULL)
                return { (char*)hpos, size_t(buf + remaining - hpos) };
            else  // Slice the buffer at hpos:hpos_next
                return { (char*)hpos, size_t(hposNext - hpos) };
            break;
        } else if (hposNext == NULL)
            // No more FASTA headers until the end of the input buffer, nothing to do
            return {};


        // Skip the record
        remaining = buf + remaining - hposNext;
        buf = hposNext;
        idx += 1;
    }

    return {};
}

static void version() {
  fprintf(
      stderr,
      "f2sz-get-fasta version %s\n"
      "Copyright (C) 2024 Anton Korobeynikov <anton+f2sz@korobeynikov.info>\n"
      "This software is distributed under the GPLv3 License\n"
      "THIS SOFTWARE IS PROVIDED \"AS IS\" WITHOUT ANY WARRANTY\n",
      VERSION);
}

static void usage(const char *naame, const char *fmt, ...) __attribute__ ((format (printf, 2, 3)));

static void usage(const char *name, const char *fmt, ...)  {
  if (fmt) {
      va_list ap;
      va_start(ap, fmt);
      vfprintf(stderr, fmt, ap);
      va_end(ap);
  }

  fprintf(stderr,
          "f2sz-get-fasta: FASTA 2 seekable zstd.\n"
          "Extract FASTA record by its number from f2sz-compressed file"
          "\n"
          "Usage: %1$s [OPTIONS...] [INPUT FILE]\n"
          "\n"
          "Options:\n"
          "\t-i N               FASTA record index to extract\n"
          "\t-v                 Verbose. List skip table and block boundaries.\n"
          "\t-h                 Print this help.\n"
          "\t-V                 Print the version.\n"
          "\n",
          name);
  version();
  exit(0);
}


int main(int argc, char **argv) {
    Context ctx;
    size_t recordNum;

    char *executable = argv[0];
    int ch;
    while ((ch = getopt(argc, argv, "i:vVh")) != -1) {
        switch (ch) {
        case 'i':
            recordNum = std::stoul(optarg);
            if (recordNum < 1)
                usage(executable, "ERROR: Invalid record number %zu, must be greater than 0",
                      recordNum);
            recordNum -= 1; // Convert to zero-based
            break;
        case 'v':
            ctx.verbose = true;
            break;
        case 'V':
            version();
            exit(0);
        case 'h':
        default:
            usage(executable, NULL);
            break;
        }
    }

    argc -= optind;
    argv += optind;

    if (argc < 1)
        usage(executable, "Not enough arguments\n");
    else if (argc > 1)
        usage(executable, "Too many arguments\n");

    char* inFilename = argv[0];
    if (access(inFilename, F_OK) != 0) {
        fprintf(stderr, "%s: File not found\n", inFilename);
        return 1;
    }

    std::unique_ptr<FILE, decltype(&fclose)>
            srcFile(fopen(inFilename, "rb"), fclose);
    if (!srcFile.get()) {
        fprintf(stderr, "%s: Cannot open file\n", inFilename);
        return 1;
    }

    // Enumerate the frames in the input file, filling the indices
    if (enumFrames(srcFile.get(), ctx) != FrameError::success)
        return -1;

    size_t frameNum = recordToFrameNum(ctx, recordNum);
    if (ctx.verbose)
        fprintf(stderr, "Record %zu belongs to frame: %zu\n",
                recordNum, frameNum);

    size_t decompressedSize = ctx.table.decompressedFrameSize(frameNum);
    std::unique_ptr<uint8_t> buf(new uint8_t[decompressedSize]);

    // Decompress the required frame
    size_t res = decompressFrame(srcFile.get(), ctx, buf.get(), frameNum);
    if (ZSTD_isError(res)) {
        fprintf(stderr, "ERROR: decompressing frame %zu failed, zstd error code: %zu\n",
                frameNum, -res);
        return res;
    }

    if (res != decompressedSize) {
        fprintf(stderr, "ERROR: unexpected decompressed frame size %zu vs %zu\n",
                res, decompressedSize);
        return -3;
    }

    // Look for the record in question
    auto str = getRecord(buf.get(), decompressedSize, recordNum - ctx.index[frameNum].idx);
    if (str.empty()) {
        fprintf(stderr, "ERROR: cannot find record %zu\n", recordNum + 1);
        return -4;
    }

    fwrite(str.data(), 1, str.size(), stdout);

    return 0;
}

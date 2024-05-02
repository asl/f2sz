/* ******************************************************************
 * f2sz
 * Copyright (c) 2020, Martinelli Marco
 * Copyright (c) 2023, Anton Korobeynikov
 *
 * This source code is licensed under the GPLv3 (found in the LICENSE
 * file in the root directory of this source tree).
 ****************************************************************** */

#include <string>

#include <fcntl.h>
#include <getopt.h>
#include <stdbool.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <sys/mman.h>
#include <unistd.h>
#include <zstd.h>

#include "index.h"
#include "seekable.h"
#include "utils.h"

typedef struct SeekTableEntry SeekTableEntry;

struct SeekTableEntry {
  uint32_t compressedSize;
  uint32_t decompressedSize;
  SeekTableEntry *next;
};

typedef struct {
  char *str;
  size_t len;
} StringRef;

typedef struct IndexEntry IndexEntry;
struct IndexEntry {
  StringRef name;
  size_t idx;
  size_t offset;
  IndexEntry *next;
};

typedef struct {
  IndexEntry *head;
  IndexEntry *tail;
} Index;

typedef enum {
    Raw = 0,
    Line,
    FASTA
} Mode;

typedef struct {
    // input parameters
    const char* inFilename;
    std::string outFilename;
    std::string idxFilename;
    uint8_t level;
    size_t minBlockSize;
    bool verbose;
    uint32_t workers;
    Mode mode;

    // input buffer
    size_t inBuffSize;
    uint8_t *inBuff;
    size_t entities;

    // output buffer
    FILE *outFile;
    size_t outBuffSize;
    void *outBuff;

    // output index
    Index blockIndex;
    Index recordIndex;
    FILE *outIndex;
    bool doIndex;
    bool fullIndex;
    bool skipExtIndex;

    // compression context
    ZSTD_CCtx *cctx;

    // seek table
    SeekTableEntry *seekTable;
    uint32_t numberOfFrames;
    bool skipSeekTable;
} Context;

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

static void writeSeekTable(Context *ctx) {
  uint8_t buf[4];

  // Skippable_Magic_Number
  writeLE32(buf, ZSTD_MAGIC_SKIPPABLE_START | 0xE);
  fwrite(buf, 4, 1, ctx->outFile);

  // Frame_Size
  writeLE32(buf, ctx->numberOfFrames * 8 + ZSTD_seekTableFooterSize);
  fwrite(buf, 4, 1, ctx->outFile);

  if (ctx->verbose) {
    fprintf(stderr, "\n---- seek table ----\n");
    fprintf(stderr, "decompressed\tcompressed\n");
  }

  // Seek_Table_Entries
  for (SeekTableEntry *e = ctx->seekTable; e; e = e->next) {
    // Compressed_Size
    writeLE32(buf, e->compressedSize);
    fwrite(buf, 4, 1, ctx->outFile);

    // Decompressed_Size
    writeLE32(buf, e->decompressedSize);
    fwrite(buf, 4, 1, ctx->outFile);

    if (ctx->verbose)
      fprintf(stderr, "%u\t%u\n", e->decompressedSize, e->compressedSize);
  }

  // Seek_Table_Footer
  // Number_Of_Frames
  writeLE32(buf, ctx->numberOfFrames);
  fwrite(buf, 4, 1, ctx->outFile);

  // Seek_Table_Descriptor
  buf[0] = 0;
  fwrite(buf, 1, 1, ctx->outFile);

  // Seekable_Magic_Number
  writeLE32(buf, ZSTD_SEEKABLE_MAGICNUMBER);
  fwrite(buf, 4, 1, ctx->outFile);
}

static SeekTableEntry *newSeekTableEntry(uint32_t compressedSize,
                                         uint32_t decompressedSize) {
  SeekTableEntry *e = new SeekTableEntry;
  memset(e, 0, sizeof(SeekTableEntry));
  e->compressedSize = compressedSize;
  e->decompressedSize = decompressedSize;
  return e;
}

static void seekTableAdd(Context *ctx, uint64_t compressedSize,
                         uint64_t decompressedSize) {
  if (ctx->skipSeekTable)
    return;

  ctx->numberOfFrames += 1;

  if (ctx->numberOfFrames >= ZSTD_SEEKABLE_MAXFRAMES) {
    ctx->skipSeekTable = true;
    fprintf(stderr,
            "Warning: Too many frames (%" PRIu32 "). Unable to generate the seek table.\n",
            ctx->numberOfFrames);
    return;
  }

  if (decompressedSize > ZSTD_SEEKABLE_MAX_FRAME_DECOMPRESSED_SIZE) {
    ctx->skipSeekTable = true;
    fprintf(
        stderr,
        "Warning: Input frame too big (%" PRIu64 "). Unable to generate the seek table.\n",
        decompressedSize);
    return;
  }

  if (!ctx->seekTable) {
    ctx->seekTable = newSeekTableEntry(compressedSize, decompressedSize);
  } else {
    SeekTableEntry *e = ctx->seekTable;
    for (; e->next; e = e->next) {
    }
    e->next = newSeekTableEntry(compressedSize, decompressedSize);
  }
}

static Context *newContext() {
  Context *ctx = new Context;
  memset(ctx, 0, sizeof(Context));
  ctx->level = 3;
  return ctx;
}

static void writeIndex(Context *ctx) {
    uint8_t buf[sizeof(size_t)];

    if (!ctx->doIndex)
        return;

    if (ctx->verbose) {
        fprintf(stderr, "\n---- index ----\n");
        fprintf(stderr, "name\tindex\tinput offset\n");
    }

    // Write index to separate text index file
    if (ctx->outIndex) {
        for (IndexEntry *e =
                     ctx->fullIndex ? ctx->recordIndex.head : ctx->blockIndex.head;
             e; e = e->next) {
            if (e->name.str) {
                fwrite(e->name.str, e->name.len, 1, ctx->outIndex);
                fputc('\t', ctx->outIndex);
                if (ctx->verbose) {
                    fwrite(e->name.str, e->name.len, 1, stderr);
                    fputc('\t', stderr);
                }
            }

            fprintf(ctx->outIndex,"%zu\t", e->idx);
            fprintf(ctx->outIndex, "%zu\n", e->offset);
            if (ctx->verbose) {
                fprintf(stderr, "%zu\t", e->idx);
                fprintf(stderr, "%zu\n", e->offset);
            }
        }
    }

    // Add index frame

    // Skippable_Magic_Number
    writeLE32(buf, ZSTD_MAGIC_SKIPPABLE_START | 0xF);
    fwrite(buf, 4, 1, ctx->outFile);

    // Determine frame size
    uint32_t frameSize = 0;
    for (IndexEntry *e = ctx->blockIndex.head; e; e = e->next) {
        frameSize += e->name.len + 1; // Zero terminated
        frameSize += 8 + 8; // idx, offset
    }
    frameSize += ZSTD_indexTableFooterSize; // footer: number of index entriees, reserved byte, magic

    // Frame_Size
    writeLE32(buf, frameSize);
    fwrite(buf, 4, 1, ctx->outFile);

    // Index_Table_Entries
    size_t entries = 0;
    for (IndexEntry *e = ctx->blockIndex.head; e; e = e->next) {
        fwrite(e->name.str, e->name.len, 1, ctx->outFile);
        fputc(0, ctx->outFile);

        writeLE64(buf, e->idx);
        fwrite(buf, 8, 1, ctx->outFile);

        writeLE64(buf, e->offset);
        fwrite(buf, 8, 1, ctx->outFile);

        entries += 1;
    }

    // Index_Table_Footer
    // Number_Of_Entries
    writeLE32(buf, entries);
    fwrite(buf, 4, 1, ctx->outFile);

    // Index_Table_Descriptor (reserved for later)
    buf[0] = 0;
    fwrite(buf, 1, 1, ctx->outFile);

    // Index_Magic_Number
    writeLE32(buf, ZSTD_FIDX_MAGICNUMBER); // 'FIDX'
    fwrite(buf, 4, 1, ctx->outFile);
}

static IndexEntry *newIndexEntry(StringRef name,
                                 size_t idx,
                                 size_t offset) {
  IndexEntry *e = new IndexEntry;
  memset(e, 0, sizeof(IndexEntry));
  e->name = name;
  e->idx = idx;
  e->offset = offset;
  return e;
}

static IndexEntry *indexAdd(Index *idx,
                            StringRef name, size_t pos,
                            size_t offset) {
    if (!idx->head) {
        idx->head = idx->tail = newIndexEntry(name, pos, offset);
    } else {
        IndexEntry *e = idx->tail;
        idx->tail = e->next = newIndexEntry(name, pos, offset);
    }

    return idx->tail;
}

static void prepareInput(Context *ctx) {
  int fd = open(ctx->inFilename, O_RDONLY, 0);
  if (fd < 0) {
    fprintf(stderr, "ERROR: Unable to open '%s'\n", ctx->inFilename);
    exit(1);
  }
  ctx->inBuffSize = lseek(fd, 0L, SEEK_END);

  ctx->inBuff =
          (uint8_t *)mmap(NULL, ctx->inBuffSize ? ctx->inBuffSize : 1,
                          PROT_READ, MAP_PRIVATE, fd, 0);
  if (ctx->inBuff == MAP_FAILED) {
    fprintf(stderr, "ERROR: Unable to mmap '%s'\n", ctx->inFilename);
    exit(1);
  }

  if (!ctx->minBlockSize)
      ctx->minBlockSize = ctx->inBuffSize < ZSTD_SEEKABLE_MAX_FRAME_DECOMPRESSED_SIZE ?
                          ctx->inBuffSize : ZSTD_SEEKABLE_MAX_FRAME_DECOMPRESSED_SIZE;

  close(fd);
}

static void prepareOutput(Context *ctx) {
  ctx->outFile = fopen(ctx->outFilename.c_str(), "wb");
  if (!ctx->outFile) {
    fprintf(stderr, "ERROR: Cannot open output file for writing\n");
    exit(1);
  }

  if (ctx->doIndex && !ctx->skipExtIndex) {
    ctx->outIndex = fopen(ctx->idxFilename.c_str(), "wt");
    if (!ctx->outIndex) {
      fprintf(stderr, "ERROR: Cannot open index file for writing\n");
      exit(1);
    }
  }
  ctx->outBuffSize = ZSTD_CStreamOutSize();
  ctx->outBuff = malloc(ctx->outBuffSize);
}

static void prepareCctx(Context *ctx) {
  ctx->cctx = ZSTD_createCCtx();
  if (ctx->cctx == NULL) {
    fprintf(stderr, "ERROR: Cannot create ZSTD CCtx\n");
    exit(1);
  }

  size_t err;
  err = ZSTD_CCtx_setParameter(ctx->cctx, ZSTD_c_compressionLevel, ctx->level);
  if (ZSTD_isError(err)) {
    fprintf(stderr, "ERROR: Cannot set compression level: %s\n",
            ZSTD_getErrorName(err));
    exit(1);
  }

  err = ZSTD_CCtx_setParameter(ctx->cctx, ZSTD_c_checksumFlag, 1);
  if (ZSTD_isError(err)) {
    fprintf(stderr, "ERROR: Cannot set checksum flag: %s\n",
            ZSTD_getErrorName(err));
    exit(1);
  }

  if (ctx->workers) {
    err = ZSTD_CCtx_setParameter(ctx->cctx, ZSTD_c_nbWorkers, ctx->workers);
    if (ZSTD_isError(err)) {
      fprintf(stderr, "ERROR: Multi-thread is supported only with libzstd >= "
                      "1.5.0 or on older versions compiled with "
                      "ZSTD_MULTITHREAD. Reverting to single-thread.\n");
      ctx->workers = 0;
      ZSTD_CCtx_setParameter(ctx->cctx, ZSTD_c_nbWorkers, ctx->workers);
    }
  }
}

typedef struct {
    uint8_t *buf;
    size_t size;
} Block;

static void roundBlockToInput(Block *block, const Context *ctx) {
    if (block->buf + block->size > ctx->inBuff + ctx->inBuffSize)
        block->size = ctx->inBuff + ctx->inBuffSize - block->buf;
}

// Look for `c` in input buffer starting from the end of block.
// Returns NULL is no `c` is found until the end of ctx->inBuff,
// otherwise - the position of `c`. Block end (block->size) is adjusted
// correspondingly. Note that `c` is not included into the block here.
static uint8_t *advanceUntil(Block *block, Context *ctx, int c) {
    size_t remaining = ctx->inBuff + ctx->inBuffSize - block->buf - block->size;
    uint8_t *pos = (uint8_t*)memchr(block->buf + block->size, c, remaining);
    if (pos == NULL) {
        // No more symbols until the end of the input buffer, grab the
        // whole chunk
        block->size += remaining;
    } else {
        // Advance block
        block->size = pos - block->buf;
    }

    return pos;
}


static void advanceBlock(Block *block, Context *ctx) {
    switch (ctx->mode) {
    default:
    case Raw:
        block->size = ctx->inBuffSize;
        ctx->entities += 1;
        break;
    case Line: {
        block->size = 0;
        IndexEntry *indexEntry = NULL;
        while (block->size < ctx->minBlockSize) {
            // End of input buffer
            if (block->buf + block->size >= ctx->inBuff + ctx->inBuffSize)
                break;

            if (ctx->doIndex) {
                StringRef dummy = { NULL, 0};
                if (indexEntry == NULL)
                    indexEntry = indexAdd(&ctx->blockIndex, dummy,
                                          ctx->entities,
                                          block->buf + block->size - ctx->inBuff);
                if (ctx->fullIndex)
                    indexAdd(&ctx->recordIndex, dummy,
                             ctx->entities,
                             block->buf + block->size - ctx->inBuff);
            }

            // Advance over input buffer counting lines, we know there is at
            // least 1 byte to check
            uint8_t *pos = advanceUntil(block, ctx, '\n');

            // No more newlines until the end of the input buffer
            if (pos == NULL)
                break;

            // Advance over newline, `pos` points to newline here
            block->size += 1;
            ctx->entities += 1;
        }

        break;
    }
    case FASTA: {
        block->size = 0;
        IndexEntry *indexEntry = NULL;
        while (block->size < ctx->minBlockSize) {
            // End of input buffer
            if (block->buf + block->size >= ctx->inBuff + ctx->inBuffSize)
                break;

            // Advance over input buffer, we know there is at least 1 byte to check
            // Find FASTA header. Usually it should be just current symbol.
            // On success block is advanced to '>' position (just before it)
            uint8_t *hpos = advanceUntil(block, ctx, '>');

            // No more FASTA headers until the end of the input buffer, grab the
            // whole chunk
            if (hpos == NULL)
                break;

            // Advance over '>'
            block->size += 1;

            // See if we need to record the name of the sequence
            if (ctx->doIndex) {
                uint8_t *eol = advanceUntil(block, ctx, '\n');
                // No more newlines until EOF - likely malformed entry
                // but we'd simply grab the whole chunk then
                if (eol == NULL)
                    break;

                // See if there is a comment here
                size_t hlen = eol - hpos - 1;
                uint8_t *space = (uint8_t*)memchr(hpos, ' ', hlen);
                if (space != NULL)
                    hlen = space - hpos - 1;

                StringRef name = { (char*)hpos + 1, hlen };

                // If this is first record in block, record it's location in block index
                if (indexEntry == NULL)
                    indexEntry = indexAdd(&ctx->blockIndex,
                                          name, ctx->entities, hpos - ctx->inBuff);

                // If we're producing a complete (record) index, record the location
                if (ctx->fullIndex)
                    indexAdd(&ctx->recordIndex,
                             name, ctx->entities, hpos - ctx->inBuff);
            }

            // Find the next header, if any
            uint8_t *hpos_next = advanceUntil(block, ctx, '>');

            // No more FASTA headers until the end of the input buffer, grab the
            // whole chunk
            if (hpos_next == NULL)
                break;

            // Block is already properly sized to before '>' position, including
            // the newline
            ctx->entities += 1;
        }

        break;
    }
    }

    roundBlockToInput(block, ctx);
}

static bool nextBlock(Block *block, Context *ctx) {
    if (block->buf == NULL) { // Very first block, determine appropriate block size
        block->buf = ctx->inBuff;
        advanceBlock(block, ctx);
    } else { // Advance the block pointer
        // Case 1: current block is the last one
        if (block->buf + block->size >= ctx->inBuff + ctx->inBuffSize)
            return false;

        // Case 2: really advance the block pointer
        block->buf += block->size;
        advanceBlock(block, ctx);
    }

    return true;
}

static void compressFile(Context *ctx) {
    prepareInput(ctx);
    prepareOutput(ctx);
    prepareCctx(ctx);

    Block block = { NULL, 0 };
    size_t records = 0;
    while (nextBlock(&block, ctx)) {
        ZSTD_CCtx_setPledgedSrcSize(ctx->cctx, block.size);

        if (ctx->verbose) {
            fprintf(stderr, "# END OF BLOCK (%zu, %zu, %zu)\n\n",
                    block.size, ctx->entities, ctx->entities - records);
            records = ctx->entities;
        }

        // Sanity check
        if (block.buf + block.size > ctx->inBuff + ctx->inBuffSize) {
            fprintf(stderr, "FATAL ERROR: This is a bug. Please, report it.\n");
            exit(-1);
        }

        ZSTD_inBuffer input = { block.buf, block.size, 0};
        size_t remaining;
        ZSTD_EndDirective mode;
        uint64_t compressedSize = 0;
        do {
            ZSTD_outBuffer output = {ctx->outBuff, ctx->outBuffSize, 0};
            mode = input.pos < input.size ? ZSTD_e_continue : ZSTD_e_end;
            remaining = ZSTD_compressStream2(ctx->cctx, &output, &input, mode);
            if (ZSTD_isError(remaining)) {
                fprintf(stderr, "ERROR: Can't compress stream: %s\n",
                        ZSTD_getErrorName(remaining));
                exit(1);
            }
            compressedSize += fwrite(ctx->outBuff, 1, output.pos, ctx->outFile);
        } while (mode == ZSTD_e_continue || remaining > 0);

        seekTableAdd(ctx, compressedSize, block.size);
    }

    if (ctx->doIndex)
        writeIndex(ctx);
    if (!ctx->skipSeekTable)
        writeSeekTable(ctx);

    ZSTD_freeCCtx(ctx->cctx);
    fclose(ctx->outFile);
    if (ctx->outIndex)
        fclose(ctx->outIndex);
    free(ctx->outBuff);
    munmap(ctx->inBuff, ctx->inBuffSize);
}

static std::string getOutFilename(std::string_view inFilename) {
    return std::string(inFilename) + ".zst";
}

static std::string getIndexFilename(std::string_view outFilename) {
    return std::string(outFilename) + ".idx";
}

static void version() {
  fprintf(
      stderr,
      "f2sz version %s\n"
      "Copyright (C) 2020 Marco Martinelli <marco+t2sz@13byte.com>\n"
      "Copyright (C) 2023 Anton Korobeynikov <anton+t2sz@korobeynikov.info>\n"
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
          "f2sz: FASTA 2 seekable zstd.\n"
          "It compress a file into a seekable zstd, splitting the file into "
          "multiple frames.\n"
          "The compressed archive can be decompressed with any Zstandard tool, "
          "including zstd.\n"
          "\nTo take advantage of seeking see the following projects:\n"
          "\tC/C++ library:  https://github.com/martinellimarco/libzstd-seek\n"
          "\tPython library: https://github.com/martinellimarco/indexed_zstd\n"
          "\n"
          "Usage: %1$s [OPTIONS...] [INPUT FILE]\n"
          "\n"
          "Examples:\n"
          "\t%1$s any.file -f 10M                        Compress any.file to "
          "any.file.zst, each input block will be of 10M\n"
          "\t%1$s contigs.fasta -f -F -i 10M             Compress contigs.fasta to "
          "contigs.fasta.zst, each input block will \n"
          "\t                                              be at least 10M aligned to FASTA "
          " boundaries. Index for blocks is generated as well\n"
          "\t%1$s any.file -o output.file.zst            Compress any.file to "
          "any.file.zst\n"
          "\t%1$s any.file -o /dev/stdout                Compress any.file to "
          "standard output\n"
          "\n"
          "Options:\n"
          "\t-l [1..22]         Set compression level, from 1 (lower) to 22 "
          "(highest). Default is 3.\n"
          "\t-o FILENAME        Output file name.\n"
          "\t-b SIZE            In raw mode: the exact size of each input "
          "block, except the last one.\n"
          "\t                   In other modes: the minimum size of each input block.\n"
          "\t                   The greater is SIZE the smaller will be the "
          "archive at the expense of the seek speed.\n"
          "\t                   SIZE may be followed by the following "
          "multiplicative suffixes:\n"
          "\t                       k/K/KiB = 1024\n"
          "\t                       M/MiB = 1024^2\n"
          "\t                       G/GiB = 1024^3\n"
          "\t                       kB/KB = 1000\n"
          "\t                       MB = 1000^2\n"
          "\t                       GB = 1000^3\n"
          "\t-L                 Line mode: align blocks to line boundaries.\n"
          "\t-F                 FASTA mode: align blocks to FASTA records boundaries.\n"
          "\t-T [1..N]          Number of thread to spawn. It improves "
          "compression speed but cost more memory. Default is single thread.\n"
          "\t                   It requires libzstd >= 1.5.0 or an older "
          "version compiled with ZSTD_MULTITHREAD.\n"
          "\t                   If `-b` is too small it is possible "
          "that a lower number of threads will be used.\n"
          "\t-i                 Generate index table for blocks.\n"
          "\t-I                 Generate index table for records / lines.\n"
          "\t-J                 Do not generate external index in .idx file\n"
          "\t-j                 Do not generate a seek table.\n"
          "\t-v                 Verbose. List skip table and block boundaries.\n"
          "\t-f                 Overwrite output without prompting.\n"
          "\t-h                 Print this help.\n"
          "\t-V                 Print the version.\n"
          "\n",
          name);
  version();
  exit(0);
}

int main(int argc, char **argv) {
  Context *ctx = newContext();
  bool overwrite = false;
  char *executable = argv[0];

  int ch;
  while ((ch = getopt(argc, argv, "l:o:b:T:LFjiIJVfvh")) != -1) {
    switch (ch) {
    case 'l':
      ctx->level = atoi(optarg);
      if (ctx->level < ZSTD_minCLevel() || ctx->level > ZSTD_maxCLevel()) {
        usage(executable, "ERROR: Invalid level. Must be between %u and %u.",
              ZSTD_minCLevel(), ZSTD_maxCLevel());
      }
      break;
    case 'o':
      ctx->outFilename = optarg;
      break;
    case 'b': {
      size_t multiplier = decodeMultiplier(optarg);
      ctx->minBlockSize = atoi(optarg) * multiplier;
      if (ctx->minBlockSize < multiplier) {
        usage(executable, "ERROR: Invalid block size");
      }
      if (ctx->minBlockSize > ZSTD_SEEKABLE_MAX_FRAME_DECOMPRESSED_SIZE) {
          usage(executable, "ERROR: Invalid block size. Must be %u or less",
                ZSTD_SEEKABLE_MAX_FRAME_DECOMPRESSED_SIZE);
      }

      break;
    }
    case 'T':
      ctx->workers = atoi(optarg);
      if (ctx->workers < 1) {
        usage(executable,
              "ERROR: Invalid number of threads. Must be greater than 0.");
      }
      break;
    case 'j':
      ctx->skipSeekTable = true;
      break;
    case 'L':
      ctx->mode = Line;
      break;
    case 'F':
      ctx->mode = FASTA;
      break;
    case 'I':
      ctx->fullIndex = true;
      // fallthrough
    case 'i':
      ctx->doIndex = true;
      break;
    case 'J':
      ctx->skipExtIndex = true;
      break;
    case 'v':
      ctx->verbose = true;
      break;
    case 'f':
      overwrite = true;
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

  ctx->inFilename = argv[0];
  if (access(ctx->inFilename, F_OK) != 0) {
    fprintf(stderr, "%s: File not found\n", ctx->inFilename);
    return 1;
  }

  if (ctx->outFilename.empty())
    ctx->outFilename = getOutFilename(ctx->inFilename);

  if (ctx->doIndex && ctx->mode != Line && ctx->mode != FASTA) {
    fprintf(stderr, "Index emission is supported only in line (-L) and FASTA (-F) mode\n");
    return 1;
  }

  if (!overwrite && access(ctx->outFilename.c_str(), F_OK) == 0) {
    char ans;
    fprintf(stderr, "%s already exists. Overwrite? [y/N]: ", ctx->outFilename.c_str());
    int res = scanf(" %c", &ans);
    if (res && ans != 'y') {
      return 0;
    }
  }

  if (!overwrite && access(ctx->outFilename.c_str(), F_OK) == 0) {
    char ans;
    fprintf(stderr, "%s already exists. Overwrite? [y/N]: ", ctx->outFilename.c_str());
    int res = scanf(" %c", &ans);
    if (res && ans != 'y') {
      return 0;
    }
  }

  if (ctx->doIndex && !ctx->skipExtIndex) {
    ctx->idxFilename = getIndexFilename(ctx->outFilename);
    if (!overwrite && access(ctx->idxFilename.c_str(), F_OK) == 0) {
      char ans;
      fprintf(stderr, "%s already exists. Overwrite? [y/N]: ", ctx->idxFilename.c_str());
      int res = scanf(" %c", &ans);
      if (res && ans != 'y') {
        return 0;
      }
    }
  }

  compressFile(ctx);

  // We leak lots of things here for the sake of simplicity

  return 0;
}

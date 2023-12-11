/* ******************************************************************
 * f2sz
 * Copyright (c) 2020, Martinelli Marco
 * Copyright (c) 2023, Anton Korobeynikov
 *
 * This source code is licensed under the GPLv3 (found in the LICENSE
 * file in the root directory of this source tree).
 ****************************************************************** */

#include <fcntl.h>
#include <getopt.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <zstd.h>

#include "utils.h"

typedef struct SeekTableEntry SeekTableEntry;

struct SeekTableEntry {
  uint32_t compressedSize;
  uint32_t decompressedSize;
  SeekTableEntry *next;
};

typedef enum {
    Raw = 0,
    Line,
    FASTA
} Mode;

typedef struct {
    // input parameters
    const char *inFilename;
    char *outFilename;
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

static void writeSeekTable(Context *ctx) {
  uint8_t buf[4];

  // Skippable_Magic_Number
  writeLE32(buf, ZSTD_MAGIC_SKIPPABLE_START | 0xE);
  fwrite(buf, 4, 1, ctx->outFile);

  // Frame_Size
  writeLE32(buf, ctx->numberOfFrames * 8 + 9);
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
  writeLE32(buf, 0x8F92EAB1);
  fwrite(buf, 4, 1, ctx->outFile);
}

static SeekTableEntry *newSeekTableEntry(uint32_t compressedSize,
                                         uint32_t decompressedSize) {
  SeekTableEntry *e = malloc(sizeof(SeekTableEntry));
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

  if (ctx->numberOfFrames >= 0x8000000U) {
    ctx->skipSeekTable = true;
    fprintf(stderr,
            "Warning: Too many frames. Unable to generate the seek table.\n");
    return;
  }

  if (decompressedSize >= 0x80000000U) {
    ctx->skipSeekTable = true;
    fprintf(
        stderr,
        "Warning: Input frame too big. Unable to generate the seek table.\n");
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
  Context *ctx = malloc(sizeof(Context));
  memset(ctx, 0, sizeof(Context));
  ctx->level = 3;
  return ctx;
}

static void prepareInput(Context *ctx) {
  int fd = open(ctx->inFilename, O_RDONLY, 0);
  if (fd < 0) {
    fprintf(stderr, "ERROR: Unable to open '%s'\n", ctx->inFilename);
    exit(1);
  }
  ctx->inBuffSize = lseek(fd, 0L, SEEK_END);

  ctx->inBuff =
      (uint8_t *)mmap(NULL, ctx->inBuffSize, PROT_READ, MAP_PRIVATE, fd, 0);
  if (ctx->inBuff == MAP_FAILED) {
    fprintf(stderr, "ERROR: Unable to mmap '%s'\n", ctx->inFilename);
    exit(1);
  }
  close(fd);
}

static void prepareOutput(Context *ctx) {
  ctx->outFile = fopen(ctx->outFilename, "wb");
  if (!ctx->outFile) {
    fprintf(stderr, "ERROR: Cannot open output file for writing\n");
    exit(1);
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
    uint8_t *pos = memchr(block->buf + block->size, c, remaining);
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
        block->size = ctx->minBlockSize ? ctx->minBlockSize : ctx->inBuffSize;
        ctx->entities += 1;
        break;
    case Line: {
        if (!ctx->minBlockSize) {
            block->size = ctx->inBuffSize;
            break;
        }

        block->size = 0;
        while (block->size < ctx->minBlockSize) {
            // End of input buffer
            if (block->buf + block->size >= ctx->inBuff + ctx->inBuffSize)
                break;

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
        if (!ctx->minBlockSize) {
            block->size = ctx->inBuffSize;
            break;
        }

        block->size = 0;
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

            // Find the next header, if any
            block->size += 1;
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
    while (nextBlock(&block, ctx)) {
        ZSTD_CCtx_setPledgedSrcSize(ctx->cctx, block.size);

        if (ctx->verbose)
            fprintf(stderr, "# END OF BLOCK (%zu, %zu)\n\n", block.size, ctx->entities);

        // Sanity check
        if (block.buf + block.size > ctx->inBuff + ctx->inBuffSize) {
            fprintf(stderr, "FATAL ERROR: This is a bug. Please, report it.\n");
            exit(-1);
        }

        ZSTD_inBuffer input = { block.buf, block.size, 0};
        size_t remaining;
        mode_t mode;
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

    if (!ctx->skipSeekTable)
        writeSeekTable(ctx);

    ZSTD_freeCCtx(ctx->cctx);
    fclose(ctx->outFile);
    free(ctx->outBuff);
    munmap(ctx->inBuff, ctx->inBuffSize);
}

static char *getOutFilename(const char *inFilename) {
  const size_t size = strlen(inFilename) + 5;
  void *const buff = malloc(size);
  memset(buff, 0, size);
  strcat(buff, inFilename);
  strcat(buff, ".zst");
  return (char *)buff;
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

static void usage(const char *name, const char *str) {
  if (str)
    fprintf(stderr, "%s\n\n", str);

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
          "Usage: %1$s [OPTIONS...] [TAR ARCHIVE]\n"
          "\n"
          "Examples:\n"
          "\t%1$s any.file -s 10M                        Compress any.file to "
          "any.file.zst, each input block will be of 10M\n"
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
          "\t-T [1..N]          Number of thread to spawn. It improves "
          "compression speed but cost more memory. Default is single thread.\n"
          "\t                   It requires libzstd >= 1.5.0 or an older "
          "version compiled with ZSTD_MULTITHREAD.\n"
          "\t                   If `-s` or `-S` are too small it is possible "
          "that a lower number of threads will be used.\n"
          "\t-j                 Do not generate a seek table.\n"
          "\t-v                 Verbose. List the elements in the tar archive "
          "and their size.\n"
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
  while ((ch = getopt(argc, argv, "l:o:b:T:LFjVfvh")) != -1) {
    switch (ch) {
    case 'l':
      ctx->level = atoi(optarg);
      if (ctx->level < ZSTD_minCLevel() || ctx->level > ZSTD_maxCLevel()) {
        usage(executable, "ERROR: Invalid level. Must be between 1 and 22.");
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
    usage(executable, "Not enough arguments");
  else if (argc > 1)
    usage(executable, "Too many arguments");

  ctx->inFilename = argv[0];
  if (access(ctx->inFilename, F_OK) != 0) {
    fprintf(stderr, "%s: File not found\n", ctx->inFilename);
    return 1;
  }

  if (ctx->outFilename == NULL)
    ctx->outFilename = getOutFilename(ctx->inFilename);

  if (!overwrite && access(ctx->outFilename, F_OK) == 0) {
    char ans;
    fprintf(stderr, "%s already exists. Overwrite? [y/N]: ", ctx->outFilename);
    int res = scanf(" %c", &ans);
    if (res && ans != 'y') {
      return 0;
    }
  }

  compressFile(ctx);

  free(ctx);

  return 0;
}

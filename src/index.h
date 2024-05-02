#ifndef _F2SZ_INDEX_H_
#define _F2SZ_INDEX_H_

#include <string_view>
#include <cstdint>

static constexpr unsigned ZSTD_indexTableFooterSize = 9;
static constexpr uint32_t ZSTD_FIDX_MAGICNUMBER = 0x46494458; // 'FIDX'

struct IndexEntry {
    std::string_view name;
    size_t idx;
    size_t offset;
};

class RecordIndex {
  public:
    void add(std::string_view name, size_t idx, size_t offset) {
        indexEntries.emplace_back(IndexEntry{name, idx, offset});
    }
    size_t size() const { return indexEntries.size(); }
    void write(FILE *outFile, bool verbose = false);

  private:
    std::vector<IndexEntry> indexEntries;
};


#endif // _F2SZ_INDEX_H_

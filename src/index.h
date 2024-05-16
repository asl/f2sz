#ifndef _F2SZ_INDEX_H_
#define _F2SZ_INDEX_H_

#include <string_view>
#include <string>
#include <vector>
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
    bool read(FILE *inFile, size_t frameSize, bool verbose);
    void write(FILE *outFile, bool verbose = false);
    void print(FILE *outFile) const;

    auto &operator[](size_t idx) {  return indexEntries[idx]; }
    const auto &operator[](size_t idx) const {  return indexEntries[idx]; }

    auto &entries() { return indexEntries; };
    const auto &entries() const { return indexEntries; }

    void clear() {
        indexEntries.clear();
        stringCache.clear();
    }

  private:
    std::vector<IndexEntry> indexEntries;
    // Normally index entries are string views, however, in some cases
    // (e.g. read from file) we need to own string. Then they are stored in this
    // cache. We cannot use std::string here as due to SSO their locations
    // could change
    std::vector<std::unique_ptr<char, decltype(&std::free)>> stringCache;
};


#endif // _F2SZ_INDEX_H_

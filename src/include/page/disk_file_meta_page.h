#ifndef MINISQL_DISK_FILE_META_PAGE_H
#define MINISQL_DISK_FILE_META_PAGE_H

#include <cstdint>

#include "page/bitmap_page.h"
// MAX_VALID_PAGE_ID 最多能够使用的块的数量
static constexpr page_id_t MAX_VALID_PAGE_ID = (PAGE_SIZE - 8) / 4 * BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();

class DiskFileMetaPage {
public:
  uint32_t GetExtentNums() {
    return num_extents_;
  }

  uint32_t GetAllocatedPages() {
    return num_allocated_pages_;
  }

  uint32_t GetExtentUsedPage(uint32_t extent_id) {
    ASSERT(MAX_VALID_PAGE_ID > extent_id,"extent_id cannot be larger than MAX_VALID_PAGE_ID");
    if (extent_id >= num_extents_) {
      return 0;
    }
    return extent_used_page_[extent_id];
  }

public:
 // 已经分配的逻辑磁盘页的数量，在分配和删除磁盘逻辑页的时候变化
  uint32_t num_allocated_pages_{0};
  // 使用的块的数量， 在块不够的时候变化
  uint32_t num_extents_{0};   // each extent consists with a bit map and BIT_MAP_SIZE pages
  // 各个磁盘块中已经使用的页的数量
  uint32_t extent_used_page_[0];
};

#endif //MINISQL_DISK_FILE_META_PAGE_H

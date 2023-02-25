#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
  meta = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  for (int i = 0; i < static_cast<int>(meta->GetExtentNums()); i++) {
    if (meta->GetExtentUsedPage(i) < BITMAP_SIZE) {
      tailBitMap = i;
      break;
    }
  }
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  // ASSERT(false, "Not implemented yet.");
  // 如果使用的页面超过当前所有块的容量，新增块
  // 空间优先，未来可以改为时间优先（比如不搜索，在满的时候直接分配新的页面,直接看最后一个块的使用情况）
  uint32_t result = 0;
  if (tailBitMap == -1 || meta->GetAllocatedPages() >= meta->GetExtentNums() * BITMAP_SIZE) {
    // 新增磁盘块
    // std::cout << "allocating new pages" << std::endl;
    // std::cout << "meta->GetAllocatedPages() is " << meta->GetAllocatedPages()
    // << std::endl; std::cout << "meta->GetExtentNums() * BITMAP_SIZE is " <<
    // meta->GetExtentNums() * BITMAP_SIZE << std::endl;
    BitmapPage<PAGE_SIZE> *new_bitmap_page = new BitmapPage<PAGE_SIZE>;
    //    char empty[PAGE_SIZE] = {0};
    int temp = meta->GetExtentNums();
    WritePhysicalPage(temp * (BITMAP_SIZE + 1) + 1, reinterpret_cast<char *>(new_bitmap_page));
    bitmap_cache_.emplace(temp * (BITMAP_SIZE + 1) + 1, new_bitmap_page);
    //    for(uint32_t i = 0; i < BITMAP_SIZE;i++){
    //    WritePhysicalPage(temp * (BITMAP_SIZE + 1) + 2 + i,empty);
    //    }
    tailBitMap = temp;
    meta->num_extents_++;
  }
  while (tailBitMap < static_cast<int>(meta->GetExtentNums())) {
    if (meta->GetExtentUsedPage(tailBitMap) < BITMAP_SIZE) {
      break;
    }
    tailBitMap++;
  }
  // 位图置位
  meta->extent_used_page_[tailBitMap]++;
  GetBitMapPage(tailBitMap * (BITMAP_SIZE + 1) + 2)->AllocatePage(result);
  result += tailBitMap * BITMAP_SIZE;
  // 增加块和已经分配的逻辑页面数量的数量
  meta->num_allocated_pages_++;
  // for debug
  return result;
}
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  //  ASSERT(false, "Not implemented yet.");
  // 减少meta中的allocatedpaed计数器
  // 设定bitmap对应位置的布尔值
  ASSERT(logical_page_id >= 0, "logical page id cannot be less than 0");
  auto temp = GetBitMapPage(logical_page_id);
  //  std::cout << "page to delete is " << logical_page_id << std::endl;
  if (temp->DeAllocatePage((logical_page_id % BITMAP_SIZE))) {
    // std::cout << "deleted !" << std::endl;
    meta->num_allocated_pages_--;
    // TODO 或许在bitmap为空的时候删除块？meta->ext或许需要改变
    meta->extent_used_page_[logical_page_id / BITMAP_SIZE]--;
    if (tailBitMap > static_cast<int>(logical_page_id / BITMAP_SIZE)) {
      tailBitMap = logical_page_id / BITMAP_SIZE;
    }
  }
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  ASSERT(logical_page_id >= 0, "logical_page_id cannot be less than 0");
  return GetBitMapPage(logical_page_id)->IsPageFree(logical_page_id % BITMAP_SIZE);
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id / BITMAP_SIZE + 2 + logical_page_id;
}
uint32_t DiskManager::LogicalToBitMapPageId(page_id_t logical_page_id) { return logical_page_id / BITMAP_SIZE; }
page_id_t DiskManager::PhysicalToLogicalPageId(page_id_t physical_page_id) {
  if (physical_page_id == 0 || ((physical_page_id - 1) % BITMAP_SIZE) == 0) {
    return -1;
  }
  if (physical_page_id % (BITMAP_SIZE + 1) == 0) {
    return physical_page_id - physical_page_id / (BITMAP_SIZE + 1) - 1;
  } else {
    return physical_page_id - physical_page_id / (BITMAP_SIZE + 1) - 2;
  }
}
BitmapPage<PAGE_SIZE> *DiskManager::GetBitMapPage(uint32_t logical_page_index) {
  auto bitmap_index_ = LogicalToBitMapPageId(logical_page_index);
  bitmap_index_ = bitmap_index_ * (BITMAP_SIZE + 1) + 1;
  auto temp = bitmap_cache_.find(bitmap_index_);
  if (temp != bitmap_cache_.end()) {
    return temp->second;
  }
  BitmapPage<PAGE_SIZE> *bitMapPagePointer = new BitmapPage<PAGE_SIZE>;
  ReadPhysicalPage(bitmap_index_, reinterpret_cast<char *>(bitMapPagePointer));
  bitmap_cache_.emplace(bitmap_index_, bitMapPagePointer);
  return bitMapPagePointer;
}
int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
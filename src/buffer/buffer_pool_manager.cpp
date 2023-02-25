#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  auto temp = page_table_.find(page_id);
  if (temp != page_table_.end()) {
    // exists, pin it and return it immediately.
    auto frame_id = temp->second;
    replacer_->Pin(frame_id);
    auto result = pages_ + frame_id;
    result->pin_count_++;
    return result;
  }
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    // pop a frame from free_list
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (replacer_->Victim(&frame_id)) {
      // check if it's dirty
      auto old_pointer = pages_ + frame_id;
      if (old_pointer->is_dirty_) {
        disk_manager_->WritePage(old_pointer->GetPageId(), old_pointer->GetData());
        old_pointer->is_dirty_ = false;
      }
      // Delete old from the page table
      page_table_.erase(old_pointer->GetPageId());
    } else {
      // all busy
      std::cout << "all page busy?" << std::endl;
      return nullptr;
    }
  }
  auto result = pages_ + frame_id;
  result->pin_count_ = 1;
  result->is_dirty_ = false;
  result->page_id_ = page_id;
  page_table_[page_id] = frame_id;
  disk_manager_->ReadPage(page_id, result->GetData());
  return result;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t frame_id = -1;
  //  std::cout << "input page_id is " << page_id << std::endl;
  //  std::cout << "\tand free list is " << (free_list_.empty() ? "empty" : "not empty") << std::endl;
  //  std::cout << "\tand replacer is " << replacer_->Size() << std::endl;

  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (replacer_->Victim(&frame_id)) {
      //      std::cout << "\tenters replacer";
      // check if it is dirty
      auto old_pointer = pages_ + frame_id;
      if (old_pointer->IsDirty()) {
        disk_manager_->WritePage(old_pointer->GetPageId(), old_pointer->GetData());
        old_pointer->is_dirty_ = false;
      }
      // delete original record in map
      page_table_.erase(old_pointer->GetPageId());
      //      std::cout << "victim is " << old_pointer->page_id_ << " new is " << page_id << std::endl;
    } else {
      // all are busy
      return nullptr;
    }
  }
  if (frame_id == -1) {
    return nullptr;
  }
  page_id = AllocatePage();
  auto result = pages_ + frame_id;
  result->ResetMemory();
  result->pin_count_ = 1;
  result->is_dirty_ = false;
  result->page_id_ = page_id;
  page_table_[page_id] = frame_id;
  //  std::cout << "\tresult page_id is " << result->page_id_ << std::endl;
  return result;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  auto temp = page_table_.find(page_id);
  if (temp != page_table_.end()) {
    DeallocatePage(page_id);
    auto frame_id = temp->second;
    Page *page_pointer = &pages_[frame_id];
    if (page_pointer->GetPinCount() != 0) {
      // If P exists, but has a non-zero pin-count, return false. Someone is using the page.
      return false;
    }
    if (page_pointer->is_dirty_) {
      // dirty, write it to disk
      disk_manager_->WritePage(page_id, page_pointer->GetData());
      page_pointer->is_dirty_ = false;
    }
    page_table_.erase(page_id);
    replacer_->Pin(frame_id);
    // TODO reset page?
    free_list_.push_back(frame_id);
    return true;
  }
  // not exist, return true
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  auto temp = page_table_.find(page_id);
  if (temp != page_table_.end()) {
    auto frame_id = temp->second;
    auto page_pointer = pages_ + frame_id;
    if (is_dirty) {
      page_pointer->is_dirty_ = true;
    }
    page_pointer->pin_count_--;
    if (page_pointer->pin_count_ == 0) {
      replacer_->Unpin(frame_id);
    }
    return true;
  }
  return false;
}
// 将page的信息写入磁盘，无论其是否为脏页
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  auto temp = page_table_.find(page_id);
  if (temp != page_table_.end()) {
    auto frame_id = temp->second;
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
    return true;
  }
  return false;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) { disk_manager_->DeAllocatePage(page_id); }

bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
size_t BufferPoolManager::GetFreeSize() { return replacer_->Size() + free_list_.size(); }
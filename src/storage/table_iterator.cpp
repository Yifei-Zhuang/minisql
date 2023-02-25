#include "storage/table_iterator.h"
#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() {}

TableIterator::TableIterator(const TableIterator &other) {
  row_ = other.row_;
  owner_heap_ = other.owner_heap_;
  rid.Set(row_->GetRowId().GetPageId(), row_->GetRowId().GetSlotNum());
}

TableIterator::~TableIterator() {
  if (row_ != nullptr) delete row_;
}

// 不知道为啥在这里定义编译不了
// bool TableIterator::operator==(const TableIterator &itr) const {
//   return rid.GetPageId() == itr.rid.GetPageId() && rid.GetSlotNum() == itr.rid.GetSlotNum();
// }

// 同上
// bool TableIterator::operator!=(const TableIterator &itr) const { return !(*this == itr); }

const Row &TableIterator::operator*() {
  //  ASSERT(false, "Not implemented yet.");
  ASSERT(row_ != nullptr, "[ ERROR ] - dereference to a nullptr is not permitted");
  return *row_;
}

Row *TableIterator::operator->() {
  ASSERT(row_ != nullptr, "[ ERROR ] - dereference to a nullptr is not permitted");
  return row_;
}

TableIterator &TableIterator::operator++() {
  // 首先，要获取当前元组所在的磁盘页
  //  ASSERT(false, "not implement yet");
  ASSERT(row_ != nullptr, "[ ERROR ] - cannot do ++ operation on a null iterator");
  page_id_t page_id = rid.GetPageId();
  ASSERT(page_id != INVALID_PAGE_ID, "[ ERROR ] - cannot do ++ operation on end iterator");
  TablePage *page = reinterpret_cast<TablePage *>(owner_heap_->buffer_pool_manager_->FetchPage(page_id));
  ASSERT(page_id == page->GetPageId(), "[ ERROR ] - page_id == page->GetPageId() should be true");
  RowId nextid;
  // 搜索下一个可用的页面
  if (page->GetNextTupleRid(rid, &nextid)) {
    //找到了
    rid.Set(nextid.GetPageId(), nextid.GetSlotNum());
    row_->SetRowId(rid);
    owner_heap_->GetTuple(row_, nullptr);
    owner_heap_->buffer_pool_manager_->UnpinPage(page_id, false);
    return *this;
  }
  page_id_t next_page_id = INVALID_PAGE_ID;
  while ((next_page_id = page->GetNextPageId()) != INVALID_PAGE_ID) {
    TablePage *next_page = reinterpret_cast<TablePage *>(owner_heap_->buffer_pool_manager_->FetchPage(next_page_id));
    owner_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = next_page;
    if (page->GetFirstTupleRid(&nextid)) {
      rid = nextid;
      row_->SetRowId(nextid);
      owner_heap_->GetTuple(row_, nullptr);
      owner_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return *this;
    }
  }
  // 到这里，说明咩有元组了
  rid.Set(INVALID_PAGE_ID, 0);
  //  row_ = nullptr;
  owner_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return *this;
}

TableIterator TableIterator::operator++(int) {
  auto row_o = new Row(*(this->row_));
  auto owner_heap_o = this->owner_heap_;
  auto rid_o = this->rid;
  ++(*this);
  return TableIterator(row_o, owner_heap_o, rid_o);
}

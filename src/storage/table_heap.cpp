#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  // 按照实验指导书中的要求，这里要用first fit策略，但是其时间复杂度为n2, 在页面非常多的时候效率极差(1e6row,
  // 1min20s)，直接从最后一页进行插入可以加速
  if (total_page > 500) {
    auto last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id));
    if (last_page == nullptr) {
      buffer_pool_manager_->UnpinPage(last_page_id, false);
      return false;
    }
    if (last_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      buffer_pool_manager_->UnpinPage(last_page_id, true);
    } else {
      page_id_t next_page_id;
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      // TODO 记得unpin（提醒自己）
      // ASSERT(new_page != nullptr, "[ ERROR ] - new_page cannot be nullptr in TableHeap::InsertTuple");
      if (new_page != nullptr && next_page_id != INVALID_PAGE_ID) {
        new_page->Init(next_page_id, last_page_id, log_manager_, txn);
        last_page->SetNextPageId(next_page_id);
        new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
        buffer_pool_manager_->UnpinPage(last_page_id, false);
        buffer_pool_manager_->UnpinPage(next_page_id, true);
        last_page_id = next_page_id;
        total_page++;
      } else {
        buffer_pool_manager_->UnpinPage(next_page_id, true);
        buffer_pool_manager_->UnpinPage(last_page_id, false);
        return false;
      }
    }
    return true;
  }

  // 获取第一页
  auto first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  //  ASSERT(first_page != nullptr, "[ ERROR ] - cannot fetch first page in TableHeap::InsertTuple");
  if (first_page == nullptr) {
    // 这里不能用assert，因为buffer pool manager 要unpin释放缓冲区。。
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    return false;
  }
  // 遍历当前有效的页面，检查是否可以插入
  auto inserted_page_id = first_page_id_;
  auto inserted_page = first_page;
  uint32_t index = 1;
  while (!(inserted_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_))) {
    // 插入失败，说明空间不足，需要检查将inserted_page和inserted_page_id切换为下一个页面
    index++;
    auto next_page_id = inserted_page->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      // 下一个页面是无效页面，那么创建新的页面
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      // TODO 记得unpin（提醒自己）
      // ASSERT(new_page != nullptr, "[ ERROR ] - new_page cannot be nullptr in TableHeap::InsertTuple");
      if (new_page != nullptr && next_page_id != INVALID_PAGE_ID) {
        new_page->Init(next_page_id, inserted_page_id, log_manager_, txn);
        inserted_page->SetNextPageId(next_page_id);
        buffer_pool_manager_->UnpinPage(inserted_page_id, false);
        inserted_page = new_page;
        inserted_page_id = next_page_id;
        last_page_id = next_page_id;
        total_page = index;
      } else {
        buffer_pool_manager_->UnpinPage(inserted_page_id, false);
        return false;
      }
    } else {
      // 果然忘记unpin了，淦
      buffer_pool_manager_->UnpinPage(inserted_page_id, false);
      // 下一个页面是一个有效页面，那么直接切换inserted_page和inserted_page_id即可
      inserted_page_id = next_page_id;
      inserted_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    }
  }
  return buffer_pool_manager_->UnpinPage(inserted_page_id, true);
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  //   TODO 添加测试（样例竟然没有update的测试）
  // 获取对应的磁盘页
  auto page_id = rid.GetPageId();
  //  auto slot_num = rid.GetSlotNum();
  auto updated_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (updated_page == nullptr) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
  Row old_row(rid);
  auto update_result = updated_page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  buffer_pool_manager_->UnpinPage(page_id, true);
  return update_result;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  auto page_id = rid.GetPageId();
  //  auto slot_num = rid.GetSlotNum();
  auto page_to_delete = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (page_to_delete == nullptr) {
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    return;
  }
  // Step2: Delete the tuple from the page.
  page_to_delete->ApplyDelete(rid, txn, log_manager_);
  buffer_pool_manager_->UnpinPage(page_id, true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

// TODO
void TableHeap::FreeHeap() {}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page_id = row->GetRowId().GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
  page->GetTuple(row, schema_, txn, lock_manager_);
  return buffer_pool_manager_->UnpinPage(page_id, false);
}

TableIterator TableHeap::Begin(Transaction *txn) {
  // 获取第一个tuple的位置
  // TODO 能否在私有属性中做记忆化存储？
  // 搜索第一个具有有效row的page
  page_id_t cur_page_id = first_page_id_;
  RowId temp;
  while (cur_page_id != INVALID_PAGE_ID) {
    auto cur_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page_id));
    if (cur_page->GetFirstTupleRid(&temp)) {
      buffer_pool_manager_->UnpinPage(cur_page_id, false);
      break;
    }
    buffer_pool_manager_->UnpinPage(cur_page_id, false);
    cur_page_id = cur_page->GetNextPageId();
  }
  if (cur_page_id != INVALID_PAGE_ID) {
    auto row = new Row(temp);
    GetTuple(row, txn);
    return TableIterator(row, this, temp);
  }
  return End();
}

TableIterator TableHeap::End() {
  RowId temp;
  return TableIterator(nullptr, this, temp);
}

#include "index/index_iterator.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *Leafpage, int index,
                                                           BufferPoolManager *buffer_pool_manager_)
    : c_page(Leafpage), c_index(index), c_buffer_pool_manager_(buffer_pool_manager_) {}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  if (c_page != nullptr) {
    Page *page = c_buffer_pool_manager_->FetchPage(c_page->GetPageId());
    page->RUnlatch();
    c_buffer_pool_manager_->UnpinPage(c_page->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  // ASSERT(false, "Not implemented yet.");
  return c_page->GetItem(c_index);
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  // ASSERT(false, "Not implemented yet.");
  c_index++;
  if (c_index == this->c_page->GetSize() && this->c_page->GetNextPageId() != -1) {
    page_id_t next = c_page->GetNextPageId();
    Page *Next_page = this->c_buffer_pool_manager_->FetchPage(next);
    B_PLUS_TREE_LEAF_PAGE_TYPE *next_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(Next_page);
    c_page = next_node;
    this->c_buffer_pool_manager_->UnpinPage(Next_page->GetPageId(), false);
    c_index = 0;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return this->c_page == itr.c_page && this->c_index == itr.c_index;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const { return !(itr == *this); }

template class IndexIterator<int, int, BasicComparator<int>>;

template class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;

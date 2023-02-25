#include "page/b_plus_tree_internal_page.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // assert(index < GetSize() && index >= 0);
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int size = GetSize();
  for (int i = 0; i < size; i++) {
    if (array_[i].second == value) return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // replace with your own code
  // int size = GetSize();
  // assert(index >= 0 && index < size);
  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // replace with your own code
  int size = GetSize();
  ValueType result;
  int i;
  for (i = 1; i < size; i++) {
    if (comparator(key, array_[i].first) < 0) {
      result = array_[i - 1].second;
      break;
    }
  }
  if (i == size) result = array_[i - 1].second;
  return result;

  // int left = 1;
  // int right = GetSize() - 1;
  // while (left <= right) {
  //   int mid = left + (right - left) / 2;
  //   if (comparator(KeyAt(mid), key) > 0) {  // 下标还需要减小
  //     right = mid - 1;
  //   } else {  // 下标还需要增大
  //     left = mid + 1;
  //   }
  // }  // upper_bound
  // int target_index = left;
  // assert(target_index - 1 >= 0);
  // // 注意，返回的value下标要减1，这样才能满足key(i-1) <= subtree(value(i)) < key(i)
  // return ValueAt(target_index - 1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int size = GetSize();
  int old_location = ValueIndex(old_value);
  for (int i = size - 1; i > old_location; i--) {
    array_[i + 1].first = array_[i].first;
    array_[i + 1].second = array_[i].second;
  }
  array_[old_location + 1].first = new_key;
  array_[old_location + 1].second = new_value;
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // assert(recipient != NULL);
  int size = GetSize();
  // assert(size == GetMaxSize() + 1);
  int start = GetMaxSize() / 2;
  int length = size - start;
  recipient->CopyNFrom(array_ + GetMaxSize() / 2, length, buffer_pool_manager);
  SetSize(GetMinSize());
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  std::copy(items, items + size, array_ + GetSize());
  for (int i = GetSize(); i < GetSize() + size; i++) {
    Page *child_page = buffer_pool_manager->FetchPage(ValueAt(i));
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int size = GetSize();
  if (index >= 0 && index < size) {
    for (int i = index + 1; i < size; i++) array_[i - 1] = array_[i];
    IncreaseSize(-1);
  }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // replace with your own code
  SetSize(0);
  return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  // assert(GetSize() + recipient->GetSize() <= GetMaxSize());
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(array_, size, buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(array_[0], buffer_pool_manager);
  Remove(0);
  Page *parent = buffer_pool_manager->FetchPage(GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *p = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(parent);
  int x = p->ValueIndex(GetPageId());
  p->SetKeyAt(x, KeyAt(0));
  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  array_[size] = pair;
  Page *page = buffer_pool_manager->FetchPage(ValueAt(size));
  BPlusTreePage *datapage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  datapage->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(array_[size - 1], buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  for (int i = size; i > 0; i--) {
    array_[i] = array_[i - 1];
  }
  array_[0] = pair;
  Page *page = buffer_pool_manager->FetchPage(ValueAt(0));
  BPlusTreePage *datapage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  datapage->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  Page *parent = buffer_pool_manager->FetchPage(GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *p = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(parent);
  int x = p->ValueIndex(GetPageId());
  p->SetKeyAt(x, KeyAt(0));
  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
  IncreaseSize(1);
}

template class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
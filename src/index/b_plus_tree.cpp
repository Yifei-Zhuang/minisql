#include "index/b_plus_tree.h"
#include <string>
#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  Page *index_root_page_raw = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  auto index_root_page = reinterpret_cast<IndexRootsPage *>(index_root_page_raw->GetData());
  root_page_id_ = INVALID_PAGE_ID;
  index_root_page->GetRootId(index_id, &root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  if (IsEmpty()) return;
  Page *page = FindLeafPage(KeyType{}, true);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  while (leaf->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next_id = leaf->GetNextPageId();
    Page *next = buffer_pool_manager_->FetchPage(next_id);
    Remove(leaf->KeyAt(0));
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    leaf = reinterpret_cast<LeafPage *>(next);
  }
  Remove(leaf->KeyAt(0));
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  if (root_page_id_ == INVALID_PAGE_ID)
    return true;
  else
    return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  if (IsEmpty()) return false;
  Page *page = FindLeafPage(key, false);
  LeafPage *mypage = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType v;
  if (mypage == nullptr) return false;
  bool myresult = mypage->Lookup(key, v, comparator_);
  if (myresult) {
    result.push_back(v);
    buffer_pool_manager_->UnpinPage(mypage->GetPageId(), false);
  }
  return myresult;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique k        ey, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  bool myresult = InsertIntoLeaf(key, value, transaction);
  return myresult;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_id;
  Page *page = buffer_pool_manager_->NewPage(new_id);
  if (page == nullptr) {
    throw std::string("out of memory");
  }
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  leaf->Init(new_id, INVALID_PAGE_ID, leaf_max_size_);
  root_page_id_ = new_id;
  UpdateRootPageId(1);
  leaf->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  UpdateRootPageId(1);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *page = FindLeafPage(key, false);
  ValueType v;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  bool myresult = leafPage->Lookup(key, v, comparator_);
  if (myresult == true) {
    buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
    return false;
  } else {
    leafPage->Insert(key, value, comparator_);
    if (leafPage->GetSize() > leafPage->GetMaxSize()) {
      B_PLUS_TREE_LEAF_PAGE_TYPE *newleafpage = Split(leafPage);
      buffer_pool_manager_->UnpinPage(newleafpage->GetPageId(), true);
      InsertIntoParent(leafPage, newleafpage->KeyAt(0), newleafpage, transaction);
    }
    buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
    return true;
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t newpage;
  Page *page = buffer_pool_manager_->NewPage(newpage);
  if (page == nullptr) {
    throw std::string("out of memory");
  }
  N *newnode;
  if (node->IsLeafPage()) {
    LeafPage *Leafnode = reinterpret_cast<LeafPage *>(node);
    LeafPage *Leafnode_page = reinterpret_cast<LeafPage *>(page);
    Leafnode_page->Init(newpage, Leafnode->GetParentPageId(), Leafnode->GetMaxSize());
    Leafnode->MoveHalfTo(Leafnode_page);
    Leafnode_page->SetNextPageId(Leafnode->GetNextPageId());
    Leafnode->SetNextPageId(Leafnode_page->GetPageId());
    newnode = reinterpret_cast<N *>(Leafnode_page);
  } else {
    InternalPage *internalnode = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_internalnode = reinterpret_cast<InternalPage *>(page);
    new_internalnode->Init(newpage, internalnode->GetParentPageId(), internalnode->GetMaxSize());
    internalnode->MoveHalfTo(new_internalnode, buffer_pool_manager_);
    newnode = reinterpret_cast<N *>(new_internalnode);
  }
  return newnode;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    Page *page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr) {
      throw std::string("out of memory");
    }
    InternalPage *newrootpage = reinterpret_cast<InternalPage *>(page->GetData());
    newrootpage->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    newrootpage->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {
    page_id_t old = old_node->GetParentPageId();
    Page *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    InternalPage *newpre_page = reinterpret_cast<InternalPage *>(page->GetData());
    new_node->SetParentPageId(old_node->GetParentPageId());
    newpre_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    if (newpre_page->GetSize() > newpre_page->GetMaxSize()) {
      InternalPage *t_newpre_page = Split(newpre_page);
      buffer_pool_manager_->UnpinPage(t_newpre_page->GetPageId(), true);
      InsertIntoParent(newpre_page, t_newpre_page->KeyAt(0), t_newpre_page, transaction);
    }
    buffer_pool_manager_->UnpinPage(old, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) return;
  Page *page = FindLeafPage(key, false);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  leaf->RemoveAndDeleteRecord(key, comparator_);
  CoalesceOrRedistribute(leaf, transaction);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  bool Deletion = false;
  if (node->IsRootPage()) {
    Deletion = AdjustRoot(node);
  } else {
    Page *parent = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    InternalPage *p = reinterpret_cast<InternalPage *>(parent);
    int x = p->ValueIndex(node->GetPageId());
    p->SetKeyAt(x, node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);

    //不管咋样先确定一下父亲节点的key
    if (node->GetSize() < node->GetMinSize())  //需要判断
    {
      page_id_t parent_id = node->GetParentPageId();
      Page *parent = buffer_pool_manager_->FetchPage(parent_id);
      InternalPage *parent_page = reinterpret_cast<InternalPage *>(parent);
      int index = parent_page->ValueIndex(node->GetPageId());
      if (index == 0)  //第一个节点
      {
        page_id_t neighbor_id = parent_page->ValueAt(1);
        Page *neighbor = buffer_pool_manager_->FetchPage(neighbor_id);
        N *neighbor_page = reinterpret_cast<N *>(neighbor);
        if (node->GetSize() + neighbor_page->GetSize() > node->GetMaxSize())
          Redistribute(neighbor_page, node, 0);
        else {
          Deletion = Coalesce(&neighbor_page, &node, &parent_page, index, transaction);
          buffer_pool_manager_->DeletePage(neighbor->GetPageId());
        }

        buffer_pool_manager_->UnpinPage(neighbor->GetPageId(), true);
      } else if (index == parent_page->GetSize() - 1)                  //最后一个节点
      {
        page_id_t neighbor_id = parent_page->ValueAt(index - 1);
        Page *neighbor = buffer_pool_manager_->FetchPage(neighbor_id);
        N *neighbor_page = reinterpret_cast<N *>(neighbor);
        if (node->GetSize() + neighbor_page->GetSize() > node->GetMaxSize())
          Redistribute(neighbor_page, node, 1);
        else {
          Deletion = Coalesce(&neighbor_page, &node, &parent_page, index, transaction);
          buffer_pool_manager_->DeletePage(node->GetPageId());
        }
        buffer_pool_manager_->UnpinPage(neighbor->GetPageId(), true);

      } else  //中间节点
      {
        page_id_t neighbor_id1 = parent_page->ValueAt(index - 1);
        Page *neighbor1 = buffer_pool_manager_->FetchPage(neighbor_id1);
        N *neighbor_page1 = reinterpret_cast<N *>(neighbor1);
        page_id_t neighbor_id2 = parent_page->ValueAt(index + 1);
        Page *neighbor2 = buffer_pool_manager_->FetchPage(neighbor_id2);
        N *neighbor_page2 = reinterpret_cast<N *>(neighbor2);
        if (node->GetSize() + neighbor_page1->GetSize() > node->GetMaxSize()) {
          Redistribute(neighbor_page1, node, 1);
        } else if (node->GetSize() + neighbor_page2->GetSize() > node->GetMaxSize()) {
          Redistribute(neighbor_page2, node, 0);

        } else {
          Deletion = Coalesce(&neighbor_page1, &node, &parent_page, index, transaction);
          buffer_pool_manager_->DeletePage(node->GetPageId());
        }
        buffer_pool_manager_->UnpinPage(neighbor1->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(neighbor2->GetPageId(), true);
      }
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }
  }

  return Deletion;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if ((*node)->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *leaf_neighbor = reinterpret_cast<LeafPage *>(*neighbor_node);
    if (index == 0 || (*parent)->ValueIndex((*node)->GetPageId()) <
                          (*parent)->ValueIndex((*neighbor_node)->GetPageId()))  //第一个点,向右
    {
      leaf_neighbor->MoveAllTo(leaf_node);
      leaf_node->SetNextPageId(leaf_neighbor->GetNextPageId());
      (*parent)->Remove(index + 1);
    } else  //向左合并
    {
      leaf_node->MoveAllTo(leaf_neighbor);
      leaf_neighbor->SetNextPageId(leaf_node->GetNextPageId());
      (*parent)->Remove(index);
    }
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *internal_neighbor = reinterpret_cast<InternalPage *>(*neighbor_node);
    if (index == 0 || (*parent)->ValueIndex((*node)->GetPageId()) <
                          (*parent)->ValueIndex((*neighbor_node)->GetPageId()))  //第一个点,向右
    {
      internal_neighbor->MoveAllTo(internal_node, (*parent)->KeyAt(index + 1), buffer_pool_manager_);
      (*parent)->Remove(index + 1);
    } else {
      internal_node->MoveAllTo(internal_neighbor, (*parent)->KeyAt(index), buffer_pool_manager_);
      (*parent)->Remove(index);
    }
  }


  bool deletions = CoalesceOrRedistribute(*parent, transaction);
  return deletions;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (index == 0) {
    if (neighbor_node->IsLeafPage()) {
      LeafPage *leafnode = reinterpret_cast<LeafPage *>(node);
      LeafPage *leafnei = reinterpret_cast<LeafPage *>(neighbor_node);
      leafnei->MoveFirstToEndOf(leafnode, buffer_pool_manager_);
    } else {
      InternalPage *leafnode = reinterpret_cast<InternalPage *>(node);
      InternalPage *leafnei = reinterpret_cast<InternalPage *>(neighbor_node);
      leafnei->MoveFirstToEndOf(leafnode, leafnei->KeyAt(0), buffer_pool_manager_);
    }
  } else if (index == 1) {
    if (neighbor_node->IsLeafPage()) {
      LeafPage *leafnode = reinterpret_cast<LeafPage *>(node);
      LeafPage *leafnei = reinterpret_cast<LeafPage *>(neighbor_node);
      leafnei->MoveLastToFrontOf(leafnode, buffer_pool_manager_);
    } else {
      InternalPage *leafnode = reinterpret_cast<InternalPage *>(node);
      InternalPage *leafnei = reinterpret_cast<InternalPage *>(neighbor_node);
      leafnei->MoveLastToFrontOf(leafnode, leafnode->KeyAt(0), buffer_pool_manager_);
    }
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  } else if (old_root_node->GetSize() == 1) {
    InternalPage *root_id = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t newroot_id;
    newroot_id = root_id->RemoveAndReturnOnlyChild();
    root_page_id_ = newroot_id;
    UpdateRootPageId(0);
    Page *page = buffer_pool_manager_->FetchPage(newroot_id);
    BPlusTreePage *newroot = reinterpret_cast<BPlusTreePage *>(page->GetData());
    newroot->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(newroot_id, true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {  // KeyType P;
  Page *page = FindLeafPage(KeyType{}, true);
  LeafPage *page_leaf = reinterpret_cast<LeafPage *>(page->GetData());
  buffer_pool_manager_->UnpinPage(page_leaf->GetPageId(), false);
  return INDEXITERATOR_TYPE(page_leaf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *page = FindLeafPage(KeyType{}, false);
  LeafPage *page_leaf = reinterpret_cast<LeafPage *>(page->GetData());
  int index = page_leaf->KeyIndex(key, comparator_);
  buffer_pool_manager_->UnpinPage(page_leaf->GetPageId(), false);
  return INDEXITERATOR_TYPE(page_leaf, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  Page *page = FindLeafPage(KeyType{}, true);
  LeafPage *page_leaf = reinterpret_cast<LeafPage *>(page->GetData());
  while (page_leaf->GetNextPageId() != -1) {
    Page *Next = buffer_pool_manager_->FetchPage(page_leaf->GetNextPageId());
    LeafPage *Next_leaf = reinterpret_cast<LeafPage *>(Next->GetData());
    buffer_pool_manager_->UnpinPage(page_leaf->GetPageId(), false);
    page = Next;
    page_leaf = Next_leaf;
  }

  return INDEXITERATOR_TYPE(page_leaf, page_leaf->GetSize() - 1, buffer_pool_manager_);  //?
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);  // now root page is pin
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page);
  while (!node->IsLeafPage()) {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    page_id_t next_page_id = leftMost ? internal_node->ValueAt(0) : internal_node->Lookup(key, comparator_);
    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);  // next_level_page pinned
    BPlusTreePage *next_node = reinterpret_cast<BPlusTreePage *>(next_page);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);  // curr_node unpinned
    page = next_page;
    node = next_node;
  }
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  Page *root_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage *root = reinterpret_cast<IndexRootsPage *>(root_page);
  if (insert_record)
    root->Insert(index_id_, root_page_id_);
  else
    root->Update(index_id_, root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template class BPlusTree<int, int, BasicComparator<int>>;

template class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;

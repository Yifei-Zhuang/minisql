#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  //  static constexpr uint32_t CATALOG_METADATA_MAGIC_NUM = 89849;
  //  std::map<table_id_t, page_id_t> table_meta_pages_;
  //  std::map<index_id_t, page_id_t> index_meta_pages_;
  // ASSERT(false, "Not Implemented yet");
  // 写入magic num
  int move = 0;
  memcpy(buf + move, &CATALOG_METADATA_MAGIC_NUM, sizeof(CATALOG_METADATA_MAGIC_NUM));
  move += sizeof(CATALOG_METADATA_MAGIC_NUM);

  // 写入table_meta_pages
  // 写入表的个数
  size_t size = table_meta_pages_.size();
  memcpy(buf + move, &size, sizeof((size)));
  move += sizeof(size);
  // 逐个写入记录
  for_each(table_meta_pages_.begin(), table_meta_pages_.end(), [&](auto &it) -> void {
    memcpy(buf + move, &it.first, sizeof(table_id_t));
    move += sizeof(table_id_t);
    memcpy(buf + move, &it.second, sizeof(page_id_t));
    move += sizeof(page_id_t);
  });

  // index_meta_pages_
  // 写入表的个数
  size_t size2 = index_meta_pages_.size();
  memcpy(buf + move, &size2, sizeof((size2)));
  move += sizeof(size2);
  // 逐个写入记录
  for_each(index_meta_pages_.begin(), index_meta_pages_.end(), [&](auto &it) -> void {
    memcpy(buf + move, &it.first, sizeof(index_id_t));
    move += sizeof(table_id_t);
    memcpy(buf + move, &it.second, sizeof(page_id_t));
    move += sizeof(page_id_t);
  });
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // ASSERT(false, "Not Implemented yet");
  // 检测magic num
  uint32_t magic_num = 0;
  uint32_t move = 0;
  memcpy(&magic_num, buf + move, sizeof(magic_num));
  move += sizeof(magic_num);
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "CatalogMeta::DeserializeFrom: magic num assert fail");

  //  std::map<table_id_t, page_id_t> table_meta_pages_;
  //  std::map<index_id_t, page_id_t> index_meta_pages_;
  void *mem = heap->Allocate(sizeof(CatalogMeta));
  CatalogMeta *result = new (mem) CatalogMeta();

  size_t table_meta_pages_size, index_meta_pages_size;
  // 获取table_meta_pages_大小
  memcpy(&table_meta_pages_size, buf + move, sizeof(table_meta_pages_size));
  move += sizeof(table_meta_pages_size);
  // 逐个插入
  table_id_t tableid;
  page_id_t pageid;
  for (uint32_t i = 0; i < table_meta_pages_size; i++) {
    memcpy(&tableid, buf + move, sizeof(tableid));
    move += sizeof(tableid);
    memcpy(&pageid, buf + move, sizeof(pageid));
    move += sizeof(pageid);
    result->table_meta_pages_.emplace(tableid, pageid);
  }

  // index_meta_pages_
  memcpy(&index_meta_pages_size, buf + move, sizeof(index_meta_pages_size));
  move += sizeof(index_meta_pages_size);
  // 逐个插入
  index_id_t indexid;
  for (uint32_t i = 0; i < index_meta_pages_size; i++) {
    memcpy(&indexid, buf + move, sizeof(indexid));
    move += sizeof(indexid);
    memcpy(&pageid, buf + move, sizeof(pageid));
    move += sizeof(pageid);
    result->index_meta_pages_.emplace(indexid, pageid);
  }
  return result;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  return sizeof(CATALOG_METADATA_MAGIC_NUM) + sizeof(decltype(index_meta_pages_.size())) * 2 +
         index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t)) +
         table_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
}

CatalogMeta::CatalogMeta() {}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager),
      lock_manager_(lock_manager),
      log_manager_(log_manager),
      heap_(new SimpleMemHeap()) {
  if (!init) {
    // 需要重新加载数据
    // 反序列化，获取catalog_meta
    auto catalog_page_raw = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(reinterpret_cast<char *>(catalog_page_raw->GetData()), heap_);

    next_index_id_ = catalog_meta_->GetNextIndexId();
    next_table_id_ = catalog_meta_->GetNextTableId();
    // load tables and indexs
    for_each(catalog_meta_->table_meta_pages_.begin(), catalog_meta_->table_meta_pages_.end(),
             [&](auto &it) -> void { LoadTable(it.first, it.second); });
    for_each(catalog_meta_->index_meta_pages_.begin(), catalog_meta_->index_meta_pages_.end(),
             [&](auto &it) -> void { LoadIndex(it.first, it.second); });
  } else {
    // 全新的数据库
    next_table_id_ = next_index_id_ = 0;
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if (table_names_.count(table_name) != 0) {
    return DB_TABLE_ALREADY_EXIST;
  }
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, heap_);
  // 如果schema中主键为空，那么遍历schema，寻找一个unique的属性
  if (schema->getPrimaryKeys().size() == 0) {
    int index = -1;
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      if (schema->GetColumn(i)->IsUnique()) {
        index = i;
        break;
      }
    }
    if (index == -1) {
      // 无unique 所有属性合起来为主键
      for (int i = 0; i < index; i++) {
        schema->getPrimaryKeys().push_back(i);
      }
    } else {
      // 有主键
      schema->getPrimaryKeys().push_back(index);
    }
  }
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (schema->GetColumn(i)->IsUnique()) {
      vector<uint32_t> &vvv = schema->getUniqueKeys();
      vvv.push_back(i);
    }
  }
  auto table_meta = TableMetadata::Create(next_table_id_, table_name, table_heap->GetFirstPageId(), schema, heap_);
  table_info = TableInfo::Create(heap_);
  table_info->Init(table_meta, table_heap);
  table_names_.emplace(table_name, next_table_id_);
  tables_.emplace(next_table_id_, table_info);
  next_table_id_++;

  // 将新的表写入matapage中
  auto catalog_meta_page_raw = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  // 获取新的meta页
  page_id_t new_table_meta_page_id = INVALID_PAGE_ID;
  auto new_table_meta_page = buffer_pool_manager_->NewPage(new_table_meta_page_id);
  assert(new_table_meta_page != nullptr);
  table_meta->SerializeTo(reinterpret_cast<char *>(new_table_meta_page));
  catalog_meta_->table_meta_pages_.emplace(table_info->GetTableId(), new_table_meta_page_id);
  // 为所有的unique列单独建立索引
  auto unique_keys = schema->getUniqueKeys();
  for (uint32_t i = 0; i < unique_keys.size(); i++) {
    IndexInfo *unused;
    vector<string> temp_vec = {schema->GetColumn(unique_keys[i])->GetName()};
    CreateIndex(table_name, table_name + "__unique__" + to_string(unique_keys[i]), temp_vec, nullptr, unused);
  }

  // 为主键建立索引
  IndexInfo *unused;
  auto primary_keys = schema->getPrimaryKeys();
  vector<string> temp_vec;
  temp_vec.reserve(primary_keys.size());
  for (uint32_t i = 0; i < primary_keys.size(); i++) {
    temp_vec.push_back(schema->GetColumn(primary_keys[i])->GetName());
  }
  CreateIndex(table_name, table_name + "__primary", temp_vec, nullptr, unused);

  buffer_pool_manager_->FlushPage(new_table_meta_page_id);
  buffer_pool_manager_->UnpinPage(new_table_meta_page_id, true);
  buffer_pool_manager_->UnpinPage(catalog_meta_page_raw->GetPageId(), true);
  return FlushCatalogMetaPage();
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto temp = table_names_.find(table_name);
  if (temp == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_.find(temp->second)->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  for (auto &it : tables_) {
    tables.push_back(it.second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // ASSERT(false, "Not Implemented yet");
  auto temp = table_names_.find(table_name);
  if (temp == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto judge = index_names_.find(table_name);
  if (judge != index_names_.end() && judge->second.find(index_name) != judge->second.end()) {
    return DB_INDEX_ALREADY_EXIST;
  }

  auto table_info = tables_.find(temp->second);
  auto *schema = table_info->second->GetSchema();
  bool tag = false;
  for (auto &it : index_keys) {
    uint32_t index__ = 0;
    auto result = schema->GetColumnIndex(it, index__);
    if (result != DB_SUCCESS) {
      return result;
    }
    if (schema->GetColumn(index__)->IsUnique()) {
      tag = true;
      break;
    }
  }
  if (!tag) {
    //如果没有唯一属性，那么必须在主键上建立索引
    // __primary
    if (index_name.length() < 9) {
      std::cout << "不能在非唯一属性上建立索引" << std::endl;
      return DB_COLUMN_NOT_UNIQUE;
    }
    string check_str = index_name.substr(index_name.length() - 9, index_name.length());
    if (check_str != "__primary") {
      std::cout << "不能在非唯一属性上建立索引" << std::endl;
      return DB_COLUMN_NOT_UNIQUE;
    }
  }
  std::vector<uint32_t> keys(index_keys.size());
  for (size_t i = 0; i < index_keys.size(); i++) {
    if (schema->GetColumnIndex(index_keys[i], keys[i]) == DB_COLUMN_NAME_NOT_EXIST) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
  }
  //遍历已有的所有index，查看是否有完全相同的索引
  for (auto &it : indexes_) {
    auto index_schema = it.second->GetIndexKeySchema();
    if (index_schema->GetColumnCount() == keys.size()) {
      bool flag = true;
      auto cols = index_schema->GetColumns();
      for (uint32_t i = 0; flag && i < keys.size(); i++) {
        if (keys[i] != cols[i]->GetTableInd()) {
          flag = false;
        }
      }
      if (flag) {
        //完全相同
        return DB_INDEX_ALREADY_EXIST;
      }
    }
  }
  auto index_meta = IndexMetadata::Create(next_index_id_, index_name, temp->second, keys, heap_);

  index_info = IndexInfo::Create(heap_);
  index_info->Init(index_meta, table_info->second, buffer_pool_manager_);

  indexes_.emplace(next_index_id_, index_info);
  auto find_table_all_index_map = index_names_.find(table_name);
  if (find_table_all_index_map == index_names_.end()) {
    // 创建新的map
    std::unordered_map<std::string, index_id_t> newMap;
    newMap.emplace(index_name, next_index_id_++);
    index_names_.emplace(table_name, newMap);
  } else {
    (index_names_.find(table_name)->second).emplace(index_name, next_index_id_++);
  }
  //插入表中的数据
  auto table_heap = table_info->second->GetTableHeap();
  vector<Field> f;
  for (auto it = table_heap->Begin(nullptr); it != table_heap->End(); it++) {
    f.clear();
    for (auto pos : keys) {
      f.push_back(*(it->GetField(pos)));
    }
    Row row(f);
    index_info->GetIndex()->InsertEntry(row, it->GetRowId(), nullptr);
  }
  next_index_id_++;
  //将新的index写入磁盘中
  page_id_t new_index_page_id = INVALID_PAGE_ID;
  auto new_index_page = buffer_pool_manager_->NewPage(new_index_page_id);
  assert(new_index_page != nullptr);
  index_meta->SerializeTo(reinterpret_cast<char *>(new_index_page));

  catalog_meta_->index_meta_pages_.emplace(index_meta->GetIndexId(), new_index_page_id);

  buffer_pool_manager_->UnpinPage(new_index_page_id, true);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  return FlushCatalogMetaPage();
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  auto index_vec = index_names_.find(table_name);
  if (index_vec == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto index = index_vec->second.find(index_name);
  if (index == index_vec->second.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_info = indexes_.find(index->second)->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  auto index_vec = index_names_.find(table_name);
  if (index_vec == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  indexes.clear();
  for (auto &it : index_vec->second) {
    indexes.push_back(indexes_.find(it.second)->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  auto temp = table_names_.find(table_name);
  if (temp == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  // 删除对应的记录
  auto table_info = tables_.find(temp->second);
  assert(table_info != tables_.end());
  // 改变metapage
  catalog_meta_->table_meta_pages_.erase(temp->second);
  // 修改内存中的map
  heap_->Free(tables_[temp->second]);
  tables_.erase(temp->second);
  table_names_.erase(table_name);
  return FlushCatalogMetaPage();
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto temp = index_names_.find(table_name);
  if (temp == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  if ((temp->second.find(index_name) == temp->second.end())) {
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = temp->second.find(index_name)->second;
  heap_->Free(indexes_[index_id]);
  indexes_.erase(index_id);
  (index_names_[table_name]).erase(index_name);
  catalog_meta_->index_meta_pages_.erase(index_id);
  return FlushCatalogMetaPage();
}

dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto CatalogMetaPage = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  assert(CatalogMetaPage != nullptr);
  catalog_meta_->SerializeTo(reinterpret_cast<char *>(CatalogMetaPage->GetData()));
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if (tables_.count(table_id) != 0) {
    return DB_SUCCESS;
  }
  auto catalog_meta_page_raw = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  assert(catalog_meta_page_raw != nullptr);
  auto table_page_raw = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata *table_page = nullptr;
  TableMetadata::DeserializeFrom(reinterpret_cast<char *>(table_page_raw->GetData()), table_page, heap_);
  table_names_.emplace(table_page->GetTableName(), table_id);
  TableInfo *table_info = TableInfo::Create(heap_);
  TableHeap *tableheap = TableHeap::Create(buffer_pool_manager_, table_page->GetFirstPageId(), table_page->GetSchema(),
                                           nullptr, nullptr, heap_);
  table_info->Init(table_page, tableheap);
  tables_.emplace(table_id, table_info);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if (indexes_.count(index_id) != 0) {
    return DB_SUCCESS;
  }
  auto catalog_meta_page_raw = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  assert(catalog_meta_page_raw != nullptr);
  auto index_page_raw = buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *index_page = nullptr;
  IndexMetadata::DeserializeFrom(reinterpret_cast<char *>(index_page_raw->GetData()), index_page, heap_);
  assert(index_page != nullptr);
  auto index_info = IndexInfo::Create(heap_);
  TableInfo *tableinfo = nullptr;
  GetTable(index_page->GetTableId(), tableinfo);
  assert(tableinfo != nullptr);
  index_info->Init(index_page, tableinfo, buffer_pool_manager_);
  indexes_.emplace(index_id, index_info);
  auto temp = index_names_.find(index_info->GetTableInfo()->GetTableName());
  if (temp == index_names_.end()) {
    std::unordered_map<std::string, index_id_t> newMap;
    newMap.emplace(index_info->GetIndexName(), index_id);
    index_names_.emplace(index_info->GetTableInfo()->GetTableName(), newMap);
  } else {
    temp->second.emplace(index_info->GetIndexName(), index_id);
  }
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto temp = tables_.find(table_id);
  if (temp == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = temp->second;
  return DB_SUCCESS;
}
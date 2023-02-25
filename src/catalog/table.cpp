#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  //  static constexpr uint32_t TABLE_METADATA_MAGIC_NUM = 344528;
  //  table_id_t table_id_;
  //  std::string table_name_;
  //  page_id_t root_page_id_;
  //  Schema *schema_;
  uint32_t move = 0;
  memcpy(buf + move, &TABLE_METADATA_MAGIC_NUM, sizeof(TABLE_METADATA_MAGIC_NUM));
  move += sizeof(TABLE_METADATA_MAGIC_NUM);

  memcpy(buf + move, &table_id_, sizeof(table_id_));
  move += sizeof(table_id_);

  size_t size = table_name_.size();
  memcpy(buf + move, &size, sizeof(size));
  move += sizeof(size);

  memcpy(buf + move, table_name_.c_str(), size);
  move += size;

  memcpy(buf + move, &root_page_id_, sizeof(root_page_id_));
  move += sizeof(root_page_id_);

  ASSERT(schema_ != nullptr, "schema to be serialize cannot be nullptr");
  move += schema_->SerializeTo(buf + move);
  return move;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return sizeof(uint32_t) + sizeof(table_id_) + sizeof(size_t) + table_name_.size() + schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  //  table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
  assert(table_meta == nullptr);
  uint32_t move = 0;
  uint32_t magic_num = 0;
  memcpy(&magic_num, buf + move, sizeof(uint32_t));
  ASSERT(magic_num == TABLE_METADATA_MAGIC_NUM, "MAGIC NUM ERROR in TableMetadata::DeserializeFrom!");
  move += sizeof(uint32_t);

  table_id_t tid;
  page_id_t root_page_id;

  memcpy(&tid, buf + move, sizeof(tid));
  move += sizeof(tid);

  // get string size
  size_t size = 0;
  memcpy(&size, buf + move, sizeof(size_t));
  move += sizeof(size_t);

  char *temp = new char[size + 1];
  temp[size] = '\0';
  memcpy(temp, buf + move, size);
  move += size;
  string temp_str(temp, size);
  delete[] temp;

  // get root_page_id
  memcpy(&root_page_id, buf + move, sizeof(root_page_id));
  move += sizeof(root_page_id);

  // get schema
  // use heap to allocate memory
  Schema *schema = nullptr;
  move += Schema::DeserializeFrom(buf + move, schema, heap);
  table_meta = Create(tid, temp_str, root_page_id, schema, heap);
  return move;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name, page_id_t root_page_id,
                                     TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new (buf) TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
    : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}

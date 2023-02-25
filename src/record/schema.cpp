#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  // 将所拥有的column的个数写入缓冲区中
  uint32_t column_count = GetColumnCount();
  memcpy(buf, &column_count, sizeof(uint32_t));
  uint32_t move = sizeof(uint32_t);

  for_each(columns_.begin(), columns_.end(), [&](Column *it) -> void {
    uint32_t temp = it->SerializeTo(buf + move);
    move += temp;
  });

  // 将所拥有的主键的个数写入缓冲区中
  size_t pkCount = primary_keys.size();
  memcpy(buf + move, &pkCount, sizeof(size_t));
  move += sizeof(size_t);

  for (uint32_t i = 0; i < pkCount; i++) {
    memcpy(buf + move, &primary_keys[i], sizeof(uint32_t));
    move += sizeof(uint32_t);
  }
  return move;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  // 最初写入的列的个数
  uint32_t result = sizeof(uint32_t) + sizeof(size_t);
  // 每个列的大小
  std::for_each(columns_.begin(), columns_.end(), [&](Column *it) -> void { result += it->GetSerializedSize(); });
  result += (sizeof(uint32_t)) * primary_keys.size();
  return result;
}

/*
 * @param: schema: 接受的schema是一个nullptr
 **/
uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
  if (schema != nullptr) {
    return 0;
  }
  /* deserialize field from buf */
  std::vector<Column *> temp;
  // can be replaced by:
  // ALLOC_P(heap, Column)(column_name, type, col_ind, nullable, unique);
  void *mem = heap->Allocate(sizeof(Schema));
  schema = new (mem) Schema(temp);

  // 读取列的个数
  uint32_t column_count = 0;
  memcpy(&column_count, buf, sizeof(uint32_t));

  uint32_t move = sizeof(uint32_t);
  schema->columns_.resize(column_count);
  fill_n(schema->columns_.begin(), column_count, nullptr);
  //读取每个列
  for (uint32_t i = 0; i < column_count; i++) {
    auto temp_result = Column::DeserializeFrom(buf + move, schema->columns_[i], heap);
    move += temp_result;
  }

  size_t pkCount;
  memcpy(&pkCount, buf + move, sizeof(size_t));
  move += sizeof(size_t);

  schema->primary_keys.resize(pkCount);
  for (uint32_t i = 0; i < pkCount; i++) {
    memcpy(&schema->primary_keys[i], buf + move, sizeof(uint32_t));
    move += sizeof(uint32_t);
  }

  return move;
}
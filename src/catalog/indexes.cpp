#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name, const table_id_t table_id,
                                     const vector<uint32_t> &key_map, MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new (buf) IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  //  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  //  index_id_t index_id_;
  //  std::string index_name_;
  //  table_id_t table_id_;
  //  std::vector<uint32_t> key_map_;  /** The mapping of index key to tuple key */
  uint32_t move = 0;

  // write magic num
  memcpy(buf + move, &INDEX_METADATA_MAGIC_NUM, sizeof(INDEX_METADATA_MAGIC_NUM));
  move += sizeof(INDEX_METADATA_MAGIC_NUM);

  // write index_id
  memcpy(buf + move, &index_id_, sizeof(index_id_t));
  move += sizeof(index_id_t);

  // write index_name
  // first write size
  size_t size = index_name_.size();
  memcpy(buf + move, &size, sizeof(size));
  move += sizeof(size);
  // then write string.c_str()
  auto pointer = index_name_.c_str();
  memcpy(buf + move, pointer, size);
  move += size;
  // then write table_id_
  memcpy(buf + move, &table_id_, sizeof(table_id_));
  move += sizeof(table_id_);

  // then write key_map_
  // 首先写入大小（键和值的大小是否需要写入？）
  size_t size_key_map = key_map_.size();
  memcpy(buf + move, &size_key_map, sizeof(size_key_map));
  move += sizeof(size_key_map);
  // 接下来写入index_key
  memcpy(buf + move, &key_map_[0], sizeof(uint32_t) * size_key_map);

  return move;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  return sizeof(INDEX_METADATA_MAGIC_NUM) + sizeof(index_id_) + sizeof(size_t) + sizeof(index_name_.size()) +
         sizeof(table_id_) + sizeof(size_t) + sizeof(uint32_t) * key_map_.size();
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  //  const index_id_t index_id, const string &index_name, const table_id_t table_id,
  //      const vector<uint32_t> &key_map, MemHeap *heap
  uint32_t move = 0;
  uint32_t magic_num = 0;
  index_id_t index_id;
  table_id_t tid;

  memcpy(&magic_num, buf + move, sizeof(INDEX_METADATA_MAGIC_NUM));
  ASSERT(magic_num == INDEX_METADATA_MAGIC_NUM, "MAGIC NUM ERROR in IndexMetadata::DeserializeFrom!");
  move += sizeof(INDEX_METADATA_MAGIC_NUM);

  memcpy(&index_id, buf + move, sizeof(index_id_t));
  move += sizeof(index_id_t);

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
  memcpy(&tid, buf + move, sizeof(tid));
  move += sizeof(tid);

  // 获取数组长度
  size_t size_key_map = 0;
  memcpy(&size_key_map, buf + move, sizeof(size_t));
  move += sizeof(size_key_map);
  // 向数组写入数据
  vector<uint32_t> temp_vec;
  temp_vec.resize(size_key_map);
  uint32_t *arr = new uint32_t[size_key_map];
  memcpy(arr, buf + move, sizeof(uint32_t) * size_key_map);
  move += sizeof(uint32_t) * size_key_map;
  memcpy(&temp_vec[0], arr, sizeof(uint32_t) * size_key_map);
  delete[] arr;
  index_meta = Create(index_id, temp_str, tid, temp_vec, heap);
  return move;
}
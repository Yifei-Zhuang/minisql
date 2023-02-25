#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  // 写入个数
  uint32_t size = this->GetFieldCount();
  memcpy(buf, &size, sizeof(uint32_t));

  uint32_t move = sizeof(uint32_t);
  // 创建空位图
  int map_size = ceil(1.0 * size / 8);
  auto null_map = new int[map_size];
  std::fill_n(null_map, map_size, 0);
  // 扫描整个field
  int idx = 0;
  for_each(fields_.begin(), fields_.end(), [&](Field *f) -> void {
    if (!f->IsNull()) {
      null_map[idx / 8] |= (1 << idx % 8);
    }
    idx = idx + 1;
  });
  // 写入空位图
  memcpy(buf + move, null_map, sizeof(int) * map_size);
  move += (sizeof(int)) * map_size;

  //将非空的元素逐个写入
  for (uint32_t i = 0; i < fields_.size(); i++) {
    Field *f = fields_[i];
    if (!f->IsNull()) {
      move += f->SerializeTo(buf + move);
    }
  }
  return move;
}
//#include <iostream>
uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  // replace with your code here
  // 读取列个数
  uint32_t num = 0;
  memcpy(&num, buf, sizeof(uint32_t));
  // 分配field数组

  fields_.resize(num);
  fill_n(fields_.begin(), num, nullptr);

  uint32_t move = sizeof(uint32_t);
  // 读取空位图
  int map_size = ceil(1.0 * num / 8);
  auto null_map = new int[map_size];
  memcpy(null_map, buf + move, sizeof(int) * map_size);
  move += sizeof(int) * map_size;

  // 读取各个field
  for (uint32_t i = 0; i < num; i++) {
    void *mem = heap_->Allocate(sizeof(Field));
    if ((null_map[i / 8] & (1 << (i % 8))) == 0) {
      // null
      fields_[i] = new (mem) Field(schema->GetColumn(i)->GetType());
    } else {
      // 不是null，那么分配空间
      auto type = schema->GetColumn(i)->GetType();
      switch (type) {
        case kTypeInt:
          fields_[i] = new (mem) Field(type, 0);
          break;
        case kTypeFloat:
          fields_[i] = new (mem) Field(type, float(0.0));
          break;
        case kTypeChar:
          fields_[i] = new (mem) Field(type, nullptr, 0, true);
          fields_[i]->SetIsNull(false);
          break;
        case kTypeInvalid:
          break;
      }
    }
    move += Field::DeserializeFrom(buf + move, schema->GetColumn(i)->GetType(), &fields_[i],
                                   (null_map[i / 8] & (1 << i % 8)) == 0, heap_);
  }
  delete[] null_map;
  return move;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here

  // 最初的num大小
  uint32_t result = sizeof(uint32_t);

  // 空位图大小
  result += sizeof(uint32_t) * (static_cast<uint32_t>(ceil(1.0 * GetFieldCount() / 8)));

  // 最后是每一个fields大小
  for_each(fields_.begin(), fields_.end(), [&](Field *f) -> void { result += f->GetSerializedSize(); });
  return result;
}

#include "record/column.h"
Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  // 序列化结构 COLUMN_MAGIC_NUM | name_.size() | name_ | type_ | len_ | table_ind_ | nullable_ | unique_
  // replace with your code here
  auto len = name_.size();
  // TODO 使用指针池避免频繁的new操作
  auto pointer = name_.c_str();
  auto type_size = sizeof(type_);
  uint64_t move = 0;
  memcpy(buf, &COLUMN_MAGIC_NUM, sizeof(uint32_t));
  move += sizeof(uint32_t);
  memcpy(buf + move, &len, sizeof(uint32_t));
  move += sizeof(uint32_t);
  memcpy(buf + move, pointer, len);
  move += len;
  memcpy(buf + move, &type_, type_size);
  move += type_size;
  memcpy(buf + move, &len_, sizeof(uint32_t));
  move += sizeof(uint32_t);
  memcpy(buf + move, &table_ind_, sizeof(uint32_t));
  move += sizeof(uint32_t);
  memcpy(buf + move, &nullable_, sizeof(bool));
  move += sizeof(bool);
  memcpy(buf + move, &unique_, sizeof(bool));
  move += sizeof(bool);
  buf += move;
  return move;
}

uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return 2 * sizeof(bool) + 4 * sizeof(uint32_t) + name_.size() + sizeof(type_);
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  //  static constexpr uint32_t COLUMN_MAGIC_NUM = 210928;
  //  std::string name_;
  //  TypeId type_;
  //  uint32_t len_{0};  // for char type this is the maximum byte length of the string data,
  //  // otherwise is the fixed size
  //  uint32_t table_ind_{0};  // column position in table
  //  bool nullable_{false};   // whether the column can be null
  //  bool unique_{false};     // whether the column is unique
  // replace with your code here
  if (column != nullptr) {
    //    throw "Pointer to column is not null in column deserialize.";
    return 0;
  }
  uint32_t checkSum = 0, stringLen = 0, len = -1, table_ind = 0;
  bool nullable_ = false, unique_ = false;
  TypeId type_;
  int move = 0;
  memcpy(&checkSum, buf + move, sizeof(uint32_t));
  ASSERT(checkSum == COLUMN_MAGIC_NUM, "[Error] - checkSum != COLUMN_MAGIC_NUM");
  move += sizeof(uint32_t);

  memcpy(&stringLen, buf + move, sizeof(uint32_t));
  move += sizeof(uint32_t);

  // TODO 使用指针池避免频繁的new操作
  auto temp = new char[stringLen + 1];
  memcpy(temp, buf + move, stringLen);
  temp[stringLen] = '\0';
  move += stringLen;
  std::string tempString(temp, stringLen);
  delete[] temp;

  memcpy(&type_, buf + move, sizeof(TypeId));
  move += sizeof(TypeId);

  memcpy(&len, buf + move, sizeof(uint32_t));
  move += sizeof(uint32_t);

  memcpy(&table_ind, buf + move, sizeof(uint32_t));
  move += sizeof(uint32_t);

  memcpy(&nullable_, buf + move, sizeof(bool));
  move += sizeof(bool);

  memcpy(&unique_, buf + move, sizeof(bool));
  move += sizeof(bool);

  buf += move;
  /* deserialize field from buf */
  // can be replaced by:
  //		ALLOC_P(heap, Column)(column_name, type, col_ind, nullable, unique);
  void *mem = heap->Allocate(sizeof(Column));
  if (type_ != TypeId::kTypeChar) {
    //    Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    column = (new (mem) Column(std::move(tempString), type_, table_ind, nullable_, unique_));
  } else {
    //    Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool
    //    unique)
    column = (new (mem) Column(std::move(tempString), type_, len, table_ind, nullable_, unique_));
  }
  return move;
}

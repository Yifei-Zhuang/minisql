#include <cstring>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "page/table_page.h"
#include "record/field.h"
#include "record/row.h"
#include "record/schema.h"

char *chars[] = {const_cast<char *>(""), const_cast<char *>("hello"), const_cast<char *>("world!"),
                 const_cast<char *>("\0")};

Field int_fields[] = {
    Field(TypeId::kTypeInt, 188), Field(TypeId::kTypeInt, -65537), Field(TypeId::kTypeInt, 33389),
    Field(TypeId::kTypeInt, 0),   Field(TypeId::kTypeInt, 999),
};
Field float_fields[] = {
    Field(TypeId::kTypeFloat, -2.33f),
    Field(TypeId::kTypeFloat, 19.99f),
    Field(TypeId::kTypeFloat, 999999.9995f),
    Field(TypeId::kTypeFloat, -77.7f),
};
Field char_fields[] = {Field(TypeId::kTypeChar, chars[0], strlen(chars[0]), false),
                       Field(TypeId::kTypeChar, chars[1], strlen(chars[1]), false),
                       Field(TypeId::kTypeChar, chars[2], strlen(chars[2]), false),
                       Field(TypeId::kTypeChar, chars[3], 1, false)};
Field null_fields[] = {Field(TypeId::kTypeInt), Field(TypeId::kTypeFloat), Field(TypeId::kTypeChar)};

TEST(TupleTest, ColumnSerializeDeserializeTest) {
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  char *p = buffer;
  SimpleMemHeap heap;
  std::vector<Column *> columns = {ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
                                   ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
                                   ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)};
  for_each(columns.begin(), columns.end(), [&](Column *c) -> void { p += c->SerializeTo(p); });
  int ofs = 0;
  Column *c = nullptr;
  for (uint32_t i = 0; i < columns.size(); i++) {
    ofs += Column::DeserializeFrom(buffer + ofs, c, &heap);
    ASSERT_FALSE(c == nullptr);
    ASSERT_EQ(c->GetName(), columns[i]->GetName());
    ASSERT_EQ(c->GetType(), columns[i]->GetType());
    ASSERT_EQ(c->GetLength(), columns[i]->GetLength());
    ASSERT_EQ(c->GetTableInd(), columns[i]->GetTableInd());
    ASSERT_EQ(c->IsNullable(), columns[i]->IsNullable());
    ASSERT_EQ(c->GetSerializedSize(), columns[i]->GetSerializedSize());
    heap.Free(c);
    c = nullptr;
  }
}
TEST(TupleTest, SchemaSerializeDeserializeTest) {
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  SimpleMemHeap heap;
  std::vector<Column *> columns = {ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
                                   ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
                                   ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  auto size = schema->SerializeTo(buffer);
  Schema *de_serialize_result = nullptr;
  ASSERT_EQ(size, Schema::DeserializeFrom(buffer, de_serialize_result, &heap));
  ASSERT_EQ(de_serialize_result->GetSerializedSize(), size);
  ASSERT_EQ(de_serialize_result->GetColumnCount(), schema->GetColumnCount());
  auto temp1 = de_serialize_result->GetColumns();
  auto temp2 = schema->GetColumns();
  ASSERT_EQ(temp1.size(), temp2.size());
  for (uint32_t i = 0; i < temp1.size(); i++) {
    auto column1 = temp1[i];
    auto column2 = temp2[i];
    ASSERT_FALSE(column1 == nullptr);
    ASSERT_FALSE(column2 == nullptr);
    ASSERT_EQ(column1->GetName(), column2->GetName());
    ASSERT_EQ(column1->GetType(), column2->GetType());
    ASSERT_EQ(column1->GetLength(), column2->GetLength());
    ASSERT_EQ(column1->GetTableInd(), column2->GetTableInd());
    ASSERT_EQ(column1->IsNullable(), column2->IsNullable());
    ASSERT_EQ(column1->GetSerializedSize(), column2->GetSerializedSize());
  }
}
TEST(TupleTest, FieldSerializeDeserializeTest) {
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  char *p = buffer;
  MemHeap *heap = new SimpleMemHeap();
  for (int i = 0; i < 4; i++) {
    p += int_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 3; i++) {
    p += float_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 4; i++) {
    p += char_fields[i].SerializeTo(p);
  }
  // Deserialize phase
  uint32_t ofs = 0;
  Field *df = nullptr;
  for (int i = 0; i < 4; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeInt, &df, false, heap);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(int_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(int_fields[4]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(int_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(int_fields[2]));
    heap->Free(df);
    df = nullptr;
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeFloat, &df, false, heap);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(float_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(float_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(float_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(float_fields[2]));
    heap->Free(df);
    df = nullptr;
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeChar, &df, false, heap);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(char_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(char_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[2]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(char_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(char_fields[2]));
    heap->Free(df);
    df = nullptr;
  }
}

TEST(TupleTest, RowTest) {
  SimpleMemHeap heap;
  TablePage table_page;
  // create schema
  std::vector<Column *> columns = {ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
                                   ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
                                   ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<Field> fields = {Field(TypeId::kTypeInt, 188),
                               Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                               Field(TypeId::kTypeFloat, 19.99f)};
  auto schema = std::make_shared<Schema>(columns);
  Row row(fields);
  table_page.Init(0, INVALID_PAGE_ID, nullptr, nullptr);
  table_page.InsertTuple(row, schema.get(), nullptr, nullptr, nullptr);
  RowId first_tuple_rid;
  ASSERT_TRUE(table_page.GetFirstTupleRid(&first_tuple_rid));
  ASSERT_EQ(row.GetRowId(), first_tuple_rid);
  Row row2(row.GetRowId());
  ASSERT_TRUE(table_page.GetTuple(&row2, schema.get(), nullptr, nullptr));
  std::vector<Field *> &row2_fields = row2.GetFields();
  ASSERT_EQ(3, row2_fields.size());
  for (size_t i = 0; i < row2_fields.size(); i++) {
    ASSERT_EQ(CmpBool::kTrue, row2_fields[i]->CompareEquals(fields[i]));
  }
  ASSERT_TRUE(table_page.MarkDelete(row.GetRowId(), nullptr, nullptr, nullptr));
  table_page.ApplyDelete(row.GetRowId(), nullptr, nullptr);
}
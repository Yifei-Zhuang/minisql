#include <set>
#include <unordered_map>
#include <vector>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "storage/table_heap.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  setbuf(stdout, NULL);
  // init testing instance
  DBStorageEngine engine(db_file_name);
  SimpleMemHeap heap;
  const int row_nums = 10000;
  // create schema
  std::vector<Column *> columns = {ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
                                   ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
                                   ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;

  TableHeap *table_heap = TableHeap::Create(engine.bpm_, schema.get(), nullptr, nullptr, nullptr, &heap);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    table_heap->InsertTuple(row, nullptr);
    row_values[row.GetRowId().Get()] = fields;
    delete[] characters;
  }
  // 迭代器测试
  auto it = table_heap->Begin(nullptr);
  set<int64_t> Set;
  for (int i = 0; i < row_nums; i++) {
    ASSERT((Set.count(it->GetRowId().Get()) == 0), "[error] - 迭代器错误");
    Set.insert(it->GetRowId().Get());
    // 确保能够拿到row_nums个元素即可
    it++;
  }

  ASSERT_EQ(row_nums, row_values.size());
  for (auto row_kv : row_values) {
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  // 测试更新函数
  // 新的field
  string characters = "123";
  Fields *fields = new Fields{Field(TypeId::kTypeInt, 2000),
                              Field(TypeId::kTypeChar, const_cast<char *>(characters.c_str()), 3, true),
                              Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
  Row row(*fields);
  table_heap->InsertTuple(row, nullptr);
  // 测试能否get
  Row testGet(row.GetRowId());
  table_heap->GetTuple(&testGet, nullptr);
  // 确认数据写入
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(CmpBool::kTrue, testGet.GetField(i)->CompareEquals(fields->at(i)));
  }

  characters = "123";
  Fields *updated_fields = new Fields{Field(TypeId::kTypeInt, 1000),
                                      Field(TypeId::kTypeChar, const_cast<char *>(characters.c_str()), 3, true),
                                      Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-1.f, 1.f))};
  Row updated_row(*updated_fields);
  table_heap->UpdateTuple(updated_row, row.GetRowId(), nullptr);
  Row testUpdated(row.GetRowId());
  table_heap->GetTuple(&testUpdated, nullptr);
  // 确认数据写入
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(CmpBool::kTrue, testUpdated.GetField(i)->CompareEquals(updated_fields->at(i)));
  }
}

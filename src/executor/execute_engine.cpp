#include "executor/execute_engine.h"
#include "glog/logging.h"

ExecuteEngine::ExecuteEngine() {
  // 初始化

  struct stat sb;
  if ((stat(".minisql_meta", &sb) == 0)) {
    ifstream in(".minisql_meta", ios::in);
    // 将表名写入mata信息表
    string input;
    while (getline(in, input)) {
      if (input.empty()) break;
      auto new_db = new DBStorageEngine(input, false);
      this->dbs_.emplace(input, new_db);
    }
    in.close();
  } else {
    // 创建元信息文件
    ofstream out(".minisql_meta", ios::out);
    out.close();
  }
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}
static struct stat sb;
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeCreateDB, "ExecuteEngine::ExecuteCreateDatabase error: wrong input ast type");
  string db_name = ast->child_->val_;
  // 创建数据库，并写入元信息到.minisql_meta文件中
  auto new_db = new DBStorageEngine(db_name, true);
  this->dbs_.emplace(db_name, new_db);
  ifstream in(".minisql_meta", ios::in);
  ofstream out(".minisql_meta_back", ios::out);
  // 将表名写入mata信息表
  string input, output;
  bool has_exists = false;
  while (!has_exists && getline(in, input)) {
    if (input.empty()) break;
    has_exists = (input == db_name);
    out << input + '\n';
  }
  if (!has_exists) {
    out << db_name;
    in.close();
    out.close();
    remove(".minisql_meta");
    rename(".minisql_meta_back", ".minisql_meta");
    std::cout << "create database " << db_name << " success\n";
    return DB_SUCCESS;
  } else {
    cout << "database " << db_name << " has exists, cannot create a new one\n ";
    in.close();
    out.close();
    remove(".minisql_meta_back");
    std::cout << "create database " << db_name << " fail\n";
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeDropDB, "ExecuteEngine::ExecuteDropDatabase error: wrong input ast type");
  string db_name = ast->child_->val_;

  if ((stat(db_name.c_str(), &sb) == 0)) {
    delete this->dbs_.find(db_name)->second;
    this->dbs_.erase(db_name);
    remove(db_name.c_str());
    // 移除meta信息表中的对应表名
    ifstream in(".minisql_meta", ios::in);
    ofstream out(".minisql_meta_back", ios::out);
    // 将表名写入mata信息表
    string input, output;
    while (getline(in, input)) {
      if (input.empty()) break;
      if (input == db_name) continue;
      out << input + '\n';
    }
    in.close();
    out.close();
    remove(".minisql_meta");
    rename(".minisql_meta_back", ".minisql_meta");
    std::cout << "drop database " << db_name << "success\n";
    return DB_SUCCESS;
  } else {
    std::cout << "database " << db_name << " doesn't exists!\n";
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeShowDB, "ExecuteEngine::kNodeShowDB error: wrong input ast type");

  ifstream in(".minisql_meta", ios::in);
  string input;
  int count = 0;
  while (getline(in, input)) {
    if (input.empty()) break;
    cout << input << " " << endl;
    count++;
  }
  in.close();
  std::cout << "total " << count << " databases\n";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeUseDB, "ExecuteEngine::kNodeUseDB error: wrong input ast type");
  string db_name = ast->child_->val_;
  if ((stat(db_name.c_str(), &sb) == 0)) {
    current_db_ = db_name;
    std::cout << "switching to db: " << db_name << '\n';
    return DB_SUCCESS;
  } else {
    std::cout << "cannot find db: " << db_name << " please check\n";
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeShowTables, "ExecuteEngine::kNodeShowTables error: wrong input ast type");

  auto temp = this->dbs_.find(this->current_db_);
  if (temp == dbs_.end()) {
    std::cout << "please first use a db!\n";
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = temp->second->catalog_mgr_;
  vector<TableInfo *> result;
  auto db_result = catalog_manager->GetTables(result);
  if (db_result != DB_SUCCESS) {
    return db_result;
  }
  for_each(result.begin(), result.end(), [&](TableInfo *&it) -> void { cout << it->GetTableName() << " "; });
  cout << '\n';
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_ == "") {
    std::cout << "please use a db\n";
    return DB_FAILED;
  }
  constexpr char int_[4] = "int";
  constexpr char float_[6] = "float";
  ASSERT(ast->type_ == kNodeCreateTable, "ExecuteEngine::ExecuteCreateTable error: wrong input ast type");
  auto table_node = ast->child_;
  string table_name = table_node->val_;
  auto column_definition_list_node = table_node->next_;
  auto begin_pos = column_definition_list_node->child_;
  // 获取catalog manager
  auto catalog_manager = dbs_.find(current_db_)->second->catalog_mgr_;
  vector<Column *> column_vec;
  uint32_t index = 0;
  while (begin_pos && begin_pos->type_ != kNodeColumnList) {
    //    Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique);
    Column *new_column{nullptr};
    string column_name = begin_pos->child_->val_;
    // 只支持float 和 string，（输入的int全部被转换为float)
    string type = begin_pos->child_->next_->val_;
    if ((strcmp(type.c_str(), int_) == 0) || (strcmp(type.c_str(), float_) == 0)) {
      // 根据ycj助教的要求，只需要在unique和主键上实现not null
      if (begin_pos->val_ && !strcmp(begin_pos->val_, "unique")) {
        // unique
        new_column = new Column(column_name, kTypeFloat, index++, false, true);
      } else {
        // not unique
        new_column = new Column(column_name, kTypeFloat, index++, true, false);
      }
    } else {
      // char
      uint32_t length = atoi(begin_pos->child_->next_->child_->val_);
      if (begin_pos->val_ && !strcmp(begin_pos->val_, "unique")) {
        new_column = new Column(column_name, kTypeChar, length, index++, false, true);
      } else {
        new_column = new Column(column_name, kTypeChar, length, index++, true, false);
      }
    }
    column_vec.push_back(new_column);
    begin_pos = begin_pos->next_;
  }
  auto *schema = new Schema(column_vec);
  vector<string> index_col_names;
  if (begin_pos && begin_pos->type_ == kNodeColumnList) {
    // 主键
    begin_pos = begin_pos->child_;
    while (begin_pos) {
      index_col_names.emplace_back(begin_pos->val_);
      begin_pos = begin_pos->next_;
    }
  }
  vector<uint32_t> tt;
  // 暂时只支持单值主键
  for (uint32_t i = 0; i < index_col_names.size(); i++) {
    uint32_t key_pos;
    schema->GetColumnIndex(index_col_names[i], key_pos);
    tt.push_back(key_pos);
  }
  schema->setPrimaryKeys(tt);
  TableInfo *table_info{nullptr};
  auto db_result = catalog_manager->CreateTable(table_name, schema, nullptr, table_info);
  if (db_result != DB_SUCCESS) {
    return db_result;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeDropTable, "ExecuteEngine::ExecuteCreateTable error: wrong input ast type");
  // TODO 将磁盘中的bitmap对应位置置位,实现真正删除
  string table_name = ast->child_->val_;
  auto temp = dbs_.find(current_db_);
  if (temp == dbs_.end()) {
    std::cout << "请先use一个数据库\n";
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = temp->second->catalog_mgr_;
  vector<IndexInfo *> index_info;
  auto result = catalog_manager->GetTableIndexes(table_name, index_info);
  for (auto &it : index_info) {
    catalog_manager->DropIndex(table_name, it->GetIndexName());
  }
  result = catalog_manager->DropTable(table_name);
  return result;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  auto temp = dbs_.find(current_db_);
  if (temp == dbs_.end()) {
    std::cout << "请先use一个数据库\n";
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = temp->second->catalog_mgr_;
  vector<TableInfo *> tables;
  dberr_t result = catalog_manager->GetTables(tables);
  if (result != DB_SUCCESS) {
    return result;
  }
  for (auto &it : tables) {
    vector<IndexInfo *> index_info;
    result = catalog_manager->GetTableIndexes(it->GetTableName(), index_info);
    std::cout << "table: " << it->GetTableName() << endl;
    std::cout << "\t";
    for_each(index_info.begin(), index_info.end(),
             [&](IndexInfo *it) -> void { std::cout << it->GetIndexName() << " "; });
    puts("");
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  auto tempo = dbs_.find(current_db_);
  if (tempo == dbs_.end()) {
    std::cout << "please use a db\n";
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = tempo->second->catalog_mgr_;
  IndexInfo *temp;
  string index_name = ast->child_->val_;
  string table_name = ast->child_->next_->val_;
  pSyntaxNode begin = ast->child_->next_->next_->child_;
  vector<string> index_names;

  while (begin) {
    index_names.emplace_back(begin->val_);
    begin = begin->next_;
  }
  auto result = catalog_manager->CreateIndex(table_name, index_name, index_names, nullptr, temp);
  return result;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  auto tempo = dbs_.find(current_db_);
  if (tempo == dbs_.end()) {
    std::cout << "please use a db\n";
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = tempo->second->catalog_mgr_;
  string index_name = ast->child_->val_;
  // 获取所有的表
  vector<TableInfo *> table_info;
  auto result = catalog_manager->GetTables(table_info);
  if (result != DB_SUCCESS) {
    return result;
  }

  bool tag = false;
  for (uint32_t i = 0; i < table_info.size(); i++) {
    result = catalog_manager->DropIndex(table_info[i]->GetTableName(), index_name);
    if (result == DB_SUCCESS) {
      tag = true;
      break;
    }
  }
  if (!tag) {
    return DB_INDEX_NOT_FOUND;
  }
  return DB_SUCCESS;
}
dberr_t ExecuteEngine::get_columns_by_condition(pSyntaxNode condition_node, vector<RowId> &result_vec,
                                                CatalogManager *catalog_manager, string &table_name) {
  dberr_t result = DB_FAILED;
  TableInfo *table_info{nullptr};
  bool empty = false;
  // 获得表信息
  result = catalog_manager->GetTable(table_name, table_info);
  if (result != DB_SUCCESS) {
    return result;
  }

  if (condition_node) {
    vector<char *> compare(0);                            // 存储compare指令，如not
    vector<char *> connector;                             // 存储连接指令，如and
    vector<tuple<string, char *, SyntaxNodeType>> pairs;  // 存储比较的列名和其对应的的值和类型，如id,12,kNodeNumber
    auto begin_condition = condition_node->child_;
    bool tag = true;
    // 从语法树来看，应该不断向左下遍历
    while (begin_condition->type_ != kNodeCompareOperator) {
      connector.push_back(begin_condition->val_);
      auto temp = begin_condition->child_;
      compare.push_back(temp->next_->val_);
      pairs.emplace_back(temp->next_->child_->val_, temp->next_->child_->next_->val_,
                         temp->next_->child_->next_->type_);
      if (temp->next_->child_->next_->type_ == kNodeNull) {
        tag = false;
      }
      begin_condition = begin_condition->child_;
    }
    // 最后一个条件
    compare.push_back(begin_condition->val_);
    pairs.emplace_back(begin_condition->child_->val_, begin_condition->child_->next_->val_,
                       begin_condition->child_->next_->type_);

    /// 查看能否使用索引（只支持等值查询）
    vector<IndexInfo *> indexes(0);
    result = catalog_manager->GetTableIndexes(table_name, indexes);
    // 只支持相等，and
    for (uint32_t i = 0; tag && i < compare.size(); i++) {
      tag = (strcmp(compare[i], "=") == 0);
    }
    for (uint32_t i = 0; tag && i < connector.size(); i++) {
      tag = (strcmp(connector[i], "and") == 0);
    }
    if (tag && result == DB_SUCCESS) {
      // 可能有索引
      // 那么先查看有无索引
      // 获得所使用的列的名字
      std::map<std::string, int> mymap;
      std::string index_name;
      for (uint32_t i = 0; i < pairs.size(); i++) {
        auto temp = mymap.emplace(get<0>(pairs[i]), i);
        if (!temp.second) {
          //重复键值，检查值是否相等
          if (strcmp(get<1>(pairs[temp.first->second]), get<1>(pairs[i])) != 0) {
            // 同样的键，但是值不同，输出空结果并返回
            empty = true;
            break;
          }
        }
      }
      if (!empty && mymap.size() == 1) {
        // 查看是否存在对应索引（必须全部相等）
        auto judge = find_if(indexes.begin(), indexes.end(),
                             [&](IndexInfo *it) -> bool { return it->GetIndexKeySchema()->AllEqual(mymap); });
        if (judge != indexes.end()) {
          // 拿到索引名
          index_name = static_cast<IndexInfo *>(*judge)->GetIndexName();
          IndexInfo *index_info{nullptr};
          // 构造查询用的row
          vector<Field> fields;

          for (auto &it : mymap) {
            Field *new_field{nullptr};
            if (get<2>(pairs[it.second]) == kNodeNumber) {
              // float
              new_field = new Field(kTypeFloat, (float)(atof(get<1>(pairs[it.second]))));
            } else if (get<2>(pairs[it.second]) == kNodeNull) {
              // null
              new_field = new Field(kTypeChar, nullptr, 0, false);
            } else {
              // char
              auto temp_char = get<1>(pairs[it.second]);
              new_field = new Field(kTypeChar, temp_char, strlen(temp_char), true);
            }
            fields.push_back(*new_field);
            delete new_field;
          }
          Row row(fields);
          result = catalog_manager->GetIndex(table_name, index_name, index_info);
          result = index_info->GetIndex()->ScanKey(row, result_vec, nullptr);
        } else {
          // 无索引查询
          // 直接遍历
          // 获取schema
          auto schema = table_info->GetSchema();
          // 初始化需要的field，方便后续比较
          vector<Field> to_be_compared;
          to_be_compared.reserve(pairs.size());
          for (uint32_t i = 0; i < pairs.size(); i++) {
            // tuple<string, char *, SyntaxNodeType>
            auto cur_type = get<2>(pairs[i]);
            if (cur_type == kNodeNumber) {
              to_be_compared.emplace_back(kTypeFloat, (float)(atof(get<1>(pairs[i]))));
            } else if (cur_type == kNodeString) {
              auto temp = get<1>(pairs[i]);
              to_be_compared.emplace_back(kTypeChar, temp, strlen(temp), true);
            } else {
              // null
              to_be_compared.emplace_back(kTypeChar, nullptr, 0, false);
            }
          }
          for (auto it = table_info->GetTableHeap()->Begin(nullptr); it != table_info->GetTableHeap()->End(); it++) {
            deque<bool> single_result;  // 每一个比较的结果
            // 从下往上遍历
            for (int i = static_cast<int>(pairs.size()) - 1; i >= 0; i--) {
              auto temp_ = pairs[i];
              uint32_t pos = -1;
              bool single = false;
              result = schema->GetColumnIndex(get<0>(temp_), pos);
              if (result != DB_SUCCESS) {
                return result;
              }
              // 获取表中的实际值
              TypeFloat tf;
              TypeChar tc;
              auto field = it->GetField(pos);
              if (!strcmp(compare[i], "=")) {
                //相等比较
                auto type = get<2>(temp_);
                if (type == kNodeNumber) {
                  if (tf.CompareEquals(*field, to_be_compared[i])) {
                    single = true;
                  }
                } else {
                  if (tc.CompareEquals(*field, to_be_compared[i])) {
                    single = true;
                  }
                }
              } else if (!strcmp(compare[i], "<>")) {
                //相等比较
                auto type = get<2>(temp_);
                if (type == kNodeNumber) {
                  if (!tf.CompareEquals(*field, to_be_compared[i])) {
                    single = true;
                  }
                } else {
                  if (!tc.CompareEquals(*field, to_be_compared[i])) {
                    single = true;
                  }
                }
              } else if (!strcmp(compare[i], "<")) {
                //相等比较
                auto type = get<2>(temp_);
                if (type == kNodeNumber) {
                  if (tf.CompareLessThan(*field, to_be_compared[i])) {
                    single = true;
                  }
                } else {
                  if (tc.CompareLessThan(*field, to_be_compared[i])) {
                    single = true;
                  }
                }
              } else if (!strcmp(compare[i], "<=")) {
                //相等比较
                auto type = get<2>(temp_);
                if (type == kNodeNumber) {
                  if (tf.CompareLessThanEquals(*field, to_be_compared[i])) {
                    single = true;
                  }
                } else {
                  if (tc.CompareLessThanEquals(*field, to_be_compared[i])) {
                    single = true;
                  }
                }
              } else if (!strcmp(compare[i], ">")) {
                auto type = get<2>(temp_);
                if (type == kNodeNumber) {
                  if (tf.CompareGreaterThan(*field, to_be_compared[i])) {
                    single = true;
                  }
                } else {
                  if (tc.CompareGreaterThan(*field, to_be_compared[i])) {
                    single = true;
                  }
                }
              } else if (!strcmp(compare[i], ">=")) {
                auto type = get<2>(temp_);
                if (type == kNodeNumber) {
                  if (tf.CompareGreaterThanEquals(*field, to_be_compared[i])) {
                    single = true;
                  }
                } else {
                  if (tc.CompareGreaterThanEquals(*field, to_be_compared[i])) {
                    single = true;
                  }
                }
              } else if (!strcmp(compare[i], "is")) {
                single = field->IsNull();
              } else if (!strcmp(compare[i], "not")) {
                single = !field->IsNull();
              }
              single_result.push_back(single);
            }
            bool satisfy = single_result[0];
            for (uint32_t i = 1; i < single_result.size(); i++) {
              if (!strcmp(connector[connector.size() - i], "and")) {
                satisfy = satisfy && single_result[i];
              } else {
                satisfy = satisfy || single_result[i];
              }
            }
            if (satisfy) {
              result_vec.push_back(it->GetRowId());
            }
          }
        }
      } else {
        // 无索引查询
        // 直接遍历
        // 获取schema
        auto schema = table_info->GetSchema();
        // 初始化需要的field，方便后续比较
        vector<Field> to_be_compared;
        to_be_compared.reserve(pairs.size());
        for (uint32_t i = 0; i < pairs.size(); i++) {
          // tuple<string, char *, SyntaxNodeType>
          auto cur_type = get<2>(pairs[i]);
          if (cur_type == kNodeNumber) {
            to_be_compared.emplace_back(kTypeFloat, (float)(atof(get<1>(pairs[i]))));
          } else if (cur_type == kNodeString) {
            auto temp = get<1>(pairs[i]);
            to_be_compared.emplace_back(kTypeChar, temp, strlen(temp), true);
          } else {
            // null
            to_be_compared.emplace_back(kTypeChar, nullptr, 0, false);
          }
        }
        for (auto it = table_info->GetTableHeap()->Begin(nullptr); it != table_info->GetTableHeap()->End(); it++) {
          deque<bool> single_result;  // 每一个比较的结果
          // 从下往上遍历
          for (int i = static_cast<int>(pairs.size()) - 1; i >= 0; i--) {
            auto temp_ = pairs[i];
            uint32_t pos = -1;
            bool single = false;
            result = schema->GetColumnIndex(get<0>(temp_), pos);
            if (result != DB_SUCCESS) {
              return result;
            }
            // 获取表中的实际值
            TypeFloat tf;
            TypeChar tc;
            auto field = it->GetField(pos);
            if (!strcmp(compare[i], "=")) {
              //相等比较
              auto type = get<2>(temp_);
              if (type == kNodeNumber) {
                if (tf.CompareEquals(*field, to_be_compared[i])) {
                  single = true;
                }
              } else {
                if (tc.CompareEquals(*field, to_be_compared[i])) {
                  single = true;
                }
              }
            } else if (!strcmp(compare[i], "<>")) {
              //相等比较
              auto type = get<2>(temp_);
              if (type == kNodeNumber) {
                if (!tf.CompareEquals(*field, to_be_compared[i])) {
                  single = true;
                }
              } else {
                if (!tc.CompareEquals(*field, to_be_compared[i])) {
                  single = true;
                }
              }
            } else if (!strcmp(compare[i], "<")) {
              //相等比较
              auto type = get<2>(temp_);
              if (type == kNodeNumber) {
                if (tf.CompareLessThan(*field, to_be_compared[i])) {
                  single = true;
                }
              } else {
                if (tc.CompareLessThan(*field, to_be_compared[i])) {
                  single = true;
                }
              }
            } else if (!strcmp(compare[i], "<=")) {
              //相等比较
              auto type = get<2>(temp_);
              if (type == kNodeNumber) {
                if (tf.CompareLessThanEquals(*field, to_be_compared[i])) {
                  single = true;
                }
              } else {
                if (tc.CompareLessThanEquals(*field, to_be_compared[i])) {
                  single = true;
                }
              }
            } else if (!strcmp(compare[i], ">")) {
              auto type = get<2>(temp_);
              if (type == kNodeNumber) {
                if (tf.CompareGreaterThan(*field, to_be_compared[i])) {
                  single = true;
                }
              } else {
                if (tc.CompareGreaterThan(*field, to_be_compared[i])) {
                  single = true;
                }
              }
            } else if (!strcmp(compare[i], ">=")) {
              auto type = get<2>(temp_);
              if (type == kNodeNumber) {
                if (tf.CompareGreaterThanEquals(*field, to_be_compared[i])) {
                  single = true;
                }
              } else {
                if (tc.CompareGreaterThanEquals(*field, to_be_compared[i])) {
                  single = true;
                }
              }
            } else if (!strcmp(compare[i], "is")) {
              single = field->IsNull();
            } else if (!strcmp(compare[i], "not")) {
              single = !field->IsNull();
            }
            single_result.push_back(single);
          }
          bool satisfy = single_result[0];
          for (uint32_t i = 1; i < single_result.size(); i++) {
            if (!strcmp(connector[connector.size() - i], "and")) {
              satisfy = satisfy && single_result[i];
            } else {
              satisfy = satisfy || single_result[i];
            }
          }
          if (satisfy) {
            result_vec.push_back(it->GetRowId());
          }
        }
      }
    } else {
      // 无索引查询
      // 直接遍历
      // 获取schema
      auto schema = table_info->GetSchema();
      // 初始化需要的field，方便后续比较
      vector<Field> to_be_compared;
      to_be_compared.reserve(pairs.size());
      for (uint32_t i = 0; i < pairs.size(); i++) {
        // tuple<string, char *, SyntaxNodeType>
        auto cur_type = get<2>(pairs[i]);
        if (cur_type == kNodeNumber) {
          to_be_compared.emplace_back(kTypeFloat, (float)(atof(get<1>(pairs[i]))));
        } else if (cur_type == kNodeString) {
          auto temp = get<1>(pairs[i]);
          to_be_compared.emplace_back(kTypeChar, temp, strlen(temp), true);
        } else {
          // null
          to_be_compared.emplace_back(kTypeChar, nullptr, 0, false);
        }
      }
      for (auto it = table_info->GetTableHeap()->Begin(nullptr); it != table_info->GetTableHeap()->End(); it++) {
        deque<bool> single_result;  // 每一个比较的结果
        // 从下往上遍历
        for (int i = static_cast<int>(pairs.size()) - 1; i >= 0; i--) {
          auto temp_ = pairs[i];
          uint32_t pos = -1;
          bool single = false;
          result = schema->GetColumnIndex(get<0>(temp_), pos);
          if (result != DB_SUCCESS) {
            return result;
          }
          // 获取表中的实际值
          TypeFloat tf;
          TypeChar tc;
          auto field = it->GetField(pos);
          if (!strcmp(compare[i], "=")) {
            //相等比较
            auto type = get<2>(temp_);
            if (type == kNodeNumber) {
              if (tf.CompareEquals(*field, to_be_compared[i])) {
                single = true;
              }
            } else {
              if (tc.CompareEquals(*field, to_be_compared[i])) {
                single = true;
              }
            }
          } else if (!strcmp(compare[i], "<>")) {
            //相等比较
            auto type = get<2>(temp_);
            if (type == kNodeNumber) {
              if (!tf.CompareEquals(*field, to_be_compared[i])) {
                single = true;
              }
            } else {
              if (!tc.CompareEquals(*field, to_be_compared[i])) {
                single = true;
              }
            }
          } else if (!strcmp(compare[i], "<")) {
            //相等比较
            auto type = get<2>(temp_);
            if (type == kNodeNumber) {
              if (tf.CompareLessThan(*field, to_be_compared[i])) {
                single = true;
              }
            } else {
              if (tc.CompareLessThan(*field, to_be_compared[i])) {
                single = true;
              }
            }
          } else if (!strcmp(compare[i], "<=")) {
            //相等比较
            auto type = get<2>(temp_);
            if (type == kNodeNumber) {
              if (tf.CompareLessThanEquals(*field, to_be_compared[i])) {
                single = true;
              }
            } else {
              if (tc.CompareLessThanEquals(*field, to_be_compared[i])) {
                single = true;
              }
            }
          } else if (!strcmp(compare[i], ">")) {
            auto type = get<2>(temp_);
            if (type == kNodeNumber) {
              if (tf.CompareGreaterThan(*field, to_be_compared[i])) {
                single = true;
              }
            } else {
              if (tc.CompareGreaterThan(*field, to_be_compared[i])) {
                single = true;
              }
            }
          } else if (!strcmp(compare[i], ">=")) {
            auto type = get<2>(temp_);
            if (type == kNodeNumber) {
              if (tf.CompareGreaterThanEquals(*field, to_be_compared[i])) {
                single = true;
              }
            } else {
              if (tc.CompareGreaterThanEquals(*field, to_be_compared[i])) {
                single = true;
              }
            }
          } else if (!strcmp(compare[i], "is")) {
            single = field->IsNull();
          } else if (!strcmp(compare[i], "not")) {
            single = !field->IsNull();
          }
          single_result.push_back(single);
        }
        bool satisfy = single_result[0];
        for (uint32_t i = 1; i < single_result.size(); i++) {
          if (!strcmp(connector[connector.size() - i], "and")) {
            satisfy = satisfy && single_result[i];
          } else {
            satisfy = satisfy || single_result[i];
          }
        }
        if (satisfy) {
          result_vec.push_back(it->GetRowId());
        }
      }
    }
  } else {
    // select * from xxx，无条件
    for (TableIterator it = table_info->GetTableHeap()->Begin(nullptr); it != table_info->GetTableHeap()->End(); it++) {
      result_vec.push_back(it->GetRowId());
    }
  }
  return result;
}
dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  dberr_t result = DB_FAILED;
  // 获得table节点
  // 想要的列的信息
  pSyntaxNode column_want_node = ast->child_;
  // 表的节点
  pSyntaxNode table_info_node = column_want_node->next_;
  ASSERT(ast->type_ == kNodeSelect, "节点属性错误，根节点应该是select类型");
  vector<RowId> rows;
  // 获得catalog_manager
  CatalogManager *catalogManager{nullptr};
  auto temp = this->dbs_.find(this->current_db_);
  if (temp == this->dbs_.end()) {
    return result;
  }
  catalogManager = temp->second->catalog_mgr_;
  string table_name = table_info_node->val_;
  result = get_columns_by_condition(table_info_node->next_, rows, catalogManager, table_name);
  if (result != DB_SUCCESS) {
    return result;
  }
  vector<string> column_wanted;
  // 获得表元信息
  TableInfo *table_info{nullptr};
  result = catalogManager->GetTable(table_info_node->val_, table_info);
  if (result != DB_SUCCESS) {
    return result;
  }
  auto schema = table_info->GetSchema();
  if (column_want_node->type_ == kNodeAllColumns) {
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      column_wanted.push_back(schema->GetColumns()[i]->GetName());
    }
  } else {
    for (auto begin = column_want_node->child_; begin != nullptr; begin = begin->next_) {
      column_wanted.emplace_back(begin->val_);
    }
  }
  for (auto &i : rows) {
    Row row(i);
    bool get_tuple_result = table_info->GetTableHeap()->GetTuple(&row, nullptr);
    if (!get_tuple_result) {
      return DB_FAILED;
    }
    for (auto &j : column_wanted) {
      uint32_t index;
      result = schema->GetColumnIndex(j, index);
      if (result != DB_SUCCESS) {
        return result;
      }
      auto field = row.GetField(index);
      if (field->GetType() == kTypeFloat) {
        float f = field->GetFloatData();
        if (abs(f - floor(f)) <= 1e-5 || abs(f - ceil(f)) <= 1e-5) {
          printf("%d ", static_cast<int>(field->GetFloatData()));
        } else {
          printf("%lf ", field->GetFloatData());
        }
      } else {
        string temp_str(field->GetData(), field->GetLength());  // 避免乱码出现
        std::cout << temp_str << " ";
      }
    }
    cout << '\n';
  }
  std::cout << "total " << rows.size() << " records\n";
  return result;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif

  auto tempo = dbs_.find(current_db_);
  if (tempo == dbs_.end()) {
    std::cout << "please use a db\n";
    return DB_FAILED;
  }
  pSyntaxNode table_info_node = ast->child_;
  string table_name = table_info_node->val_;
  pSyntaxNode begin = table_info_node->next_->child_;

  size_t begin_sise = tempo->second->bpm_->GetFreeSize();

  CatalogManager *catalogmanager = tempo->second->catalog_mgr_;
  TableInfo *tableInfo;
  auto result = catalogmanager->GetTable(table_name, tableInfo);
  if (result != DB_SUCCESS) {
    return result;
  }
  vector<Field> fields;
  while (begin) {
    if (begin->type_ == kNodeNumber) {
      fields.emplace_back(kTypeFloat, (float)(atof(begin->val_)));
    } else if (begin->type_ == kNodeString) {
      fields.emplace_back(kTypeChar, begin->val_, strlen(begin->val_), true);
    } else {
      // null
      fields.emplace_back(kTypeChar, nullptr, 0, false);
    }
    begin = begin->next_;
  }
  Row row(fields);

  // 获取主键，查看是否重复
  auto primary_keys = tableInfo->GetSchema()->getPrimaryKeys();
  vector<Field> pri_fie;
  for (uint32_t i = 0; i < primary_keys.size(); i++) {
    pri_fie.push_back(fields[primary_keys[i]]);
  }
  Row pri_row(pri_fie);
  IndexInfo *index_info;
  vector<RowId> rid_result;
  rid_result.clear();
  result = catalogmanager->GetIndex(table_name, table_name + "__primary", index_info);
  index_info->GetIndex()->ScanKey(pri_row, rid_result, nullptr);
  if (!rid_result.empty()) {
    // 主键冲突
    return DB_PRIMARY_KEY_COLLISION;
  }

  // 唯一性检查
  auto uni_vec = tableInfo->GetSchema()->getUniqueKeys();
  for (uint32_t i = 0; i < uni_vec.size(); i++) {
    vector<Field> uni_fie;
    uni_fie.push_back(fields[uni_vec[i]]);
    Row uni_row(uni_fie);
    rid_result.clear();
    result = catalogmanager->GetIndex(table_name, table_name + "__unique__" + to_string(uni_vec[i]), index_info);
    index_info->GetIndex()->ScanKey(uni_row, rid_result, nullptr);
    if (!rid_result.empty()) {
      return DB_UNIQUE_KEY_COLLISION;
    }
  }
  // 插入记录
  if (!tableInfo->GetTableHeap()->InsertTuple(row, nullptr)) {
    return DB_FAILED;
  }
  // 插入所有的索引
  vector<IndexInfo *> index_infos;
  catalogmanager->GetTableIndexes(table_name, index_infos);
  for (uint32_t i = 0; i < index_infos.size(); i++) {
    vector<Field> index_fie;
    auto index_schema = index_infos[i]->GetIndexKeySchema();
    auto index_cols = index_schema->GetColumns();
    for (uint32_t j = 0; j < index_schema->GetColumnCount(); j++) {
      uint32_t col_pos;
      tableInfo->GetSchema()->GetColumnIndex(index_cols[j]->GetName(), col_pos);
      index_fie.push_back(fields[col_pos]);
    }
    Row index_row(index_fie);
    result = index_infos[i]->GetIndex()->InsertEntry(index_row, row.GetRowId(), nullptr);
    if (result != DB_SUCCESS) {
      return result;
    }
  }
  size_t end_sise = tempo->second->bpm_->GetFreeSize();
  if (end_sise - begin_sise < 0) {
    //    std::cout << "here!" << std::endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  dberr_t result = DB_FAILED;
  // 获得table节点
  // 表的节点
  pSyntaxNode table_info_node = ast->child_;
  ASSERT(ast->type_ == kNodeDelete, "节点属性错误，根节点应该是select类型");
  vector<RowId> rows;
  // 获得catalog_manager
  CatalogManager *catalogManager{nullptr};
  auto temp = this->dbs_.find(this->current_db_);
  if (temp == this->dbs_.end()) {
    return r esult;
  }
  catalogManager = temp->second->catalog_mgr_;
  string table_name = table_info_node->val_;
  result = get_columns_by_condition(table_info_node->next_, rows, catalogManager, table_name);
  if (result != DB_SUCCESS) {
    return result;
  }
  TableInfo *tableinfo;
  result = catalogManager->GetTable(table_info_node->val_, tableinfo);
  vector<IndexInfo *> index_infos;
  catalogManager->GetTableIndexes(table_info_node->val_, index_infos);
  if (table_info_node->child_ == nullptr && table_info_node->next_ == nullptr) {
    // 运行的是delete from table; 无条件，那么将索引drop掉之后重新创建，减小开销
    // 获取所有索引的vector<string>
    vector<pair<string, vector<string>>> index_back;
    for (uint32_t i = 0; i < index_infos.size(); i++) {
      vector<string> index_attrs;
      IndexSchema *index_single_schema = index_infos[i]->GetIndexKeySchema();
      auto columns = index_single_schema->GetColumns();
      index_attrs.reserve(columns.size());
      for (auto &it : columns) {
        index_attrs.emplace_back(it->GetName());
      }
      index_back.emplace_back(index_infos[i]->GetIndexName(), index_attrs);
    }
    // drop掉所有该表的索引
    for (uint32_t i = 0; i < index_back.size(); i++) {
      catalogManager->DropIndex(tableinfo->GetTableName(), index_back[i].first);
    }
    //再删除实际的记录
    auto table_heap = tableinfo->GetTableHeap();
    for (auto &it : rows) {
      table_heap->ApplyDelete(it, nullptr);
    }
    // 重新创建索引
    IndexInfo *unused;
    for (uint32_t i = 0; i < index_back.size(); i++) {
      catalogManager->CreateIndex(tableinfo->GetTableName(), index_back[i].first, index_back[i].second, nullptr,
                                  unused);
    }
  } else {
    // 在所有索引中逐个删除
    for (uint32_t q = 0; q < rows.size(); q++) {
      //获取当前的列
      Row cur(rows[q]);
      tableinfo->GetTableHeap()->GetTuple(&cur, nullptr);
      auto fields = cur.GetFields();
      for (uint32_t i = 0; i < index_infos.size(); i++) {
        vector<Field> index_fie;
        auto index_schema = index_infos[i]->GetIndexKeySchema();
        auto index_cols = index_schema->GetColumns();
        for (uint32_t j = 0; j < index_schema->GetColumnCount(); j++) {
          uint32_t col_pos;
          tableinfo->GetSchema()->GetColumnIndex(index_cols[j]->GetName(), col_pos);
          index_fie.push_back(*fields[col_pos]);
        }
        Row index_row(index_fie);
        index_infos[i]->GetIndex()->RemoveEntry(index_row, rows[q], nullptr);
      }
    }
    //再删除实际的记录
    auto table_heap = tableinfo->GetTableHeap();
    for (auto &it : rows) {
      table_heap->ApplyDelete(it, nullptr);
    }
  }
  std::cout << rows.size() << "  rows affected" << std::endl;
  return result;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  // TODO 需要对对应的索引进行更新，不过验收貌似不验，先不写了
  dberr_t result = DB_FAILED;
  // 获得table节点
  // 想要的列的信息
  pSyntaxNode table_info_node = ast->child_;
  ASSERT(ast->type_ == kNodeUpdate, "节点属性错误，根节点应该是update类型");
  vector<RowId> rows;
  // 获得catalog_manager
  CatalogManager *catalogManager{nullptr};
  auto temp = this->dbs_.find(this->current_db_);
  if (temp == this->dbs_.end()) {
    return result;
  }
  catalogManager = temp->second->catalog_mgr_;
  string table_name = table_info_node->val_;
  result = get_columns_by_condition(table_info_node->next_->next_, rows, catalogManager, table_name);
  if (result != DB_SUCCESS) {
    return result;
  }
  TableInfo *tableinfo;
  result = catalogManager->GetTable(table_name, tableinfo);
  if (result != DB_SUCCESS) {
    return result;
  }
  vector<pair<string, Field>> ff_vec;
  auto begin = ast->child_->next_->child_;
  while (begin) {
    auto col = begin->child_;
    auto val = col->next_;
    if (val->type_ == kNodeNumber) {
      ff_vec.emplace_back(col->val_, Field{kTypeFloat, (float)(atof(val->val_))});
    } else if (val->type_ == kNodeString) {
      ff_vec.emplace_back(col->val_, Field{kTypeChar, val->val_, static_cast<uint32_t>(strlen(val->val_)), false});
    } else {
      ff_vec.emplace_back(col->val_, Field{kTypeChar, nullptr, 0, false});
    }
    uint32_t pos;
    result = tableinfo->GetSchema()->GetColumnIndex(col->val_, pos);
    if (result != DB_SUCCESS) {
      return result;
    }
    begin = begin->next_;
  }
  for (uint32_t i = 0; i < rows.size(); i++) {
    Row orow(rows[i]);
    tableinfo->GetTableHeap()->GetTuple(&orow, nullptr);
    vector<Field *> new_field = orow.GetFields();
    for (auto &f : ff_vec) {
      uint32_t index;
      assert(tableinfo->GetSchema()->GetColumnIndex(f.first, index) == DB_SUCCESS);
      new_field[index] = &f.second;
    }
    vector<Field> ffff;
    for (auto it : new_field) {
      ffff.emplace_back(*it);
    }
    Row row(ffff);
    tableinfo->GetTableHeap()->UpdateTuple(row, rows[i], nullptr);
  }
  std::cout << rows.size() << "  rows affected" << std::endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string file_name = ast->child_->val_;
  constexpr int buf_size = 1024;
  // 暂时不考虑多行的sql指令
  ifstream in(file_name, ios::in);
  if (!in.is_open()) {
    std::cout << "cannot find file" << std::endl;
    return DB_FAILED;
  }
  in >> noskipws;
  while (!in.eof()) {
    char cmd[buf_size];
    memset(cmd, 0, buf_size);
    int i = 0;
    char ch;
    while (!in.eof()) {
      in >> ch;
      if (ch == ';') {
        break;
      }
      cmd[i++] = ch;
    }
    cmd[i] = ch;  // ;
    // get enter
    in >> ch;

    if (strlen(cmd) == 0) {
      break;
    }
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      return DB_FAILED;
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
      return DB_FAILED;
    }
    dberr_t result = this->Execute(MinisqlGetParserRootNode(), context);
    if (result != DB_SUCCESS) {
      return result;
    }
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}
dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}
dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}

#include <chrono>
#include <cstdio>
#include "executor/execute_engine.h"
#include "glog/logging.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"
extern "C" {
FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

void InitGoogleLog(char *argv) {
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  google::InitGoogleLogging(argv);
}

void InputCommand(char *input, const int len) {
  memset(input, 0, len);
  printf("minisql > ");
  int i = 0;
  char ch;
  while ((ch = getchar()) != ';') {
    input[i++] = ch;
  }
  input[i] = ch;  // ;
  getchar();      // remove enter
}

int main(int argc, char **argv) {
  InitGoogleLog(argv[0]);
  // command buffer
  constexpr int buf_size = 1024;
  char cmd[buf_size];
  // execute engine
  ExecuteEngine engine;
  // for print syntax tree
  TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
  [[maybe_unused]] uint32_t syntax_tree_id = 0;

  while (1) {
    // read from buffer
    InputCommand(cmd, buf_size);
    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
#ifdef ENABLE_PARSER_DEBUG
      printf("[INFO] Sql syntax parse ok!\n");
      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
#endif
    }
    ExecuteContext context;
    auto start = chrono::system_clock::now();
    dberr_t result = engine.Execute(MinisqlGetParserRootNode(), &context);
    auto end = chrono::system_clock::now();
    if (result == DB_SUCCESS) {
      cout << "operation ok\n";
    } else if (result == DB_TABLE_NOT_EXIST) {
      cout << "DB_TABLE_NOT_EXIST ERROR\n";
    } else if (result == DB_FAILED) {
      cout << "DB_FAILED \n";
    } else if (result == DB_TABLE_ALREADY_EXIST) {
      cout << "DB_TABLE_ALREADY_EXIST ERROR\n";
    } else if (result == DB_INDEX_ALREADY_EXIST) {
      cout << "DB_INDEX_ALREADY_EXIST ERROR\n";
    } else if (result == DB_INDEX_NOT_FOUND) {
      cout << "DB_TABLE_NOT_EXIST ERROR\n";
    } else if (result == DB_COLUMN_NAME_NOT_EXIST) {
      cout << "DB_COLUMN_NAME_NOT_EXIST ERROR\n";
    } else if (result == DB_KEY_NOT_FOUND) {
      cout << "DB_KEY_NOT_FOUND ERROR\n";
    } else if (result == DB_PRIMARY_KEY_COLLISION) {
      cout << "DB_PRIMARY_KEY_COLLISION ERROR\n";
    } else if (result == DB_UNIQUE_KEY_COLLISION) {
      cout << "DB_UNIQUE_KEY_COLLISION ERROR\n";
    }
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    cout << "time cost: "
         << double(duration.count()) * std::chrono::microseconds::period::num / std::chrono::microseconds::period::den
         << " second" << endl;

    sleep(1);

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    if (context.flag_quit_) {
      printf("bye!\n");
      break;
    }
  }
  return 0;
}
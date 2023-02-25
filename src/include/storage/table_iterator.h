#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
 public:
  // you may define your own constructor based on your member variables
  explicit TableIterator();

  explicit TableIterator(Row *row, TableHeap *ownerHeap) : row_(row), owner_heap_(ownerHeap), rid{INVALID_PAGE_ID, 0} {}

  explicit TableIterator(Row *row, TableHeap *ownerHeap, RowId &rid) : row_(row), owner_heap_(ownerHeap), rid{rid} {}

  explicit TableIterator(const TableIterator &other);

  explicit TableIterator(const TableIterator &&other)
      : row_(other.row_), owner_heap_(other.owner_heap_), rid{other.rid} {}
  virtual ~TableIterator();
  inline bool operator==(const TableIterator &itr) const { return rid == itr.rid; }

  inline bool operator!=(const TableIterator &itr) const { return !(*this == itr); };

  inline TableIterator &operator=(const TableIterator &itr) noexcept {
    row_ = itr.row_;
    owner_heap_ = itr.owner_heap_;
    rid = itr.rid;
    return *this;
  }

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

 private:
  // add your own private member variables here
  Row *row_{nullptr};
  TableHeap *owner_heap_{nullptr};
  RowId rid{INVALID_PAGE_ID, 0};
};

#endif  // MINISQL_TABLE_ITERATOR_H

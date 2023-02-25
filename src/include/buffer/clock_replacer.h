#ifndef MINISQL_CLOCK_REPLACER_H
#define MINISQL_CLOCK_REPLACER_H

#include <list>
#include <map>
#include <mutex>
#include <unordered_set>
#include <utility>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

// TODO support Concurrency,?
using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class CLOCKReplacer : public Replacer {
 public:
  /**
   * Create a new CLOCKReplacer.
   * @param num_pages the maximum number of pages the CLOCKReplacer will be required to store
   */
  explicit CLOCKReplacer(size_t num_pages);

  /**
   * Destroys the CLOCKReplacer.
   */
  ~CLOCKReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // add your own private member variables here
  std::vector<pair<frame_id_t, bool>> victims_;
  int pointer_{0};
  int pos_can_pick_ = 0;
};

#endif  // MINISQL_CLOCK_REPLACER_H

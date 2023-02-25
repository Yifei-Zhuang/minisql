//
// Created by 庄毅非 on 2022/4/19.
//

#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages) {
  for (size_t i = 0; i < num_pages; i++) {
    victims_.emplace_back(i, true);
  }
}
CLOCKReplacer::~CLOCKReplacer() = default;
bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  if (pos_can_pick_ > 0) {
    int size = static_cast<int>(victims_.size());
    for (int i = 0; i < size; i++) {
      if (!victims_[pointer_].second) {
        break;
      }
      victims_[pointer_].second = false;
      pointer_ = (pointer_ + 1) % size;
    }
    victims_[pointer_].second = true;
    pos_can_pick_--;
    *frame_id = pointer_;
    return true;
  }
  return false;
};

void CLOCKReplacer::Pin(frame_id_t frame_id) {
  if (frame_id < static_cast<int>(victims_.size())) {
    if (!victims_[frame_id].second) {
      pos_can_pick_--;
    }
    victims_[frame_id].second = true;
  }
};

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  if (frame_id < static_cast<int>(victims_.size())) {
    if (victims_[frame_id].second) {
      pos_can_pick_++;
    }
    victims_[frame_id].second = false;
  }
};

size_t CLOCKReplacer::Size() { return pos_can_pick_; };
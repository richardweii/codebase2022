#pragma once

#include "block.h"

class BlockReader {
 public:
  explicit BlockReader(const char *data);

 private:
  char *data_;
};
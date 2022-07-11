#pragma once

#include <cstddef>
#include <cstring>
#include <string>
#include "config.h"
#include "util/hash.h"
#include "util/nocopy.h"
#include "util/slice.h"

namespace kv {

template <int Length>
class Internal NOCOPYABLE {
 public:
  struct InternalHash {
    size_t operator()(const Internal &key) const { return Hash(key.data(), key.size(), 0x1237423); }
  };

  struct InternalEqual {
    bool operator()(const Internal &a, const Internal &b) const { return strncmp(a.data(), b.data(), Length) == 0; }
  };

  Internal() = default;
  ~Internal() { delete[] data_; }
  explicit Internal(const std::string &s) {
    assert(s.size() == Length);
    data_ = new char[Length];
    memcpy(data_, s.c_str(), s.size());
  }

  explicit Internal(const Slice &s) {
    assert(s.size() == Length);
    data_ = new char[Length];
    memcpy(data_, s.data(), s.size());
  }

  Internal(Internal &&internal) {
    if (this != &internal) {
      delete[] data_;
      data_ = internal.data();
      internal.data_ = nullptr;
    }
  }

  Internal &operator=(Internal &&internal) {
    if (this != &internal) {
      delete[] data_;
      data_ = internal.data();
      internal.data_ = nullptr;
    }
    return *this;
  }

  char *data() const { return data_; }
  Slice toSlice() const { return Slice(data_, Length); }
  bool isValid() const { return data_ != nullptr; }
  size_t size() const { return Length; }

 private:
  char *data_ = nullptr;
};

using Key = Internal<kKeyLength>;
using Value = Internal<kValueLength>;

};  // namespace kv
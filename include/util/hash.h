#pragma once

#include <cstddef>
#include <cstdint>
namespace kv {

uint32_t Hash(const char *data, size_t n, uint32_t seed);

static inline uint32_t fuck_hash(const char *data, size_t n, uint32_t seed) {
  const uint32_t *a = reinterpret_cast<const uint32_t *>(data);
  return a[0] ^ a[1] ^ a[2] ^ a[3];
}

}  // namespace kv

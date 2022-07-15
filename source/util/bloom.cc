#include <cstddef>
#include <memory>
#include "util/filter.h"
#include "util/hash.h"
#include "util/slice.h"
namespace kv {

static uint32_t BloomHash(const Slice &key) { return Hash(key.data(), key.size(), 0xbc9f1d34); }

class BloomFilter : public Filter {
 public:
  explicit BloomFilter(int bits_per_key) : bits_per_key_(bits_per_key) {
    // We intentionally round down to reduce probing cost a little bit
    k_ = static_cast<size_t>(bits_per_key * 0.69);  // 0.69 =~ ln(2)
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
  }

  void CreateFilter(const Slice *keys, int n, char *dst) const override {
    // Compute bloom filter size (in both bits and bytes)
    size_t bits = n * bits_per_key_;

    // For small n, we can see a very high false positive rate.  Fix it
    // by enforcing a minimum bloom filter length.
    if (bits < 64) bits = 64;

    size_t bytes = (bits + 7) / 8;
    bits = bytes * 8;

    for (int i = 0; i < n; i++) {
      // Use double-hashing to generate a sequence of hash values.
      // See analysis in [Kirsch,Mitzenmacher 2006].
      uint32_t h = BloomHash(keys[i]);
      const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
      for (size_t j = 0; j < k_; j++) {
        const uint32_t bitpos = h % bits;
        dst[bitpos / 8] |= (1 << (bitpos % 8));
        h += delta;
      }
    }
  }

  void AddFilter(const Slice key, size_t bits, char *dst) const override {
    // For small n, we can see a very high false positive rate.  Fix it
    // by enforcing a minimum bloom filter length.
    if (bits < 64) bits = 64;

    size_t bytes = (bits + 7) / 8;
    bits = bytes * 8;

    // Use double-hashing to generate a sequence of hash values.
    // See analysis in [Kirsch,Mitzenmacher 2006].
    uint32_t h = BloomHash(key);
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < k_; j++) {
      const uint32_t bitpos = h % bits;
      dst[bitpos / 8] |= (1 << (bitpos % 8));
      h += delta;
    }
  }

  bool KeyMayMatch(const Slice &key, const Slice &bloom_filter) const override {
    const size_t len = bloom_filter.size();
    if (len < 2) return false;

    const char *array = bloom_filter.data();
    const size_t bits = len * 8;


    uint32_t h = BloomHash(key);
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < k_; j++) {
      const uint32_t bitpos = h % bits;
      if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) return false;
      h += delta;
    }
    return true;
  }

  ~BloomFilter() override = default;

 private:
  size_t bits_per_key_;
  size_t k_;
};

Filter* NewBloomFilterPolicy(int bits_per_key) {
  return (new BloomFilter(bits_per_key));
}

}  // namespace kv
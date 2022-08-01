#include "zipf.h"
#include <cstddef>
#include <vector>
#include "util/logging.h"

int main() {
  std::vector<int> counter(101, 0);
  Zipf zipf(100, 0x1231, 2);
  for (int i = 0; i < 1000; i++) {
    int num = zipf.Next();
    LOG_DEBUG("Zipf %d", num);
    counter[num]++;
  }
  for (size_t i = 1; i < counter.size(); i++) {
    LOG_INFO("num %zu, frequency %d", i, counter[i]);
  }
}
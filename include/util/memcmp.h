#pragma once

#include <smmintrin.h>
#include <cstdint>
inline int memcmp_128bit_eq_a(const void *str1, const void *str2) {
  __m128i item1 = _mm_loadu_si128((__m128i *)str1);
  __m128i item2 = _mm_loadu_si128((__m128i *)str2);
  __m128i result = _mm_cmpeq_epi64(item1, item2);
  // cmpeq returns 0xFFFFFFFFFFFFFFFF per 64-bit portion where equality is
  // true, and 0 per 64-bit portion where false

  // If result is not all ones, then there is a difference here
  if (!(unsigned int)_mm_test_all_ones(result)) {
    return -1;
  }
  return 0;
}

#pragma once

#include <smmintrin.h>

void *memcpy_128bit_as(void *dest, const void *src, size_t len) {
  __m128i *s = (__m128i *)src;
  __m128i *d = (__m128i *)dest;

  while (len--) {
    _mm_stream_si128(d++, _mm_stream_load_si128(s++));
  }
  _mm_sfence();
}
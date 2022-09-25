#include "util/memcpy.h"
#include <assert.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

// 采用16字节对齐
// 16B
void *memcpy_128bit_16B_as(void *dest, const void *src) {
  const __m128i *s = (__m128i *)src;
  __m128i *d = (__m128i *)dest;

  _mm_store_si128(d++, _mm_load_si128(s++));

  return dest;
}

// 32B
void *memcpy_128bit_32B_as(void *dest, const void *src) {
  __m256i *s = (__m256i *)src;
  __m256i *d = (__m256i *)dest;

  __m256i m0 = _mm256_loadu_si256(s);
  _mm256_storeu_si256(dest, m0);

  return dest;
}

// 64B
void *memcpy_512bit_64B_as(void *dest, const void *src) {
  const __m512i *s = (__m512i *)src;
  __m512i *d = (__m512i *)dest;

  _mm512_storeu_si512(d, _mm512_loadu_si512(s));
  _mm_sfence();

  return dest;
}

// 128B
void *memcpy_512bit_128B_as(void *dest, const void *src) {
  const __m512i *s = (__m512i *)src;
  __m512i *d = (__m512i *)dest;

  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 1
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 2
  _mm_sfence();

  return dest;
}

// 256B
void *memcpy_512bit_256B_as(void *dest, const void *src) {
  const __m512i *s = (__m512i *)src;
  __m512i *d = (__m512i *)dest;

  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 1
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 2
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 3
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 4
  _mm_sfence();

  return dest;
}

// 512B
void *memcpy_512bit_512B_as(void *dest, const void *src) {
  const __m512i *s = (__m512i *)src;
  __m512i *d = (__m512i *)dest;

  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 1
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 2
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 3
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 4
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 5
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 6
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 7
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 8

  _mm_sfence();

  return dest;
}

// 1024B
void *memcpy_512bit_1kB_as(void *dest, const void *src) {
  const __m512i *s = (__m512i *)src;
  __m512i *d = (__m512i *)dest;

  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 1
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 2
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 3
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 4
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 5
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 6
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 7
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 8
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 9
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 10
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 11
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 12
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 13
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 14
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 15
  _mm512_storeu_si512(d++, _mm512_loadu_si512(s++));  // 16
  _mm_sfence();

  return dest;
}

//--------------
void *memcpy_128B_as(void *dest, const void *src) {
  memcpy_512bit_128B_as(dest, src);
  return dest;
}

// 192 = 128 + 64
void *memcpy_192B_as(void *dest, const void *src) {
  memcpy_512bit_128B_as(dest, src);
  memcpy_512bit_64B_as(dest+128, src+128);
  return dest;
}

void *memcpy_256B_as(void *dest, const void *src) {
  memcpy_512bit_256B_as(dest, src);
  return dest;
}

// 320 = 256 + 64
void *memcpy_320B_as(void *dest, const void *src) {
  memcpy_512bit_256B_as(dest, src);
  memcpy_512bit_64B_as(dest+256, src+256);
  return dest;
}

// 384 = 256 + 128
void *memcpy_384B_as(void *dest, const void *src) {
  memcpy_512bit_256B_as(dest, src);
  memcpy_512bit_128B_as(dest+256, src+256);
  return dest;
}

// 448 = 256 + 128 + 64
void *memcpy_448B_as(void *dest, const void *src) {
  memcpy_512bit_256B_as(dest, src);
  memcpy_512bit_128B_as(dest+256, src+256);
  memcpy_512bit_64B_as(dest+384, src+384);
  return dest;
}

void *memcpy_512B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  return dest;
}

// 576 = 512+64
void *memcpy_576B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_512bit_64B_as(dest+512, src+512);
  return dest;
}

// 640 = 512+128
void *memcpy_640B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_512bit_128B_as(dest+512, src+512);
  return dest;
}

// 704 = 512 + 128 + 64
void *memcpy_704B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_192B_as(dest+512, src+512);
  return dest;
}

// 768 = 512 + 256
void *memcpy_768B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_512bit_256B_as(dest+512, src+512);
  return dest;
}

// 832 = 512 + 256 + 64
void *memcpy_832B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_320B_as(dest+512, src+512);
  return dest;
}

// 896 = 512 + 256 + 128
void *memcpy_896B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_384B_as(dest+512, src+512);
  return dest;
}

// 960 = 512 + 256 + 196
void *memcpy_960B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_448B_as(dest+512, src+512);
  return dest;
}

void *memcpy_1024B_as(void *dest, const void *src) {
  memcpy_512bit_1kB_as(dest, src);
  return dest;
}

void *my_memcpy(void *dest, const void *src, size_t len) {
  switch (len) {
    case 128:
      return memcpy_128B_as(dest, src);
    case 192:
      return memcpy_192B_as(dest, src);
    case 256:
      return memcpy_256B_as(dest, src);
    case 320:
      return memcpy_320B_as(dest, src);
    case 384:
      return memcpy_384B_as(dest, src);
    case 448:
      return memcpy_448B_as(dest, src);
    case 512:
      return memcpy_512B_as(dest, src);
    case 576:
      return memcpy_576B_as(dest, src);
    case 640:
      return memcpy_640B_as(dest, src);
    case 704:
      return memcpy_704B_as(dest, src);
    case 768:
      return memcpy_768B_as(dest, src);
    case 832:
      return memcpy_832B_as(dest, src);
    case 896:
      return memcpy_896B_as(dest, src);
    case 960:
      return memcpy_960B_as(dest, src);
    case 1024:
      return memcpy_1024B_as(dest, src);
    default:
      return memcpy(dest, src, len);
  }
}

#ifdef __cplusplus
}
#endif
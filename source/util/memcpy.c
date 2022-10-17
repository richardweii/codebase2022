#include "util/memcpy.h"
#include <assert.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

// 采用16字节对齐
// 16B
static inline void memcpy_128bit_16B_as(void *dest, const void *src) {
  __m128i xmm0;

  xmm0 = _mm_loadu_si128((const __m128i *)src);
  _mm_storeu_si128((__m128i *)dest, xmm0);
}

// 32B
static inline void memcpy_256bit_32B_as(void *dest, const void *src) {
  __m256i ymm0;

  ymm0 = _mm256_loadu_si256((const __m256i *)src);
  _mm256_storeu_si256((__m256i *)dest, ymm0);
}

// 64B
static inline void memcpy_512bit_64B_as(void *dest, const void *src) {
  __m512i zmm0;

  zmm0 = _mm512_loadu_si512((const void *)src);
  _mm512_storeu_si512((void *)dest, zmm0);
}

// 128B
static inline void memcpy_512bit_128B_as(void *dest, const void *src) {
  memcpy_512bit_64B_as(dest + 0 * 64, src + 0 * 64);
  memcpy_512bit_64B_as(dest + 1 * 64, src + 1 * 64);
}

// 256B
static inline void memcpy_512bit_256B_as(void *dest, const void *src) {
  memcpy_512bit_64B_as(dest + 0 * 64, src + 0 * 64);
  memcpy_512bit_64B_as(dest + 1 * 64, src + 1 * 64);
  memcpy_512bit_64B_as(dest + 2 * 64, src + 2 * 64);
  memcpy_512bit_64B_as(dest + 3 * 64, src + 3 * 64);
}

// 512B
static inline void memcpy_512bit_512B_as(void *dest, const void *src) {
  memcpy_512bit_64B_as(dest + 0 * 64, src + 0 * 64);
  memcpy_512bit_64B_as(dest + 1 * 64, src + 1 * 64);
  memcpy_512bit_64B_as(dest + 2 * 64, src + 2 * 64);
  memcpy_512bit_64B_as(dest + 3 * 64, src + 3 * 64);
  memcpy_512bit_64B_as(dest + 4 * 64, src + 4 * 64);
  memcpy_512bit_64B_as(dest + 5 * 64, src + 5 * 64);
  memcpy_512bit_64B_as(dest + 6 * 64, src + 6 * 64);
  memcpy_512bit_64B_as(dest + 7 * 64, src + 7 * 64);
}

// 1024B
static inline void memcpy_512bit_1kB_as(void *dest, const void *src) {
  memcpy_512bit_64B_as(dest + 0 * 64, src + 0 * 64);
  memcpy_512bit_64B_as(dest + 1 * 64, src + 1 * 64);
  memcpy_512bit_64B_as(dest + 2 * 64, src + 2 * 64);
  memcpy_512bit_64B_as(dest + 3 * 64, src + 3 * 64);
  memcpy_512bit_64B_as(dest + 4 * 64, src + 4 * 64);
  memcpy_512bit_64B_as(dest + 5 * 64, src + 5 * 64);
  memcpy_512bit_64B_as(dest + 6 * 64, src + 6 * 64);
  memcpy_512bit_64B_as(dest + 7 * 64, src + 7 * 64);
  memcpy_512bit_64B_as(dest + 8 * 64, src + 8 * 64);
  memcpy_512bit_64B_as(dest + 9 * 64, src + 9 * 64);
  memcpy_512bit_64B_as(dest + 10 * 64, src + 10 * 64);
  memcpy_512bit_64B_as(dest + 11 * 64, src + 11 * 64);
  memcpy_512bit_64B_as(dest + 12 * 64, src + 12 * 64);
  memcpy_512bit_64B_as(dest + 13 * 64, src + 13 * 64);
  memcpy_512bit_64B_as(dest + 14 * 64, src + 14 * 64);
  memcpy_512bit_64B_as(dest + 15 * 64, src + 15 * 64);
}

// 64B align
static inline void memcpy_512bit_64B_align(void *dest, const void *src) {
  __m512i zmm0;

  zmm0 = _mm512_load_si512((const void *)src);
  _mm512_store_si512((void *)dest, zmm0);
}

// 1KB align
static inline void memcpy_512bit_1kB_align(void *dest, const void *src) {
  memcpy_512bit_64B_align(dest + 0 * 64, src + 0 * 64);
  memcpy_512bit_64B_align(dest + 1 * 64, src + 1 * 64);
  memcpy_512bit_64B_align(dest + 2 * 64, src + 2 * 64);
  memcpy_512bit_64B_align(dest + 3 * 64, src + 3 * 64);
  memcpy_512bit_64B_align(dest + 4 * 64, src + 4 * 64);
  memcpy_512bit_64B_align(dest + 5 * 64, src + 5 * 64);
  memcpy_512bit_64B_align(dest + 6 * 64, src + 6 * 64);
  memcpy_512bit_64B_align(dest + 7 * 64, src + 7 * 64);
  memcpy_512bit_64B_align(dest + 8 * 64, src + 8 * 64);
  memcpy_512bit_64B_align(dest + 9 * 64, src + 9 * 64);
  memcpy_512bit_64B_align(dest + 10 * 64, src + 10 * 64);
  memcpy_512bit_64B_align(dest + 11 * 64, src + 11 * 64);
  memcpy_512bit_64B_align(dest + 12 * 64, src + 12 * 64);
  memcpy_512bit_64B_align(dest + 13 * 64, src + 13 * 64);
  memcpy_512bit_64B_align(dest + 14 * 64, src + 14 * 64);
  memcpy_512bit_64B_align(dest + 15 * 64, src + 15 * 64);
}
void my_memcpy_NKB_align(void *dest, const void *src, int n) {
  for (int i = 0; i < n; i++) {
    memcpy_512bit_1kB_align(dest + i*1024, src + i*1024);
  }
}

//--------------
static inline void memcpy_80B_as(void *dest, const void *src) {
  memcpy_512bit_64B_as(dest, src);
  memcpy_128bit_16B_as(dest + 64, src + 64);
}

static inline void memcpy_96B_as(void *dest, const void *src) {
  memcpy_512bit_64B_as(dest, src);
  memcpy_256bit_32B_as(dest + 64, src + 64);
}

static inline void memcpy_112B_as(void *dest, const void *src) {
  memcpy_512bit_64B_as(dest, src);
  memcpy_256bit_32B_as(dest + 64, src + 64);
  memcpy_128bit_16B_as(dest + 96, src + 96);
}

static inline void memcpy_128B_as(void *dest, const void *src) { memcpy_512bit_128B_as(dest, src); }

static inline void memcpy_144B_as(void *dest, const void *src) {
  memcpy_512bit_128B_as(dest, src);
  memcpy_128bit_16B_as(dest + 128, src + 128);
}

static inline void memcpy_160B_as(void *dest, const void *src) {
  memcpy_512bit_128B_as(dest, src);
  memcpy_256bit_32B_as(dest + 128, src + 128);
}

static inline void memcpy_176B_as(void *dest, const void *src) {
  memcpy_512bit_128B_as(dest, src);
  memcpy_256bit_32B_as(dest + 128, src + 128);
  memcpy_128bit_16B_as(dest + 160, src + 160);
}

// 192 = 128 + 64
static inline void memcpy_192B_as(void *dest, const void *src) {
  memcpy_512bit_128B_as(dest, src);
  memcpy_512bit_64B_as(dest + 128, src + 128);
}

static inline void memcpy_208B_as(void *dest, const void *src) {
  memcpy_192B_as(dest, src);
  memcpy_128bit_16B_as(dest + 192, src + 192);
}

// 224 = 128+64+32
static inline void memcpy_224B_as(void *dest, const void *src) {
  memcpy_192B_as(dest, src);
  memcpy_256bit_32B_as(dest + 192, src + 192);
}

// 240 = 128+64+32+16
static inline void memcpy_240B_as(void *dest, const void *src) {
  memcpy_224B_as(dest, src);
  memcpy_128bit_16B_as(dest + 224, src + 224);
}

static inline void memcpy_256B_as(void *dest, const void *src) { memcpy_512bit_256B_as(dest, src); }

// 320 = 256 + 64
static inline void memcpy_320B_as(void *dest, const void *src) {
  memcpy_512bit_256B_as(dest, src);
  memcpy_512bit_64B_as(dest + 256, src + 256);
}

// 384 = 256 + 128
static inline void memcpy_384B_as(void *dest, const void *src) {
  memcpy_512bit_256B_as(dest, src);
  memcpy_512bit_128B_as(dest + 256, src + 256);
}

// 448 = 256 + 128 + 64
static inline void memcpy_448B_as(void *dest, const void *src) {
  memcpy_512bit_256B_as(dest, src);
  memcpy_512bit_128B_as(dest + 256, src + 256);
  memcpy_512bit_64B_as(dest + 384, src + 384);
}

static inline void memcpy_512B_as(void *dest, const void *src) { memcpy_512bit_512B_as(dest, src); }

// 576 = 512+64
static inline void memcpy_576B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_512bit_64B_as(dest + 512, src + 512);
}

// 640 = 512+128
static inline void memcpy_640B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_512bit_128B_as(dest + 512, src + 512);
}

// 704 = 512 + 128 + 64
static inline void memcpy_704B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_192B_as(dest + 512, src + 512);
}

// 768 = 512 + 256
static inline void memcpy_768B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_512bit_256B_as(dest + 512, src + 512);
}

// 832 = 512 + 256 + 64
static inline void memcpy_832B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_320B_as(dest + 512, src + 512);
}

// 896 = 512 + 256 + 128
static inline void memcpy_896B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_384B_as(dest + 512, src + 512);
}

// 960 = 512 + 256 + 196
static inline void memcpy_960B_as(void *dest, const void *src) {
  memcpy_512bit_512B_as(dest, src);
  memcpy_448B_as(dest + 512, src + 512);
}

static inline void memcpy_1024B_as(void *dest, const void *src) { memcpy_512bit_1kB_as(dest, src); }

void my_memcpy(void *dest, const void *src, size_t len) {
  switch (len) {
    case 80:
      memcpy_80B_as(dest, src);
      return;
    case 96:
      memcpy_96B_as(dest, src);
      return;
    case 112:
      memcpy_112B_as(dest, src);
      return;
    case 128:
      memcpy_128B_as(dest, src);
      return;
    case 144:
      memcpy_144B_as(dest, src);
      return;
    case 160:
      memcpy_160B_as(dest, src);
      return;
    case 176:
      memcpy_176B_as(dest, src);
      return;
    case 192:
      memcpy_192B_as(dest, src);
      return;
    case 208:
      memcpy_208B_as(dest, src);
      return;
    case 224:
      memcpy_224B_as(dest, src);
      return;
    case 240:
      memcpy_240B_as(dest, src);
      return;
    case 256:
      memcpy_256B_as(dest, src);
      return;
    case 320:
      memcpy_320B_as(dest, src);
      return;
    case 384:
      memcpy_384B_as(dest, src);
      return;
    case 448:
      memcpy_448B_as(dest, src);
      return;
    case 512:
      memcpy_512B_as(dest, src);
      return;
    case 576:
      memcpy_576B_as(dest, src);
      return;
    case 640:
      memcpy_640B_as(dest, src);
      return;
    case 704:
      memcpy_704B_as(dest, src);
      return;
    case 768:
      memcpy_768B_as(dest, src);
      return;
    case 832:
      memcpy_832B_as(dest, src);
      return;
    case 896:
      memcpy_896B_as(dest, src);
      return;
    case 960:
      memcpy_960B_as(dest, src);
      return;
    case 1024:
      memcpy_1024B_as(dest, src);
      return;
    default:
      memcpy(dest, src, len);
      return;
  }
}

#ifdef __cplusplus
}
#endif
#pragma once

#include <smmintrin.h>
#include <immintrin.h> 
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif
void* my_memcpy(void* dest, const void* src, size_t len);

#ifdef __cplusplus
}
#endif
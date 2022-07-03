#pragma once
#include <memory>
#include <string>

constexpr int kKeyLength = 16;
constexpr int kValueLength = 128;
constexpr int kItemNum = 64;
constexpr int kBloomFilterBitsPerKey = 10;
constexpr int kBloomFilterSize = kBloomFilterBitsPerKey * kItemNum / 4 + 1;
constexpr int kDataBlockSize = (kKeyLength + kValueLength) * kItemNum + kBloomFilterSize;  // > 9KB

constexpr int kAlign = 8;  // kAlign show be powers of 2, say 2, 4 ,8, 16, 32, ...

inline constexpr int roundUp(unsigned int nBytes) { return ((nBytes) + (kAlign - 1)) & ~(kAlign - 1); }

template<typename Tp>
using Ptr = std::shared_ptr<Tp>;

using InternalKey = Ptr<std::string>;
using InternalValue = Ptr<std::string>;


InternalKey newInternalKey();

InternalValue newInternalValue();
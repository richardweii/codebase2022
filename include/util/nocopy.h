#pragma once

#define NOCOPYABLE : public _no_copyable

class _no_copyable {
 public:
  _no_copyable() = default;
 private:
  _no_copyable(const _no_copyable &nocopy);
  _no_copyable &operator=(const _no_copyable &nocopy);
};
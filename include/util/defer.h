#include <utility>
// C++ implementation of golang defer. execute the codes when leave the scope.
template <typename T>
class _Defer {
 public:
  _Defer(T &&d) : _d(std::move(d)) {}
  ~_Defer() { _d(); }

 private:
  T _d;
};

class Defer {
 public:
  template <typename T>
  _Defer<T> operator+(T &&d) {
    return _Defer<T>(std::forward<T>(d));
  }
};

#define CONCAT_IMPL(prefix, suffix) prefix##suffix
#define CONCAT(perfix, suffix) CONCAT_IMPL(prefix, suffix)
#define UNIQUE_NAME(prefix) CONCAT(prefix, __COUNTER__)
#define defer auto UNIQUE_NAME(defer_) = Defer() + [&]

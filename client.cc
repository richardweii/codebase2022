#include <cstdio>
#include <iostream>
#include <string>
#include "include/kv_engine.h"

using namespace kv;
using namespace std;
int main() {
  LocalEngine *local_engine = new LocalEngine();
  local_engine->start("172.16.5.129",
                      "12344");  // ip 必须写具体ip，不能直接写localhost和127.0.0.1
  local_engine->write("hello", "world");
  local_engine->write("holly", "shit");
  {
    string value;
    value.resize(5);

    local_engine->read("hello", value);
    if (value != "world") {
      fprintf(stdout, "value : %s\n", value.c_str());
      std::cout << value << std::endl;
      fprintf(stderr, "something wrong happened when read 'hello'\n");
      exit(1);
    }
  }
  {
    string value;
    value.resize(4);

    local_engine->read("holly", value);
    if (value != "shit") {
      fprintf(stderr, "something wrong happened when read 'holly'/n");
      exit(1);
    }
  }
  return 0;
}
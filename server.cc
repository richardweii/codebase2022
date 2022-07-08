#include "include/kv_engine.h"

using namespace kv;

int main() {
  RemoteEngine *engine = new RemoteEngine();
  engine->start("", "12344");
  while (engine->alive())
    ;
  return 0;
}
#include "include/kv_engine.h"
#include <cstdio>
#include <string>


using namespace kv;
using namespace std;
int main() {
    LocalEngine *local_engine = new LocalEngine();
    local_engine->start("172.16.5.129", "12344"); // ip 必须写具体ip，不能直接写localhost和127.0.0.1
    local_engine->write("hello", "world");
    local_engine->write("holly", "shit");
    std::string value;
    local_engine->read("hello", value);
    if (value != "world") {
        fprintf(stderr, "something wrong happened when read 'hello'/n");
        exit(1);
    }

    local_engine->read("holly", value);
    if (value != "shit") {
        fprintf(stderr, "something wrong happened when read 'holly'/n");
        exit(1);
    }
    return 0;
}
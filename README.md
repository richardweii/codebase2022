### 一些注意事项：
1. 请详细阅读下文的参赛流程；
2. RDMA 注册内存时申请的内存(较大的话)4K对齐；
3. 避免过多过小的内存注册，以避免额外元信息开销；
4. 避免超大的内存注册，避免注册失败或RDMA访问变慢
5. 模拟环境跑demo，server在创建pd的时候默认用第一块网卡，而选手在添加模拟网卡时，可能会遇到一些网卡成功添加但不能使用的情况，这样在跑demo的时候，即使设置了对应网卡的addr和port，也会因为pd没对应上失败；
    解决方法是用rdma link delete NAME命令，删掉无法使用的网卡；
6. 模拟环境单次注册内存，到64M就会失败，erdma单次可以注册2G；
7. 自己写server/client测试的时候，如果复用同一个工程，注意在server/client编译时候得加link对应库文件；
8. 测试数据中保证value的长度为16byte的倍数，因此加解密过程中不需要对value进行padding


## 复赛测试相关流程
### 新增接口
1. deleteK 依据key删除记录；
2. set_aes 初始化加密方法环境，会在start后调用，如果有加密需求；
3. get_aes 获得加密信息，用作评测程序加密正确性判断
4. 修改write接口为bool write(const std::string &key, const std::string &value, bool use_aes = false)
   当use_aes = true时，需要writre加密数据，**read接口需要返回加密后的value**
```c++
  // ... init env
  if (!kv_imp->start(rdma_addr, rdma_port)) {
    //......
    exit(EXIT_FAILURE);
  }
  /* Init aes key message. */
  if(!kv_imp->set_aes()) {
    //......
    exit(EXIT_FAILURE);
  }
  /*Get aes key message. */
  crypto_message_t *aes_msg = kv_imp->get_aes();
  int m_ctxsize;
  IppsAESSpec* pAES = nullptr;
  /*Set aes context message. */
  set_crypt_context(aes_msg, &pAES, m_ctxsize);
  /* Testing Correctness Phase. */
  // ...
```

### 测验流程
0. 数据加密测试（不计入时间），和初赛流程一致，定长数据；后续测试不加密
1. 16个线程每个线程写12M的数据进入，value会存在几个长度，保证最大的数据量控制在26个G左右。这部分将value的长度写在key的某个字节或者固定某个线程写固定长度？
2. 16个线程读取12M的数据，判断是否长度以及数量一致
3. update 其中一部分的数据，比如其中10%的数据，同时判断数据的长度，保证最大的数据量
4. 每个线程read这部分数据并进行正确性验证
5. delete其中一部分数据
6. read这部分数据，验证是否已经删除
7. 重新写入部分数据量，并且更新这部分长度
8. 新这部分数据的读测试
9. 16个线程每个线程读写64M数据，hot data。


## 1. 赛题背景 / 2. 赛题描述
参见相关网页

## 3. 参数程序 
### 3.1 程序目标 
实现LocalEngine类（位于local节点），重载Start, Read, Write, Delete, Stop函数来实现引擎主节点的启动、读取、写入、停止功能。
实现set_aes函数初始化加密过程中所需要的必要信息，实现get_aes函数返回加密需要的信息，评测程序会根据get_aes返回的内容，自适应的选择对应的算法，进行评测。
在正确性验证阶段，评测程序会调用Write函数, 将标识位use_aes设置为true，选手根据标志位，实现加密，将加密后的数据写入内存。

实现RemoteEngine类（位于remote节点），重载Start, Stop函数来实现引擎远端节点的启动、停止功能。

我们会先在remote节点启动RemoteEngine对象，RemoteEngine在有通讯需要时应当作为server角色，LocalEngine作为client角色；Engine的启动参数如下：

```c++
/**
 * @param   {addr}    the address of RemoteEngine to connect
 * @param   {port}    the port of RemoteEngine to connect
 * @return  {bool}    true for success
 */
bool LocalEngine::start(const std::string addr, const std::string port);

/**
 * @param   {addr}    empty string
 * @param   {port}    the port the server listened
 * @return  {bool}    true for success
 */
bool RemoteEngine::start(const std::string addr, const std::string port)
```

请完全实现类中定义的接口，其中KV读写接口需要保证并发安全。

### 3.2 程序使用
**编译方式：**
mkdir build && cd build && cmake .. && make && make install

生成后install文件需要在'git项目/build/polarkv'目录下：

静态库文件install在'git项目/build/polarkv/lib'，命名为libpolarkv.a；

头文件路径install在'git项目/build/polarkv/include'（参考CMakeLists.txt）, 评测程序只会引用kv_engine.h，选手自包含所需头文件。


**评测程序调用方式：**
a. 在节点B生成remote端服务程序（期间KV库调用为生成RemoteEngine对象的并调用其Start接口）

b. 在节点A生成local端服务程序（期间KV库调用为生成LocalEngine节点对象的并调用其Start接口）

c. 在节点A依赖所规定的读写接口，进行程序正确性验证，通过后进行性能验证

d. 关闭并销毁local端服务程序、remote端服务程序（调用相应对象的Stop接口并销毁对象）

e. 完成验证后生成选手成绩


**调用样例：**
a. remote节点
```c++
int main(int argc, char *argv[]) {
  const std::string rdma_addr(argv[1]);
  const std::string rdma_port(argv[2]);
  
  RemoteEngine *kv_imp = new RemoteEngine();
  assert(kv_imp);
  kv_imp->start(rdma_addr, rdma_port);

  do {
    // sleep
  } while (kv_imp->alive());

  kv_imp->stop();
  delete kv_imp;

  return 0;
}
```

a. local节点
```c++
int main(int argc, char *argv[]) {
  const std::string rdma_addr(argv[1]);
  const std::string rdma_port(argv[2]);
  
  LocalEngine *kv_imp = new LocalEngine();
  assert(kv_imp);
  kv_imp->start(rdma_addr, rdma_port);

  do {
    // task
  } while (kv_imp->alive());

  kv_imp->stop();
  delete kv_imp;

  return 0;
}
```

### 3.3 提交输出 
每次评测结束后，评测程序会将性能评测的结果生成的信息（流程信息、选手日志）上传到OSS供参赛者查看。
选手所有日志信息请**使用标准输出**，评测程序会将标准输出重定向到日志文件并放到OSS上供参赛者查看，仅保留最近一次运行日志。

评测程序对整体日志大小有所限制（10MB），过大的日志文件将失效。

### 4. 参赛方法说明
1. 注册阿里云账号登陆阿里天池，找到"[第四届全球数据库大赛赛道1：云原生共享内存数据库性能优化]"，并报名参加
2. 登陆[code.aliyun.com](code.aliyun.com)并完成账号注册，如果阿里云账号对应已有code平台相应账号则会直接登陆，请勿链接到新版（codeup）
3. fork或者拷贝本仓库的代码到自己的仓库，重写Engine类中相关接口的实现。（本仓库的分支***demo_kv***有样例代码，可供参考）
4. 为了允许评测程序拉取选手代码，选手需要在项目的***成员***配置处将官方账号：***tianchi_polardbm_2022***添加到这个项目，角色为***Reporter***
5. 在天池提交成绩的入口，提交自己仓库的git地址（评测拉取的默认分支为master）
6. 等待评测结果以及排名更新

### 5. 测试环境
阿里云线上ECS机器，基于eRDMA技术；

Local节点限制为16 Core、8 GB内存，Remote节点限制为4 Core、32 GB内存；


操作系统: Alibaba Cloud Linux 2.1903 LTS 64位

gcc version 8.3.1 20190311 (Red Hat 8.3.1-3) (GCC)

### 6. 程序评测逻辑
评测程序分为2个阶段：

1. 正确性评测：
  验证KV数据读写正确性和完整性

2. 性能评测：
  2.1 写入压测；
  2.2 读取压测；
  2.3 读写混合压测；

### 7. 排名规则
在正确性验证通过的情况下，对性能评测整体计时，根据规模压测总用时从低到高进行排名（用时越短排名越靠前）

### 8. 选手lib的评测程序生成example
```cmake
cmake_minimum_required(VERSION 2.8)
project(TCTEST)

set(CMAKE_BUILD_TYPE "Release")

set(RDMA_LIB "-lrdmacm -libverbs -libumad -lpci -lefa")
set(CMAKE_CXX_FLAGS "-std=c++17 ${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "-pthread -ldl -lrt ${RDMA_LIB} -${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fopenmp")

set(CMAKE_INSTALL_PREFIX ${CMAKE_BINARY_DIR}/xxxxx)
MESSAGE("Install prefix is: ${CMAKE_INSTALL_PREFIX}")

add_subdirectory(include)
add_subdirectory(lib)

include_directories(include)
include_directories(${PROJECT_SOURCE_DIR}/lib)
link_directories(${PROJECT_SOURCE_DIR}/lib)

add_executable(local
            local.cc)
target_link_libraries(dblocal polarkv)
install(TARGETS dblocal
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include)

add_executable(remote
            remote.cc)
target_link_libraries(dbremote polarkv)
install(TARGETS dbremote
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include)
```

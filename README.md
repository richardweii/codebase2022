## 1. 赛题背景
参见相关网页

## 2. 赛题描述
参见相关网页

## 3. 参数程序 
### 3.1 程序目标 
实现LocalEngine类（位于local节点），重载Start, Read, Write, Stop函数来实现引擎主节点的启动、读取、写入、停止功能。

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

**调用方式：**
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
1. 注册阿里云账号登陆阿里天池，找到"[第四届全球数据库大赛赛道2：云原生共享内存数据库性能优化]"，并报名参加
2. 登陆[code.aliyun.com](code.aliyun.com)并完成账号注册，如果阿里云账号对应已有code平台相应账号则会直接登陆，请勿链接到新版（codeup）
3. fork或者拷贝本仓库的代码到自己的仓库，重写Engine类中相关接口的实现，并在*成员*配置处将**官方账号mzyeee**添加到这个项目，角色为**Reporter**
4. 在天池提交成绩的入口，提交自己仓库的git地址（评测拉取的分支为master）
5. 等待评测结果以及排名更新

### 5. 测试环境
阿里云线上ECS机器，基于eRDMA技术；

Local节点限制为16 Core、8 GB内存，Remote节点限制为4 Core、32 GB内存；

OS为Linux version 3.10.0-693.2.2.el7.x86_64

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

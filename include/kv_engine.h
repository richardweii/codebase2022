#pragma once

#include "string"

namespace kv {

/* Abstract base engine */
class Engine {
  public:
    virtual ~Engine();

    virtual bool start(const std::string addr, const std::string port) = 0;
    virtual void stop() = 0;

    virtual bool alive() = 0;
};

/* Local-side engine */
class LocalEngine : public Engine {
  public:
    virtual ~LocalEngine();

    bool start(const std::string addr, const std::string port) override;
    void stop() override;
    bool alive() override;

    bool write(const std::string key, const std::string value);
    bool read(const std::string key, std::string &value);
};

/* Remote-side engine */
class RemoteEngine : public Engine {
  public:
    virtual ~RemoteEngine();

    bool start(const std::string addr, const std::string port) override;
    void stop() override;
    bool alive() override;
};

}
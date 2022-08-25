#pragma once

#include "string"
#include "ippcp.h"

namespace kv {

/* Encryption algorithm competitor can choose. */
enum aes_algorithm {
  CTR=0, CBC, CBC_CS1, CBC_CS2, CBC_CS3, CFB, OFB
};

/* Algorithm relate message. */
typedef struct crypto_message_t {
  aes_algorithm algo;
  Ipp8u *key;
  Ipp32u key_len;
  Ipp8u *counter;
  Ipp32u counter_len;
  Ipp8u *piv;
  Ipp32u piv_len;
  Ipp32u blk_size;
  Ipp32u counter_bit;
} crypto_message_t;


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

  /* Init aes context message. */
  bool set_aes();
  /* Evaluation problem will call this function. */
  crypto_message_t* get_aes();
  
  bool write(const std::string &key, const std::string &value, bool use_aes = false);
  bool read(const std::string &key, std::string &value);
  /** The delete interface */
  bool deleteK(const std::string &key);
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
#include "kv_engine.h"

namespace kv {

Engine::~Engine() {}


LocalEngine::~LocalEngine() {}

/**
 * @description: start local engine service
 * @param {string} addr    the address string of RemoteEngine to connect
 * @param {string} port   the port of RemoteEngine to connect
 * @return {bool} true for success
 */
bool LocalEngine::start(const std::string addr, const std::string port) {
  return true;
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void LocalEngine::stop() {}

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool LocalEngine::alive() { return true; }

/**
 * @description: put a key-value pair to engine
 * @param {string} key
 * @param {string} value
 * @return {bool} true for success
 */
bool LocalEngine::write(const std::string key, const std::string value) {
  return true;
}

/**
 * @description: read value from engine via key
 * @param {string} key
 * @param {string} &value
 * @return {bool}  true for success
 */
bool LocalEngine::read(const std::string key, std::string &value) {
  return true;
}


RemoteEngine::~RemoteEngine() {}

/**
 * @description: start remote engine service
 * @param {string} addr   empty string for RemoteEngine as server
 * @param {string} port   the port the server listened
 * @return {bool} true for success
 */
bool RemoteEngine::start(const std::string addr, const std::string port) {
  /* true for success*/
  return true;
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void RemoteEngine::stop() {}

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool RemoteEngine::alive() { return true; }
}
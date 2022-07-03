#pragma once

#include <time.h>

#define GET_TIME \
  char tmp[64];  \
  time_t ptime;  \
  time(&ptime);  \
  strftime(tmp, sizeof(tmp), "%Y-%m-%d %H:%M:%S", localtime(&ptime));

#define TIME tmp

#ifndef NDEBUG
#define DEBUG(format, ...) printf(format, ##__VA_ARGS__)
#else
#define DEBUG(format, ...)
#endif

//定义日志级别
enum LOG_LEVEL {
  LOG_LEVEL_OFF = 0,
  LOG_LEVEL_FATAL,
  LOG_LEVEL_ERROR,
  LOG_LEVEL_INFO,
  LOG_LEVEL_DEBUG,
};

extern LOG_LEVEL level;

#define LOG_FATAL(format, ...)                                                                          \
  do {                                                                                                  \
    GET_TIME                                                                                            \
    if (level >= LOG_LEVEL_FATAL)                                                                       \
      DEBUG("\033[;31m[FATAL] %s %s:%d: " format "\n\033[0m", TIME, __FILE__, __LINE__, ##__VA_ARGS__); \
  } while (0)

#define LOG_ERROR(format, ...)                                                                          \
  do {                                                                                                  \
    GET_TIME                                                                                            \
    if (level >= LOG_LEVEL_ERROR)                                                                       \
      DEBUG("\033[;31m[ERROR] %s %s:%d: " format "\n\033[0m", TIME, __FILE__, __LINE__, ##__VA_ARGS__); \
  } while (0)

#define LOG_INFO(format, ...)                                                                            \
  do {                                                                                                   \
    GET_TIME                                                                                             \
    if (level >= LOG_LEVEL_INFO) DEBUG("\033[;34m[INFO]  %s: " format "\n\033[0m", TIME, ##__VA_ARGS__); \
  } while (0)

#define LOG_DEBUG(format, ...)                                                                            \
  do {                                                                                                    \
    GET_TIME                                                                                              \
    if (level >= LOG_LEVEL_DEBUG) DEBUG("\033[;33m[DEBUG] %s: " format "\n\033[0m", TIME, ##__VA_ARGS__); \
  } while (0)

#define LOG_ASSERT(condition, format, ...)                                                             \
  if (!(condition)) {                                                                                  \
    DEBUG("\033[;31mAssertion Failed! %s:%d: " format "\n\033[0m", __FILE__, __LINE__, ##__VA_ARGS__); \
    exit(1);                                                                                           \
  }

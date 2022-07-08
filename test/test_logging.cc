#include <gtest/gtest.h>
#include "util/logging.h"

TEST(TestLogging, Basic) {
    level = LOG_LEVEL_DEBUG;
    LOG_INFO("%s", "fuck debug");
    LOG_ERROR("%s", "fuck debug");
    LOG_DEBUG("%s", "fuck debug");
    LOG_FATAL("%s", "fuck fatal");
    LOG_ASSERT(1 != true, "%s", "fuck");
}
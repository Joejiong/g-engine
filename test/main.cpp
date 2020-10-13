#include <inttypes.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <base/logging.h>

int32_t main(int32_t argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    com_logstat_t logstat;
    logstat.sysevents = 16;
    com_device_t dev[1];
    memcpy(dev[0].type, "TTY", 4);
    COMLOG_SETSYSLOG(dev[0]);
    com_openlog("main", dev, 1, &logstat);

    return RUN_ALL_TESTS();
}

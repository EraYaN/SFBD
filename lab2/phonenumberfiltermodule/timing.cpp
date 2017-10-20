#include "timing.h"

using namespace std;

PerfClock::time_point now() {
    return PerfClock::now();
}
double diffToNanoseconds(PerfClock::time_point t1, PerfClock::time_point t2) {
    chrono::duration<double, nano> measurement = t2 - t1;
    return measurement.count();
}
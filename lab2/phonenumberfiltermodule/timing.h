#pragma once
#include <chrono>
#include <ratio>


typedef std::chrono::high_resolution_clock PerfClock;
typedef PerfClock::time_point perftime_t;
PerfClock::time_point now();
double diffToNanoseconds(PerfClock::time_point t1, PerfClock::time_point t2);

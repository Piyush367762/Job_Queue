CXX ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -Wpedantic -pthread
LDLIBS ?= -lws2_32

TARGETS := main.exe stress_test.exe wal_server.exe wal_submit.exe rest_server.exe

all: $(TARGETS)

main.exe: main.cpp JobQueue/TaskRegistry.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDLIBS)

stress_test.exe: JobQueue/stress.cpp JobQueue/TaskRegistry.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDLIBS)

wal_server.exe: wal_server.cpp JobQueue/TaskRegistry.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDLIBS)

wal_submit.exe: wal_submit.cpp JobQueue/TaskRegistry.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDLIBS)

rest_server.exe: rest_server.cpp JobQueue/TaskRegistry.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDLIBS)

clean:
	-@powershell -NoProfile -Command "Remove-Item -LiteralPath 'main.exe','stress_test.exe','wal_server.exe','wal_submit.exe','rest_server.exe' -Force -ErrorAction SilentlyContinue"

.PHONY: all clean

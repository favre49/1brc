ifndef NTHREADS
NTHREADS=$(shell nproc --all 2>/dev/null || sysctl -n hw.logicalcpu)
endif

CXX := g++
CXXFLAGS := -std=c++20 -O3 -march=native -DNUM_THREADS=$(NTHREADS)
DEBUG := false
DEBUG_CXXFLAG := -g -fsanitize=address -fsanitize=undefined -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize-recover=all -fstack-protector-all -D_FORTIFY_SOURCE=2
DEBUG_CXXFLAGS += -D_GLIBCXX_DEBUG -D_GLIBCXX_DEBUG_PEDANTIC

ifeq ($(DEBUG), true)
	CXXFLAGS += $(DEBUG_CXXFLAGS)
endif

1brc: 1brc.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@

.PHONY: profile
profile: 1brc
	perf record --call-graph dwarf,4096 ./1brc measurements_100m.txt

.PHONY: clean
clean:
	rm 1brc

.PHONY: measure
measure: 1brc
	hyperfine --warmup 1 --runs 5 './1brc measurements_100m.txt'

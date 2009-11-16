.PHONY: force

LDLIBS=-lfb303 -lthrift
LDFLAGS=-Lfb303/cpp
CFLAGS=-fPIC
INCLUDE=-I/usr/include/thrift -I/usr/include -Ifb303 -Ifb303/cpp/gen-cpp
LIBRARY=libscribe.a
SRCS=scribe_c.cpp $(wildcard gen-cpp/*.cpp)
OBJS=$(SRCS:%.cpp=%.o)
FB303_OBJS=$(wildcard fb303/cpp/*.o)

DEBUG=-g3

all: $(LIBRARY)
	@true

%.o:
	$(CC) $(CFLAGS) -c -static $(INCLUDE) $(DEBUG) $*.cpp -o $*.o

$(LIBRARY): $(OBJS)
	ar crv libscribe.a $(OBJS) $(FB303_OBJS)

test: $(LIBRARY) test.c
	$(CC) scribe_c.o -L. -lscribe test.c $(INCLUDE) $(DEBUG) $(LDFLAGS) $(LDLIBS) -o test

clean: force
	rm -f *~ $(OBJS) *.a
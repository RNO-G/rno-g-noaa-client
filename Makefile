LDFLAGS=-lpq
CFLAGS=-O2 -g -Wall -Wextra

.PHONY: all clean 
all: rno-g-noaa-client

clean: 
	rm -f rno-g-noaa-client


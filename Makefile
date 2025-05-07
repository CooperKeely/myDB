
CC = gcc
CFLAGS = -g -Wall - Wextra
SRC = $(wildcard src/*.c)
BIN = build/myDB

.PHONY = all clean test




all: build $(BIN)

build: 
	mkdir -p build
	mkdir -p src
	mkdir -p test

$(BIN): $(SRC)
	$(CC) $(CFLAGS) -o $@ $^

test: all
	pytest tests/integration

clean:
	rm -rf build

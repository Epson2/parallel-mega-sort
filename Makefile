CC = gcc
CFLAGS = -Wall -Wextra -O2

all: megasort

megasort: megasort.c
	$(CC) $(CFLAGS) -o megasort megasort.c

clean:
	rm -f megasort

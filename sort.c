#include <stdio.h>
#include <assert.h>

#define N 100000

int main(int argc, char **argv) {
	int array[N];

	int x, n=0;
	while(scanf("%d", &x) == 1) {
		assert(n < N);
		array[n++] = x;
	}

	for(int i=0; i < n; i++) {
		for(int j=i+1; j < n; j++)
			if(array[i] > array[j]) {
				x = array[i];
				array[i] = array[j];
				array[j] = x;
			}
		printf("%d\n", array[i]);
	}

	return 0;
}

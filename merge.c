#include <stdio.h>
#include <assert.h>

int main(int argc, char **argv) {
	assert(argc == 3);

	FILE *fp1 = fopen(argv[1], "r"), *fp2 = fopen(argv[2], "r");
	assert(fp1 != NULL && fp2 != NULL);

	int x, y, x_stat, y_stat;
	x_stat = fscanf(fp1, "%d", &x);
	y_stat = fscanf(fp2, "%d", &y);
	while(x_stat == 1 && y_stat == 1) {
		if(x < y) {
			printf("%d\n", x);
			x_stat = fscanf(fp1, "%d", &x);
		}
		else {
			printf("%d\n", y);
			y_stat = fscanf(fp2, "%d", &y);
		}
	}
	while(x_stat == 1) {
		printf("%d\n", x);
		x_stat = fscanf(fp1, "%d", &x);
	}
	while(y_stat == 1) {
		printf("%d\n", y);
		y_stat = fscanf(fp2, "%d", &y);
	}

	fclose(fp1);
	fclose(fp2);

	return 0;
}

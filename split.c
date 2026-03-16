#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

int main(int argc, char **argv) {
	assert(argc == 3);

	FILE *fp1 = fopen(argv[1], "r"), *fp2 = NULL;
	assert(fp1 != NULL);

	int max_lines = atoi(argv[2]);
	assert(max_lines > 0);

	char file_output_name[128];
	int file_counter = 0;

	int x, c;
	while(fscanf(fp1, "%d", &x) == 1) {
		if(fp2 == NULL) {
			sprintf(file_output_name, "part%d.txt", file_counter);
			file_counter++;
			c = 0;
			fp2 = fopen(file_output_name, "w");
			assert(fp2 != NULL);
		}
		fprintf(fp2, "%d\n", x);
		c++;
		if(c >= max_lines) {
			fclose(fp2);
			fp2 = NULL;
		}
	}

	fclose(fp1);
	if(fp2 != NULL)
		fclose(fp2);

	printf("%d\n", file_counter);

	return 0;
}

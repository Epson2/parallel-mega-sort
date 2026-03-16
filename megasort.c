#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static void die(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

static void die_msg(const char *msg) {
    fprintf(stderr, "megasort: %s\n", msg);
    exit(EXIT_FAILURE);
}

static int safe_close(int fd) {
    int r;
    do { r = close(fd); } while (r == -1 && errno == EINTR);
    return r;
}

static pid_t safe_wait(int *status) {
    pid_t r;
    do { r = wait(status); } while (r == -1 && errno == EINTR);
    return r;
}

static pid_t safe_waitpid(pid_t pid, int *status) {
    pid_t r;
    do { r = waitpid(pid, status, 0); } while (r == -1 && errno == EINTR);
    return r;
}

static void wait_or_die(pid_t pid) {
    int status = 0;
    if (safe_waitpid(pid, &status) == -1) die("waitpid");
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fprintf(stderr, "megasort: child %ld failed (status=%d)\n",
                (long)pid, status);
        exit(EXIT_FAILURE);
    }
}

static long get_cpu_count(void) {
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n < 1) n = 1;
    return n;
}

static void xdup2(int oldfd, int newfd) {
    if (dup2(oldfd, newfd) == -1) die("dup2");
}

static int xopen_ro(const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        fprintf(stderr, "megasort: failed to open for read: %s\n", path);
        die("open");
    }
    return fd;
}

static int xopen_wo_trunc(const char *path) {
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        fprintf(stderr, "megasort: failed to open for write: %s\n", path);
        die("open");
    }
    return fd;
}

/*
 * Run split_prog(input_file, max_lines) and capture integer printed to stdout.
 * Returns number of parts.
 */
static int run_split_capture_parts(const char *split_prog,
                                  const char *input_file,
                                  const char *max_lines_str) {
    int pipefd[2];
    if (pipe(pipefd) == -1) die("pipe");

    pid_t pid = fork();
    if (pid == -1) die("fork");

    if (pid == 0) {
        // Child: redirect stdout -> pipe write end
        if (safe_close(pipefd[0]) == -1) _exit(127);
        xdup2(pipefd[1], STDOUT_FILENO);
        // Optional: keep stderr as-is for debugging
        safe_close(pipefd[1]);

        execl(split_prog, split_prog, input_file, max_lines_str, (char *)NULL);
        perror("exec split_prog");
        _exit(127);
    }

    // Parent: read from pipe read end
    if (safe_close(pipefd[1]) == -1) die("close");

    char buf[256];
    ssize_t total = 0;

    while (1) {
        ssize_t r = read(pipefd[0], buf + total, (ssize_t)sizeof(buf) - 1 - total);
        if (r == 0) break;
        if (r < 0) {
            if (errno == EINTR) continue;
            die("read split output");
        }
        total += r;
        if (total >= (ssize_t)sizeof(buf) - 1) break;
    }
    buf[total] = '\0';
    safe_close(pipefd[0]);

    wait_or_die(pid);

    // split prints a single integer (possibly with newline)
    char *endptr = NULL;
    long parts = strtol(buf, &endptr, 10);
    if (endptr == buf || parts < 1 || parts > INT_MAX) {
        fprintf(stderr, "megasort: invalid split output: '%s'\n", buf);
        exit(EXIT_FAILURE);
    }
    return (int)parts;
}

/*
 * Spawn a sort child:
 *   sort_prog reads from infile (as stdin) and writes to outfile (as stdout)
 */
static pid_t spawn_sort_child(const char *sort_prog,
                              const char *infile,
                              const char *outfile) {
    pid_t pid = fork();
    if (pid == -1) die("fork");

    if (pid == 0) {
        int in_fd = xopen_ro(infile);
        int out_fd = xopen_wo_trunc(outfile);

        xdup2(in_fd, STDIN_FILENO);
        xdup2(out_fd, STDOUT_FILENO);

        safe_close(in_fd);
        safe_close(out_fd);

        execl(sort_prog, sort_prog, (char *)NULL);
        perror("exec sort_prog");
        _exit(127);
    }
    return pid;
}

/*
 * Spawn a merge child:
 *   merge_prog fileA fileB -> stdout
 * We redirect stdout to outfile.
 */
static pid_t spawn_merge_child(const char *merge_prog,
                               const char *fileA,
                               const char *fileB,
                               const char *outfile) {
    pid_t pid = fork();
    if (pid == -1) die("fork");

    if (pid == 0) {
        int out_fd = xopen_wo_trunc(outfile);
        xdup2(out_fd, STDOUT_FILENO);
        safe_close(out_fd);

        execl(merge_prog, merge_prog, fileA, fileB, (char *)NULL);
        perror("exec merge_prog");
        _exit(127);
    }
    return pid;
}

static void build_part_name(char *dst, size_t dstsz, const char *prefix, int idx) {
    // Example: prefix="part" -> "part0.txt"
    //          prefix="sorted_part" -> "sorted_part0.txt"
    int n = snprintf(dst, dstsz, "%s%d.txt", prefix, idx);
    if (n < 0 || (size_t)n >= dstsz) die_msg("filename too long");
}

static void build_temp_merge_name(char *dst, size_t dstsz, int round, int idx) {
    // Example: "merge_r0_3.txt"
    int n = snprintf(dst, dstsz, "merge_r%d_%d.txt", round, idx);
    if (n < 0 || (size_t)n >= dstsz) die_msg("temp filename too long");
}

static void remove_if_exists(const char *path) {
    if (unlink(path) == -1) {
        if (errno != ENOENT) {
            // Not fatal, but warn.
            fprintf(stderr, "megasort: warning: could not remove %s: %s\n",
                    path, strerror(errno));
        }
    }
}

int main(int argc, char **argv) {
    if (argc != 7) {
        fprintf(stderr,
                "Usage: %s [split_prog] [sort_prog] [merge_prog] [input_file] [max_lines] [output_file]\n",
                argv[0]);
        return EXIT_FAILURE;
    }

    const char *split_prog = argv[1];
    const char *sort_prog  = argv[2];
    const char *merge_prog = argv[3];
    const char *input_file = argv[4];
    const char *max_lines_str = argv[5];
    const char *output_file = argv[6];

    // Validate max_lines is a positive integer
    {
        char *end = NULL;
        long ml = strtol(max_lines_str, &end, 10);
        if (end == max_lines_str || *end != '\0' || ml < 1) {
            die_msg("max_lines must be a positive integer");
        }
    }

    // 1) Split the big file into partX.txt, capture number of parts.
    int parts = run_split_capture_parts(split_prog, input_file, max_lines_str);

    // 2) Sort each part in parallel (bounded by CPU count).
    long max_workers = get_cpu_count();
    if (max_workers < 1) max_workers = 1;

    int active = 0;

    for (int i = 0; i < parts; i++) {
        // If too many active children, wait for one to finish.
        while (active >= max_workers) {
            int status = 0;
            pid_t done = safe_wait(&status);
            if (done == -1) die("wait");

            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                fprintf(stderr, "megasort: a sort child failed (status=%d)\n", status);
                exit(EXIT_FAILURE);
            }
            active--;
        }

        char part_name[PATH_MAX];
        char sorted_name[PATH_MAX];
        build_part_name(part_name, sizeof(part_name), "part", i);
        build_part_name(sorted_name, sizeof(sorted_name), "sorted_part", i);

        (void)spawn_sort_child(sort_prog, part_name, sorted_name);
        active++;
    }

    // Wait for remaining sort children
    while (active > 0) {
        int status = 0;
        pid_t done = safe_wait(&status);
        if (done == -1) die("wait");
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            fprintf(stderr, "megasort: a sort child failed (status=%d)\n", status);
            exit(EXIT_FAILURE);
        }
        active--;
    }

    // 3) Merge sorted files until one remains.
    // We'll maintain a "current list" of filenames, starting with sorted_part0..N-1.
    char **curr = calloc((size_t)parts, sizeof(char *));
    if (!curr) die("calloc");
    int curr_n = parts;

    for (int i = 0; i < parts; i++) {
        char name[PATH_MAX];
        build_part_name(name, sizeof(name), "sorted_part", i);
        curr[i] = strdup(name);
        if (!curr[i]) die("strdup");
    }

    int round = 0;
    while (curr_n > 1) {
        int next_n = (curr_n + 1) / 2;
        char **next = calloc((size_t)next_n, sizeof(char *));
        if (!next) die("calloc");

        int pairs = curr_n / 2;
        int active_merges = 0;

        // Spawn merges with bounded parallelism to avoid CPU/disk thrashing.
        for (int p = 0; p < pairs; p++) {
            while (active_merges >= max_workers) {
                int status = 0;
                pid_t done = safe_wait(&status);
                if (done == -1) die("wait");
                if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                    fprintf(stderr, "megasort: a merge child failed (status=%d)\n", status);
                    exit(EXIT_FAILURE);
                }
                active_merges--;
            }

            int i = 2 * p;
            char tmpname[PATH_MAX];
            build_temp_merge_name(tmpname, sizeof(tmpname), round, p);

            (void)spawn_merge_child(merge_prog, curr[i], curr[i + 1], tmpname);
            active_merges++;
            next[p] = strdup(tmpname);
            if (!next[p]) die("strdup");
        }

        // Carry odd leftover forward unchanged.
        if (curr_n % 2 == 1) {
            next[pairs] = curr[curr_n - 1];
            curr[curr_n - 1] = NULL;
        }

        // Wait for remaining merges of this round.
        while (active_merges > 0) {
            int status = 0;
            pid_t done = safe_wait(&status);
            if (done == -1) die("wait");
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                fprintf(stderr, "megasort: a merge child failed (status=%d)\n", status);
                exit(EXIT_FAILURE);
            }
            active_merges--;
        }

        // Cleanup merged input files for this round.
        for (int p = 0; p < pairs; p++) {
            int i = 2 * p;
            remove_if_exists(curr[i]);
            remove_if_exists(curr[i + 1]);
            free(curr[i]);
            free(curr[i + 1]);
            curr[i] = curr[i + 1] = NULL;
        }

        // free curr container (filenames moved/freed already)
        free(curr);
        curr = next;
        curr_n = next_n;
        round++;
    }

    // The remaining file in curr[0] is the fully merged sorted output.
    if (curr_n != 1 || curr[0] == NULL) die_msg("merge produced no output");

    // Move/rename final to output_file (fallback to copy if rename fails across fs)
    if (rename(curr[0], output_file) == -1) {
        // fallback: copy contents
        int in_fd = xopen_ro(curr[0]);
        int out_fd = xopen_wo_trunc(output_file);

        char buf[1 << 16];
        while (1) {
            ssize_t r = read(in_fd, buf, sizeof(buf));
            if (r == 0) break;
            if (r < 0) {
                if (errno == EINTR) continue;
                die("read final");
            }
            ssize_t off = 0;
            while (off < r) {
                ssize_t w = write(out_fd, buf + off, (size_t)(r - off));
                if (w < 0) {
                    if (errno == EINTR) continue;
                    die("write final");
                }
                off += w;
            }
        }
        safe_close(in_fd);
        safe_close(out_fd);
        remove_if_exists(curr[0]);
    }

    free(curr[0]);
    free(curr);
    // Optional: delete the partX.txt files generated by split
    for (int i = 0; i < parts; i++) {
        char part_name[PATH_MAX];
        build_part_name(part_name, sizeof(part_name), "part", i);
        remove_if_exists(part_name);
    }

    return EXIT_SUCCESS;
}

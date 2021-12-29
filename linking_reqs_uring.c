#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include "liburing.h"

#define FILE_NAME       "/tmp/io_uring_link_test.txt"
#define STR             "Hello, io_uring!"
char buf[32];

int link_operations(struct io_uring *ring) {
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;

    int fd = open(FILE_NAME, O_RDWR | O_TRUNC | O_CREAT, 0644);
    if (fd < 0) {
        perror("open()");
        return 1;
    }

    // sqe #1 - write
    sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        fprintf(stderr, "failed to get SQE\n");
        return 1;
    }

    io_uring_prep_write(sqe, fd, STR, strlen(STR), 0);
    sqe->flags |= IOSQE_IO_LINK;

    // sqe #2 - read
    sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        fprintf(stderr, "failed to get SQE\n");
        return 1;
    }

    io_uring_prep_read(sqe, fd, buf, strlen(STR), 0);
    sqe->flags |= IOSQE_IO_LINK;

    // sqe #3 - close
    sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        fprintf(stderr, "failed to get SQE\n");
        return 1;
    }
    io_uring_prep_close(sqe, fd);

    // now submit to the queue!
    io_uring_submit(ring);

    for (int i = 0; i < 3; i++) {
        int ret = io_uring_wait_cqe(ring, &cqe);
        if (ret < 0) {
            fprintf(stderr, "error waiting for completion: %s\n", strerror(-ret));
            return 1;
        }
        if (cqe->res < 0) {
            fprintf(stderr, "error in async op: %s\n", strerror(-cqe->res));
        }
        printf("result of operation: %d\n", cqe->res);
        io_uring_cqe_seen(ring, cqe);
    }
    printf("buffer contents: %s\n:", buf);
    return 0;
}

int main(int argc, char *argv[]) {
    struct io_uring ring;

    int ret = io_uring_queue_init(8, &ring, 0);
    if (ret) {
        fprintf(stderr, "unable to setup uring: %s\n", strerror(-ret));
        return 1;
    }
    link_operations(&ring);
    io_uring_queue_exit(&ring);
    return 0;
}



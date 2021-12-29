#include <stdio.h>
#include <sys/eventfd.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#include "liburing.h"

struct io_uring ring;

#define EVENT_TYPE_SHUTDOWN 0
#define EVENT_TYPE_NORMAL   1
#define CQE_BATCH_SIZE      16

struct user_data {
    int event_type;
    int lock_flag; // indicator to main thread to unblock
};

void error_exit(char *message) {
    perror(message);
    exit(EXIT_FAILURE);
}

/*
* bg thread to block on the eventfd, and when it awakens,
* check the ring for CQEs. For each CQE available, poke the "lock"
* in the user_data to awaken the blocked read/write thread.
*
* This thread will not free any memory, except for the SHUTDOWN event message.
*/
void *ring_consumer(void *data) {
    struct io_uring_cqe *cqes[CQE_BATCH_SIZE];
    struct io_uring_cqe *cqe;
    int efd = (int) data;
    struct user_data *ud;
    eventfd_t v;

    bool must_exit = false;

    while (!must_exit) {
        // this blocks forever ... i think :(
        printf("about to block on eventfd\n");
        int ret = eventfd_read(efd, &v);
        if (ret < 0)
            error_exit("eventfd_read");

        // TODO: there's also io_uring_for_each_cqe()

        // make sure we get all the CQEs that are ready, else we won't get 
        // re-notified from the eventfd blocking
        while (1) {
            int cnt = io_uring_peek_batch_cqe(&ring, cqes, CQE_BATCH_SIZE);
            printf("JEB:: batch count = %d\n", cnt);
            if (!cnt) {
                // not sure when we'd get 0 count if the eventfd triggered, unless spurious :shrug:
                break;
            }
            
            cqe = *cqes;
            for (int i = 0; i < cnt; i++, cqe++) {
                if (cqe->res < 0) {
                    fprintf(stderr, "async error: %s\n", strerror(-cqe->res));
                    // cqe_seen & continue???
                }

                ud = io_uring_cqe_get_data(cqe);
                
                switch(ud->event_type){
                    case EVENT_TYPE_NORMAL:
                        // TODO: bump ud.lock/flag .....
                        break;
                    case EVENT_TYPE_SHUTDOWN:
                        printf("CONSUMER:: handle shutdown event\n");
                        must_exit = true;
                        break;
                }

                free(ud);
                io_uring_cqe_seen(&ring, cqe);
            }
        }
    }

    return NULL;
}

int setup_io_uring(int efd) {
    struct io_uring_params params;

    if (geteuid()) {
        fprintf(stderr, "You need root privileges to run this program.\n");
        return 1;
    }
    memset(&params, 0, sizeof(struct io_uring_params));
    params.flags |= IORING_SETUP_SQPOLL;
    params.sq_thread_idle = 120000; // 2 minutes in ms;

    int ret = io_uring_queue_init_params(4096, &ring, &params);
    if (ret) {
        fprintf(stderr, "unable to setup uring: %s\n", strerror(-ret));
        return 1;
    }
    io_uring_register_eventfd(&ring, efd);

    return 0;
}

int send_msg() {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    io_uring_prep_nop(sqe);

    struct user_data *ud = malloc(sizeof(struct user_data));
    ud->event_type = EVENT_TYPE_NORMAL;

    // TODO: set a lock/flag into the user_data struct

    io_uring_sqe_set_data(sqe, ud);
    io_uring_submit(&ring);

    return 0;
}

int send_shutdown() {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    io_uring_prep_nop(sqe);

    struct user_data *ud = malloc(sizeof(struct user_data));
    ud->event_type = EVENT_TYPE_SHUTDOWN;
    io_uring_sqe_set_data(sqe, ud);
    io_uring_submit(&ring);

    return 0;
}

int main(int argc, char *argv[]) {
    pthread_t t;
    int efd;

    efd = eventfd(0, 0);
    if (efd < 0)
        error_exit("eventfd");

    pthread_create(&t, NULL, ring_consumer, (void *)efd);
    sleep(2);
    setup_io_uring(efd);

    for (int i = 0; i < 4000; i++) {
        send_msg();
    }

    sleep(2);

    printf("about to send shutdown\n");
    send_shutdown();
    printf("sent shutdown\n");

    pthread_join(t, NULL);
    io_uring_queue_exit(&ring);

    return EXIT_SUCCESS;
}
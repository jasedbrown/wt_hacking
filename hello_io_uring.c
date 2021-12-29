#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <linux/fs.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#include <linux/io_uring.h>

#define QUEUE_DEPTH 1
#define BLOCK_SZ    1024


/* x86-specific. TODO(jeb) need arm64 equivalents! */
#define read_barrier()  __asm__ __volatile__("":::"memory")
#define write_barrier() __asm__ __volatile__("":::"memory")

struct app_io_sq_ring {
    unsigned *head;
    unsigned *tail;
    unsigned *ring_mask;
    unsigned *ring_entries;
    unsigned *flags;
    unsigned *array;
};

struct app_io_cq_ring {
    unsigned *head;
    unsigned *tail;
    unsigned *ring_mask;
    unsigned *ring_entries;
    struct io_uring_cqe *cqes;
};

struct submitter {
    int ring_fd;
    struct app_io_sq_ring sq_ring;
    struct io_uring_sqe *sqes;
    struct app_io_cq_ring cq_ring;
};

struct file_info {
    off_t file_sz;
    struct iovec iovecs[];
};

/*
 * This code was written before io_uring syscalls were part of the
  * std C libs. so, roll own syscall wrapper functions.
 */
int io_uring_setup(unsigned entries, struct io_uring_params *p)
{
    return (int) syscall(__NR_io_uring_setup, entries, p);
}

int io_uring_enter(int ring_fd, unsigned int to_submit, 
                    unsigned int min_complete, unsigned int flags)
{
    return (int) syscall(__NR_io_uring_enter, ring_fd, to_submit, min_complete, 
                flags, NULL, 0);
}

/*
* Most of this setup is boilerplate, hidden away in liburing.
*/
int app_setup_uring(struct submitter *s) {
    struct app_io_sq_ring *sring = &s->sq_ring;
    struct app_io_cq_ring *cring = &s->cq_ring;
    struct io_uring_params p;
    void *sq_ptr, *cq_ptr;

    // need to pass io_uring_params zerodout
    memset(&p, 0, sizeof(p));
    s->ring_fd = io_uring_setup(QUEUE_DEPTH, &p);
    if (s->ring_fd < 0) {
        perror("io_uring_setup failed");
        return 1;
    }

    /*
     * io_uring uses 2 shared kernel-user space ring buffers,
     * which can be jointly mapped with a single mmap() call in recent kernels.
     * While completion queue is directly manipulated, the submission queue
     * has an indirection array in between. We map that as well.
     */
    int sring_sz = p.sq_off.array + p.sq_entries * sizeof(unsigned);
    int cring_sz = p.cq_off.cqes + p.cq_entries * sizeof(struct io_uring_cqe);

    /*
     * IN kernel 5.4 and above, it is possible to map the submission and
     * completion buffers with a single mmap() call. Rather than check kernel
     * version, we check the features fiels of the io_uring_params struct.
     */
    if (p.features & IORING_FEAT_SINGLE_MMAP) {
        if (cring_sz > sring_sz) {
            sring_sz = cring_sz;
        }
        cring_sz = sring_sz;
    }

    // map in the submission and completion queue ring buffers.
    sq_ptr = mmap(0, sring_sz, PROT_READ | PROT_WRITE, 
            MAP_SHARED | MAP_POPULATE,
            s->ring_fd, IORING_OFF_SQ_RING);
    if (sq_ptr == MAP_FAILED) {
        perror("mmap failed");
        return 1;
    }

    if (p.features & IORING_FEAT_SINGLE_MMAP) {
        cq_ptr = sq_ptr;
    } else {
        // map in completion ring for older kernals 
        // NOP ....
    }

    // save fields in a global app_io_sq_ring struct for later reference
    sring->head = sq_ptr + p.sq_off.head;
    sring->tail = sq_ptr + p.sq_off.tail;
    sring->ring_mask = sq_ptr + p.sq_off.ring_mask;
    sring->ring_entries = sq_ptr + p.sq_off.ring_entries;
    sring->flags = sq_ptr + p.sq_off.flags;
    sring->array = sq_ptr + p.sq_off.array;

    // map in the submission queue entries array
    s->sqes = mmap(0, p.sq_entries * sizeof(struct io_uring_sqe),
                    PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE,
                    s->ring_fd, IORING_OFF_SQES);
    if (s->sqes == MAP_FAILED) {
        perror("sqes mmap failed");
        return 1;
    }

    // save fields in a global app_io_cq_ring struct for later reference
    cring->head = cq_ptr + p.cq_off.head;
    cring->tail = cq_ptr + p.cq_off.tail;
    cring->ring_mask = cq_ptr + p.cq_off.ring_mask;
    cring->ring_entries = cq_ptr + p.cq_off.ring_entries;
    cring->cqes = cq_ptr + p.cq_off.cqes;

    return 0;
}

/*
* Returns the size of a file whose fd is passed in.
*/
off_t get_file_size(int fd) {
    struct stat st;

    if (fstat(fd, &st) < 0) {
        perror("fstat error");
        return 1;
    }

    if (S_ISBLK(st.st_mode)) {
        unsigned long long bytes;
        if (ioctl(fd, BLKGETSIZE64, &bytes) != 0) {
            perror("ioctl");
            return 1;
        }
        return bytes;
    } else if (S_ISREG(st.st_mode)) {
        return st.st_size;
    }
    return -1;
}

/*
* Submit to the submission queue.
* Here we sdubmit requests to the submissions queue. There's many
* diff types of reqs you can perform. This will simply be the readv() call,
* which is sprcified via IORING_OP_READV.
*/
int submit_to_sq(char *file_path, struct submitter *s) {
    // this will be the user_data part of the sqe
    struct file_info *fi;

    int file_fd = open(file_path, O_RDONLY);
    if (file_fd < 0) {
        perror("failed to open file");
        return 1;
    }

    struct app_io_sq_ring *sring = &s->sq_ring;
    unsigned index = 0, current_block = 0, tail = 0, next_tail = 0;

    off_t file_sz = get_file_size(file_fd);
    if (file_sz < 0) {
        return 1;
    }
    off_t bytes_remaining = file_sz;
    int blocks = (int) file_sz / BLOCK_SZ;
    if (file_sz % BLOCK_SZ) blocks++;

    fi = malloc(sizeof(*fi) + sizeof(struct iovec) * blocks);
    if (!fi) {
        fprintf(stderr, "unable to alloc memory\n");
        return 1;
    }
    fi->file_sz = file_sz;

    // for each block of the file we want to read, alloc a buffer for the iovec struct
    // which is in the iovec array (see just above). The array is passed as part
    // of the submission (in the file_info struct).
    while (bytes_remaining) {
        off_t bytes_to_read = bytes_remaining;
        if (bytes_to_read > BLOCK_SZ)
            bytes_to_read = BLOCK_SZ;

        fi->iovecs[current_block].iov_len = bytes_to_read;

        void *buf;
        // this allocates the actual block of memory for the iovec (which is where
        // the file data will end up)
        if (posix_memalign(&buf, BLOCK_SZ, BLOCK_SZ)) {
            perror("posix_memalign fail!");
            return 1;
        }
        fi->iovecs[current_block].iov_base = buf;

        current_block++;
        bytes_remaining -= bytes_to_read;
    }

    // add our submission queue entry to the tail of the SQE ring buffer
    next_tail = tail = *sring->tail;
    next_tail++;
    read_barrier();
    index = tail & *s->sq_ring.ring_mask;
    struct io_uring_sqe *sqe = &s->sqes[index];
    sqe->fd = file_fd;
    sqe->flags = 0;
    sqe->opcode = IORING_OP_READV;
    sqe->addr = (unsigned long) fi->iovecs;
    sqe->len = blocks;
    sqe->off = 0;
    sqe->user_data = (unsigned long long) fi;
    sring->array[index] = index;
    tail = next_tail;

    // update the kernel so the kernel can see it
    if (*sring->tail != tail) {
        *sring->tail = tail;
        write_barrier();
    }

    // tell the kernel we've submitted via the io_uring_enter syscall.
    // we also pass IOURIUNG_ENTER_GETEVENTS flag which causes io_uring_enter
    // to wait until min_complete events complete.
    int ret = io_uring_enter(s->ring_fd, 1, 1, IORING_ENTER_GETEVENTS);
    if (ret < 0) {
        perror("failed on io_uring_enter");
        return 1;
    }

    return 0;
}

/*
* Output chars to stdout.
* Uses buffered output to be efficient, since we output char-by-char.
*/
void output_to_console(char *buf, int len) {
    while (len--) {
        fputc(*buf++, stdout);
    }
}

/*
* Read from the completion queue.
* here we read from the queue, get the data buffer with file data that was read,
* then print it to the console.
*/
void read_from_cq(struct submitter *s) {
    struct file_info *fi;
    struct app_io_cq_ring *cring = &s->cq_ring;
    struct io_uring_cqe *cqe;
    unsigned head = 0;

    head = *cring->head;

    do {
        read_barrier();
        // this is a ring buffer: if head == tail, the buffer is emtpy
        if (head == *cring->tail)
            break;

        // get the cqe
        cqe = &cring->cqes[head & *s->cq_ring.ring_mask];
        fi = (struct file_info*) cqe->user_data;
        if (cqe->res < 0) {
            fprintf(stderr, "Error: %s\n", strerror(abs(cqe->res)));
        }

        int blocks = (int) fi->file_sz / BLOCK_SZ;
        if (fi->file_sz % BLOCK_SZ) blocks++;

        for (int i = 0; i < blocks; i++)
            output_to_console(fi->iovecs[i].iov_base, fi->iovecs[i].iov_len);
        
        head++;
    } while (1);

    *cring->head = head;
    write_barrier();
}

int main(int argc, char *argv[]) {
    struct submitter *s;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <filename>\n", argv[0]);
        return 1;
    }

    s = malloc(sizeof(*s));
    if (!s) {
        perror("malloc");
        return 1;
    }
    memset(s, 0, sizeof(*s));

    if(app_setup_uring(s)) {
        fprintf(stderr, "unable to setup uring!\n");
        return 1;
    }

    for (int i = 1; i < argc; i++) {
        if(submit_to_sq(argv[i], s)) {
            fprintf(stderr, "error reading file\n");
            return 1;
        }
        read_from_cq(s);
    }

    return 0;
}




































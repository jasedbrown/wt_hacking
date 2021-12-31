#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include <string.h>
#include <pthread.h>
#include "wiredtiger.h"
#include "wiredtiger_ext.h"
#include "liburing.h"

static const char *home;
static const char *config;

#define EVENT_TYPE_SHUTDOWN 0
#define EVENT_TYPE_NORMAL   1
#define CQE_BATCH_SIZE      16


/* ! [JEB :: message handler] */
// EventHandler stuffs borrowed from https://source.wiredtiger.com/develop/message_handling.html

/*
 * Create our own event handler structure to allow us to pass context through to event handler
 * callbacks. For this to work the WiredTiger event handler must appear first in our custom event
 * handler structure.
 */
typedef struct {
    WT_EVENT_HANDLER h;
    const char *app_id;
} CUSTOM_EVENT_HANDLER;
 
/*
 * handle_wiredtiger_error --
 *     Function to handle error callbacks from WiredTiger.
 */
int
handle_wiredtiger_error(
  WT_EVENT_HANDLER *handler, WT_SESSION *session, int error, const char *message)
{
    CUSTOM_EVENT_HANDLER *custom_handler;
 
    /* Cast the handler back to our custom handler. */
    custom_handler = (CUSTOM_EVENT_HANDLER *)handler;
 
    /* Report the error on the console. */
    fprintf(stderr, "ERR app_id %s, thread context %p, error %d, message %s\n", custom_handler->app_id,
      (void *)session, error, message);
 
    /* Exit if the database has a fatal error. */
    if (error == WT_PANIC)
        exit(1);
 
    return (0);
}
 
/*
 * handle_wiredtiger_message --
 *     Function to handle message callbacks from WiredTiger.
 */
int
handle_wiredtiger_message(WT_EVENT_HANDLER *handler, WT_SESSION *session, const char *message)
{
    /* Cast the handler back to our custom handler. */
    fprintf(stderr, "MSG app id %s, thread context %p, message %s\n", ((CUSTOM_EVENT_HANDLER *)handler)->app_id,
      (void *)session, message);
 
    return (0);
}
/* ! [JEB :: message handler] */

/*
* A wrapper struct to be used with io_uring SQEs and CQEs.
*/
typedef struct __ring_event_user_data {
    int event_type;
    int lock_flag; // indicator to main thread to unblock

    // might just use posix cond vars ... or copy over WT's wr_condvar.
    // tbh just need something that looks like a for now ...

    // TODO: might need to capture event status ....
} RING_EVENT_USER_DATA;


/* ! [JEB :: FILE_SYSTEM] */
typedef struct __jeb_file_handle {
    WT_FILE_SYSTEM iface;

    // one ring to rule them all ...
    struct io_uring ring;
    
    // eventfd used in conjunction with the uring
    int efd;
    
    // a background thread that reads CQEs off the uring and notifies blocked callers
    pthread_t uring_consumer; 

    WT_EXTENSION_API *wtext;
} JEB_FILE_SYSTEM;


typedef struct jeb_file_handle {
    WT_FILE_HANDLE iface;

    /* pointer to the enclosing file system */
    JEB_FILE_SYSTEM *fs;

    int fd;

} JEB_FILE_HANDLE;

/* 
* initialization function; must be invoked at WT_CONNECTION creation, 
* via the extensions sxtring config
*/
int create_custom_file_system(WT_CONNECTION *, WT_CONFIG_ARG *);

/*
* Forward function declarations for file system API.
*/
static int jeb_fs_open(
  WT_FILE_SYSTEM *, WT_SESSION *, const char *, WT_FS_OPEN_FILE_TYPE, uint32_t, WT_FILE_HANDLE **);
static int jeb_fs_directory_list(
  WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, char ***, uint32_t *);
static int jeb_fs_directory_list_free(WT_FILE_SYSTEM *, WT_SESSION *, char **, uint32_t);
static int jeb_fs_exist(WT_FILE_SYSTEM *, WT_SESSION *, const char *, bool *);
static int jeb_fs_remove(WT_FILE_SYSTEM *, WT_SESSION *, const char *, uint32_t);
static int jeb_fs_rename(WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, uint32_t);
static int jeb_fs_size(WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *);
static int jeb_fs_terminate(WT_FILE_SYSTEM *, WT_SESSION *);

/*
* Forward function declarations for file handle API.
*/
static int jeb_fh_close(WT_FILE_HANDLE *, WT_SESSION *);
static int jeb_fh_lock(WT_FILE_HANDLE *, WT_SESSION *, bool);
static int jeb_fh_read(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, size_t, void *);
static int jeb_fh_size(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t *);
static int jeb_fh_sync(WT_FILE_HANDLE *, WT_SESSION *);
static int jeb_fh_sync_nowait(WT_FILE_HANDLE *, WT_SESSION *);
static int jeb_fh_truncate(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t);
static int jeb_fh_write(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, size_t, const void *);

static int jeb_fh_map(WT_FILE_HANDLE *, WT_SESSION *, void *, size_t *, void *);
static int jeb_fh_map_discard(WT_FILE_HANDLE *, WT_SESSION *, void *, size_t, void *);
static int jeb_fh_map_preload(WT_FILE_HANDLE *, WT_SESSION *, const void *, size_t, void *);
static int jeb_fh_unmap(WT_FILE_HANDLE *, WT_SESSION *, void *, size_t, void *);

/*
FILE_HANDLE functions not currently defined:

fh_advise
fh_extend
fh_extend_nolock
*/

int init_io_uring(struct io_uring *ring, int efd) {
    struct io_uring_params params;

    // if (geteuid()) {
    //     fprintf(stderr, "You need root privileges to run this program.\n");
    //     return 1;
    // }
    memset(&params, 0, sizeof(struct io_uring_params));
    // params.flags |= IORING_SETUP_SQPOLL;

    // TODO: make idle time a config option?
    params.sq_thread_idle = 120000; // 2 minutes in ms;

    // TODO: make queue depth a config option?
    int ret = io_uring_queue_init_params(16, ring, &params);
    if (ret) {
        fprintf(stderr, "unable to setup uring: %s\n", strerror(-ret));
        return 1;
    }
    io_uring_register_eventfd(ring, efd);

    return 0;
}

/*
* function exxecuted by a bg thread to block on the eventfd, and when it awakens,
* check the ring for CQEs. For each CQE available, poke the "lock"
* in the user_data to awaken the blocked read/write thread.
*
* This thread will not free any memory, except for the SHUTDOWN event message.
*/
void *ring_consumer(void *data) {
    struct io_uring_cqe *cqes[CQE_BATCH_SIZE];
    struct io_uring_cqe *cqe;
    JEB_FILE_SYSTEM *fs = (JEB_FILE_SYSTEM *) data;
    RING_EVENT_USER_DATA *ud;
    eventfd_t v;

    bool must_exit = false;

    while (!must_exit) {
        // this blocks forever ... i think :(
        printf("about to block on eventfd\n");
        int ret = eventfd_read(fs->efd, &v);
        if (ret < 0)
            // TODO: find some better way to handle this error
            exit(1);

        // TODO: there's also io_uring_for_each_cqe()

        // make sure we get all the CQEs that are ready, else we won't get 
        // re-notified from the eventfd blocking
        while (1) {
            int cnt = io_uring_peek_batch_cqe(&fs->ring, cqes, CQE_BATCH_SIZE);
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

                // TODO: see if there's a way to batch update the pointer here, instead of doing it one at a time.
                // io_uring_for_each_cqe() has a helper function to do that, i think ....
                io_uring_cqe_seen(&fs->ring, cqe);
            }
        }
    }

    return (NULL);
}

/*
* Initialization function for the custom file system/handle.
*/
int create_custom_file_system(WT_CONNECTION *conn, WT_CONFIG_ARG *config) {
    JEB_FILE_SYSTEM *fs;
    WT_EXTENSION_API *wtext;
    WT_FILE_SYSTEM *file_system;
    // struct io_uring *ring = NULL;
    int ret = 0;
    int efd;

    wtext = conn->get_extension_api(conn);

    if ((fs = calloc(1, sizeof(JEB_FILE_SYSTEM))) == NULL) {
        (void)wtext->err_printf(wtext, NULL, "failed to allocate custom file system: %s",
                wtext->strerror(wtext, NULL, ENOMEM));
        return (ENOMEM);
    }

    fs->wtext = wtext;
    file_system = (WT_FILE_SYSTEM *)fs;

    // NOTE: this is where we can parse the config string to set values into the custom FS,
    // but I'm omitting for now (not sure if i need ....)

    file_system->fs_directory_list = jeb_fs_directory_list;
    file_system->fs_directory_list_free = jeb_fs_directory_list_free;
    file_system->fs_exist = jeb_fs_exist;
    file_system->fs_open_file = jeb_fs_open;
    file_system->fs_remove = jeb_fs_remove;
    file_system->fs_rename = jeb_fs_rename;
    file_system->fs_size = jeb_fs_size;
    file_system->terminate = jeb_fs_terminate;

    // now, set up the uring
    efd = eventfd(0, 0);
    if (efd < 0) {
        (void)wtext->err_printf(wtext, NULL, "failed to create eventfd: %s",
                wtext->strerror(wtext, NULL, efd));
        free(fs);
        exit (1);
    }
    fs->efd = efd;
    if ((ret = init_io_uring(&fs->ring, efd)) != 0) {
        (void)wtext->err_printf(wtext, NULL, "failed to create uring: %s",
                wtext->strerror(wtext, NULL, ret));
        free(fs);
        exit(1);
    }

    if ((ret = pthread_create(&fs->uring_consumer, NULL, ring_consumer, (void *)fs)) != 0) {
        (void)wtext->err_printf(wtext, NULL, "failed to create uring: %s",
                wtext->strerror(wtext, NULL, ret));
        // TODO: probably need better clean up code, esp after init'ing the uring
        free(fs);
        exit(1);
    }

    printf("JEB::create_custom_file_system about to set FS into the connection\n");
    if ((ret = conn->set_file_system(conn, file_system, NULL)) != 0) {
        (void)wtext->err_printf(wtext, NULL, "WT_CONNECTION.set_file_system: %s", 
                wtext->strerror(wtext, NULL, ret));
        free(fs);
        exit(1);
    }

    printf("JEB::create_custom_file_system successfully set up custom file system!\n");
    return (0);
}

static int 
jeb_fs_open(WT_FILE_SYSTEM *fs, WT_SESSION *session, const char *name , 
    WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags , WT_FILE_HANDLE **file_handlep) {
    JEB_FILE_HANDLE *jeb_file_handle;
    JEB_FILE_SYSTEM *jeb_fs;
    // WT_EXTENSION_API *wtext;
    WT_FILE_HANDLE *file_handle;
    struct io_uring_sqe *sqe;
    int ret = 0;
    int f;

    printf("JEB::jeb_fs_open %s\n", name);

    (void)flags; /* ignored for now */
    (void)file_type; /* unused */

    *file_handlep = NULL;

    jeb_fs = (JEB_FILE_SYSTEM *)fs;
    jeb_file_handle = NULL;
    // wtext = jeb_fs->wtext;

    // TODO: inspect the flags to see if we should actually create the file, lol
    // blindly assume we'll just open the file ... cuz YOLO
    f |= O_CREAT;
    sqe = io_uring_get_sqe(&jeb_fs->ring);
    io_uring_prep_openat(sqe, NULL, name, f, 0x666);
    RING_EVENT_USER_DATA *ud = malloc(sizeof(RING_EVENT_USER_DATA));
    ud->event_type = EVENT_TYPE_NORMAL;

    // TODO: set a lock/flag into the user_data struct

    io_uring_sqe_set_data(sqe, ud);
    io_uring_submit(&jeb_fs->ring);

    // TODO: look to see if we've already opened the file (and have a handle to it)

    // file isn't open, so create a new handle
    if ((jeb_file_handle = calloc(1, sizeof(JEB_FILE_HANDLE))) == NULL) {
        ret = ENOMEM;
        // goto err?????
    }

    jeb_file_handle->fs = jeb_fs;

    file_handle = (WT_FILE_HANDLE *)jeb_file_handle;
    file_handle->file_system = fs;
    if ((file_handle->name = strdup(name)) == NULL) {
        ret = ENOMEM;
        // goto err;
    }

    file_handle->close = jeb_fh_close;
    file_handle->fh_advise = NULL;
    file_handle->fh_extend = NULL;
    file_handle->fh_extend_nolock = NULL;
    file_handle->fh_lock = jeb_fh_lock;
    file_handle->fh_map = jeb_fh_map;
    file_handle->fh_map_discard = jeb_fh_map_discard;
    file_handle->fh_map_preload = jeb_fh_map_preload;
    file_handle->fh_read = jeb_fh_read;
    file_handle->fh_size = jeb_fh_size;
    file_handle->fh_sync = jeb_fh_sync;
    file_handle->fh_sync_nowait = jeb_fh_sync_nowait;
    file_handle->fh_truncate = jeb_fh_truncate;
    file_handle->fh_unmap = jeb_fh_unmap;
    file_handle->fh_write = jeb_fh_write;

    *file_handlep = file_handle;

    // NOW, block on the file create (via the earlier uring submit)


    return (ret);
}

/* return if file exists */
static int 
jeb_fs_exist(WT_FILE_SYSTEM *fs, WT_SESSION *session, const char *name, bool *existp) {
    printf("JEB::jeb_fs_exist %s\n", name);
    return (ENOTSUP);
}

/* POSIX remove */
static int 
jeb_fs_remove(WT_FILE_SYSTEM *fs, WT_SESSION *session, const char *name, uint32_t flags) {
    printf("JEB::jeb_fs_remove %s\n", name);
    return (ENOTSUP);
}

static int 
jeb_fs_rename(WT_FILE_SYSTEM *fs , WT_SESSION *session, const char *from, const char *to, uint32_t flags) {
    printf("JEB::jeb_fs_rename\n");
    return (ENOTSUP);
}

/* get the size of file in bytes */
static int 
jeb_fs_size(WT_FILE_SYSTEM *fs, WT_SESSION *session, const char *name, wt_off_t *sizep) {
    printf("JEB::jeb_fs_size %s\n", name);
    return (ENOTSUP);
}

/* return a list of files in a given sub-directory */
static int 
jeb_fs_directory_list(WT_FILE_SYSTEM *fs, WT_SESSION *session, const char *directory, 
    const char *prefix, char ***dirlistp, uint32_t *countp) {
    printf("JEB::jeb_fs_directory_list %s\n", directory);
    return (ENOTSUP);
}

/* free memory allocated by jeb_fs_directory_list */
static int 
jeb_fs_directory_list_free(WT_FILE_SYSTEM *fs, WT_SESSION *session, char **dirlist, uint32_t count) {
    printf("JEB::jeb_fs_directory_list_free\n");
    return (ENOTSUP);
}

/* discard any resources on termination */
static int jeb_fs_terminate(WT_FILE_SYSTEM *fs, WT_SESSION *session) {
    printf("JEB::jeb_fs_terminate\n");
    return (ENOTSUP);
}
/* ! [JEB :: FILE_SYSTEM] */

/* ! [JEB :: FILE HANDLE] */
static int 
jeb_fh_close(WT_FILE_HANDLE *file_handle, WT_SESSION *session) {
    printf("JEB::jeb_fh_close\n");
    return (ENOTSUP);
}

/* lock/unlock a file */
static int 
jeb_fh_lock(WT_FILE_HANDLE *file_handle, WT_SESSION *session, bool lock) {
    printf("JEB::jeb_fh_lock\n");
    return (ENOTSUP);
}

/* POSIX read :( */
static int 
jeb_fh_read(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset, 
    size_t len, void *buf) {
    printf("JEB::jeb_fh_read\n");
    return (ENOTSUP);
}


static int 
jeb_fh_size(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t *sizep) {
    printf("JEB::jeb_fh_size\n");
    return (ENOTSUP);
}

/* ensure file content is stable */
static int 
jeb_fh_sync(WT_FILE_HANDLE *file_handle, WT_SESSION *session) {
    printf("JEB::jeb_fh_sync\n");
    return (ENOTSUP);
}

/* ensure file content is stable */
static int 
jeb_fh_sync_nowait(WT_FILE_HANDLE *file_handle, WT_SESSION *session) {
    printf("JEB::jeb_fh_sync_nowait\n");
    return (ENOTSUP);
}

/* POSIX truncate */
static int 
jeb_fh_truncate(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset) {
    printf("JEB::jeb_fh_truncate\n");
    return (ENOTSUP);
}

/* POSIX write */
static int 
jeb_fh_write(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset, 
    size_t len, const void *buf) {
    printf("JEB::jeb_fh_write\n");
    return (ENOTSUP);
}

/* Map a file into memory */
static int 
jeb_fh_map(WT_FILE_HANDLE *file_handle, WT_SESSION *session, void *mapped_region, size_t *length, void *mapped_cookie) {
    printf("JEB::jeb_fh_map\n");
    return (ENOTSUP);
}

/* Unmap part of a memory mapped file */
static int 
jeb_fh_map_discard(WT_FILE_HANDLE *file_handle, WT_SESSION *session, void *mapped_region, size_t length, void *mapped_cookie) {
    printf("JEB::jeb_fh_map_discard\n");
    return (ENOTSUP);
}

/* Preload part of a memory mapped file */
static int 
jeb_fh_map_preload(WT_FILE_HANDLE *file_handle, WT_SESSION *session, const void *mapped_region, size_t length, void *mapped_cookie) {
    printf("JEB::jeb_fh_map_preload\n");
    return (ENOTSUP);
}

/* Unmap a memory mapped file */
static int 
jeb_fh_unmap(WT_FILE_HANDLE *file_handle, WT_SESSION *session, void *mapped_region, size_t length, void *mapped_cookie) {
    printf("JEB::jeb_fh_unmap\n");
    return (ENOTSUP);
}

/* ! [JEB :: FILE HANDLE] */



int
main(int args, char *argv[]) {
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_CURSOR *cursor;
    // CUSTOM_EVENT_HANDLER event_handler;
    int ret;
    home = "/tmp/wt_hacking";
    config = "create,session_max=10000,statistics=(all),statistics_log=(wait=1),log=(file_max=1MB,enabled=true,compressor=zstd,path=journal)," \
    "extensions=[local={entry=create_custom_file_system,early_load=true},/usr/local/lib/libwiredtiger_lz4.so,/usr/local/lib/libwiredtiger_zstd.so]," \
    "error_prefix=ERROR_JEB,verbose=[recovery_progress,checkpoint_progress,compact_progress,recovery]";

    // event_handler.h.handle_error = handle_wiredtiger_error;
    // event_handler.h.handle_message = handle_wiredtiger_message;
    // event_handler.app_id = "jasobrown_wt_hacking";

    fprintf(stderr, "about to open conn\n");
    // NOTE: hit some blocking behavior when using a custom event handler, so punting for now
    // if((ret = wiredtiger_open(home, (WT_EVENT_HANDLER *)&event_handler, config, &conn)) != 0) {
    if((ret = wiredtiger_open(home, NULL, config, &conn)) != 0) {
        fprintf(stderr, "failed to open dir: %s\n", wiredtiger_strerror(ret));
        return -1;
    }
    fprintf(stderr, "about to open session\n");
    if((ret = conn->open_session(conn, NULL, NULL, &session)) != 0) {
        fprintf(stderr, "failed to open session: %s\n", wiredtiger_strerror(ret));
        return -1;
    }

    fprintf(stderr, "about create table\n");
    if ((ret = session->create(session, "table:jeb1", "key_format=S,value_format=S")) != 0) {
        fprintf(stderr, "failed to create table: %s\n", wiredtiger_strerror(ret));
        return -1;
    }

    if ((ret = session->open_cursor(session, "table:jeb1", NULL, NULL, &cursor)) != 0) {
        fprintf(stderr, "failed to open cursor: %s\n", wiredtiger_strerror(ret));
        return -1;
    }

    // FINALLY, insert some data :)
    cursor->set_key(cursor, "key1");
    cursor->set_value(cursor, "val1");
    if ((ret = cursor->insert(cursor)) != 0) {
        fprintf(stderr, "failed to write data: %s\n", wiredtiger_strerror(ret));
        return -1;
    }

    const char *key, *value;
    cursor->reset(cursor);
    while((ret = cursor->next(cursor)) == 0) {
        cursor->get_key(cursor, &key);
        cursor->get_value(cursor, &value);
        printf("next record: %s : %s\n", key, value);
    }
    //scan_end_check(ret == WT_NOTFOUND);

    // the sleep is mostly to let the stats pile up in the statslog
    // fprintf(stderr, "about to sleep\n");
    // sleep(10);

    fprintf(stderr, "about to close session\n");
    if((ret = conn->close(conn, NULL)) != 0) {
        fprintf(stderr, "failed to close connection: %s\n", wiredtiger_strerror(ret));
        return -1;
    }
    return 0;
}
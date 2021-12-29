#include <stdlib.h>
#include <unistd.h>
#include "wiredtiger.h"
#include "liburing.h"

static const char *home;
static const char *config;


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



/* ! [JEB :: FILE_SYSTEM] */
typedef struct __jeb_file_handle {
    WT_FILE_SYSTEM iface;

    // one ring to rule them all ...
    struct io_uring ring;

} JEB_FILE_SYSTEM;


typedef struct jeb_file_handle {
    WT_FILE_HANDLE iface;

    /* pointer to the enclosing file system */
    JEB_FILE_SYSTEM *fs;


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
static int jeb_fh_truncate(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t);
static int jeb_fh_write(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, size_t, const void *);




/* ! [JEB :: FILE_SYSTEM] */



int
main(int args, char *argv[]) {
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_CURSOR *cursor;
    CUSTOM_EVENT_HANDLER event_handler;
    int ret;
    home = "/tmp/wt_hacking";
    config = "create,session_max=10000,statistics=(all),statistics_log=(wait=1),log=(file_max=1MB,enabled=true,compressor=zstd,path=journal)," \
    "extensions=[/usr/local/lib/libwiredtiger_lz4.so,/usr/local/lib/libwiredtiger_zstd.so],error_prefix=ERROR_JEB," \
    "verbose=[recovery_progress,checkpoint_progress,compact_progress,recovery]";

    event_handler.h.handle_error = handle_wiredtiger_error;
    event_handler.h.handle_message = handle_wiredtiger_message;
    event_handler.app_id = "jasobrown_wt_hacking";

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

    if ((ret = session->create(session, "table:jeb_lsm", "key_format=S,value_format=S, type=lsm")) != 0) {
        fprintf(stderr, "failed to create table: %s\n", wiredtiger_strerror(ret));
        return -1;
    }
    WT_CURSOR *lsm_cursor;
    if ((ret = session->open_cursor(session, "table:jeb_lsm", NULL, NULL, &lsm_cursor)) != 0) {
        fprintf(stderr, "failed to open cursor: %s\n", wiredtiger_strerror(ret));
        return -1;
    }

    // FINALLY, insert some data :)
    lsm_cursor->set_key(lsm_cursor, "key1");
    lsm_cursor->set_value(lsm_cursor, "val1");
    if ((ret = lsm_cursor->insert(lsm_cursor)) != 0) {
        fprintf(stderr, "failed to write data: %s\n", wiredtiger_strerror(ret));
        return -1;
    }


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
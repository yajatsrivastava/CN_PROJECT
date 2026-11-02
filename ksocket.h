#ifndef KSOCKET_H
#define KSOCKET_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <signal.h>

/* Socket type for KTP */
#define SOCK_KTP 505

/* Protocol timing and error rate parameters */
#define T 5               /* Retransmission timeout in seconds */
#define P_DROP 0.05f      /* Default drop probability */

/* Buffer and window constraints specified in assignment */
#define MSG_SIZE 512          /* Fixed payload size in bytes */
#define RECV_BUFFER_SIZE 10   /* Receiver message buffer size (10 messages) */
#define SEND_BUFFER_SIZE 20   /* Sender message buffer size */
#define SWND_MAX_SIZE 10      /* Maximum sending window size */
#define RWND_MAX_SIZE 10      /* Maximum receiving window size */
#define MAX_SOCKETS 20        /* Maximum concurrent active KTP sockets (N) */

/* Shared memory key */
#define SHM_KEY_PATH "ksocket.h"
#define SHM_KEY_ID 'K'
#define DEFAULT_SHM_KEY 0x4B545031 /* Fallback key "KTP1" */

/* Error codes */
#define ENOSPACE 1001
#define ENOTBOUND 1002
#define ENOMESSAGE 1003

/* Global error variable declared for KTP socket library */
extern int k_error;

/* Packet Types */
#define KTP_TYPE_DATA 1
#define KTP_TYPE_ACK  2

/* Fixed-format KTP message structure (transferred over UDP) */
typedef struct {
    uint8_t type;              /* KTP_TYPE_DATA or KTP_TYPE_ACK */
    uint8_t seq_num;           /* 8-bit sequence number (1..255) */
    uint8_t ack_num;           /* Last in-order sequence number acknowledged */
    uint8_t rwnd;              /* Advertised available receiver buffer space (0..10) */
    uint16_t length;           /* Actual payload size in bytes (1..MSG_SIZE) */
    char payload[MSG_SIZE];    /* 512-byte payload chunk */
} __attribute__((packed)) ktp_packet_t;

/* Sender-side message buffer entry */
typedef struct {
    ktp_packet_t packet;
    time_t last_sent_time;     /* Timestamp of last transmission */
    int is_occupied;           /* 1 if slot holds a valid message */
    int is_sent;               /* 1 if transmitted at least once */
    int is_acked;              /* 1 if acknowledged */
} send_msg_t;

/* Receiver-side message buffer entry */
typedef struct {
    char data[MSG_SIZE];
    uint16_t length;           /* Actual payload size */
    uint8_t seq_num;           /* 8-bit sequence number */
    int is_valid;              /* 1 if slot holds a message */
    int is_in_order;           /* 1 if message is in-order and ready for k_recvfrom */
} recv_msg_t;

/* Sender Window (swnd) Structure */
typedef struct {
    uint8_t window_size;       /* Current swnd size (bounded by receiver's rwnd) */
    uint8_t start_seq;         /* First unacknowledged sequence number */
    uint8_t next_seq;          /* Next sequence number to assign */
    uint8_t unacked_seqs[SWND_MAX_SIZE]; /* Sequence numbers sent but unACKed */
    int num_unacked;           /* Count of unacknowledged packets currently in flight */
} swnd_t;

/* Receiver Window (rwnd) Structure */
typedef struct {
    uint8_t window_size;       /* Current free space in receive buffer (0..10) */
    uint8_t expected_seq;      /* Next in-order sequence number expected */
    uint8_t last_in_order_seq; /* Last in-order sequence number acknowledged */
    uint8_t out_of_order_seqs[RECV_BUFFER_SIZE]; /* Buffered out-of-order sequence numbers */
    int num_out_of_order;      /* Count of out-of-order packets buffered */
} rwnd_t;

/* KTP Socket Table Entry (SM[i]) */
typedef struct {
    int is_allocated;          /* 1 if socket is in use, 0 if free */
    pid_t pid;                 /* Process ID of the process that created the socket */
    int udp_sock;              /* Underlying UDP socket file descriptor */
    int is_bound;              /* 1 if socket has been bound */
    struct sockaddr_in src_addr;  /* Bound source address and port */
    struct sockaddr_in dest_addr; /* Bound destination address and port */

    /* Buffers */
    send_msg_t send_buf[SEND_BUFFER_SIZE];
    recv_msg_t recv_buf[RECV_BUFFER_SIZE];

    /* Flow Control Windows */
    swnd_t swnd;
    rwnd_t rwnd;

    /* Deadlock prevention & flow control flags */
    int nospace;                   /* 1 if receive buffer became full (rwnd == 0) */
    int nospace_update_pending;    /* 1 if space opened up and duplicate ACK needs sending */
    time_t last_ack_sent_time;     /* Timestamp of last ACK sent */
    time_t last_probe_time;        /* Timestamp of last zero-window probe sent */

    /* Transmission statistics */
    unsigned long total_transmissions; /* Total UDP transmissions (initial + retransmits) */
    unsigned long total_data_msgs;     /* Unique messages submitted by application */
} ktp_socket_t;

/* Global Shared Memory (SM) Segment */
typedef struct {
    pthread_mutex_t lock;      /* Process-shared mutex for thread/process safety */
    ktp_socket_t sockets[MAX_SOCKETS];
    int init_done;             /* Set to 1 when initksocket is running */
    float drop_prob;           /* Configured drop probability p */
} ktp_sm_t;

/* Sequence number helper functions (1..255 wrapping back to 1) */
static inline uint8_t ktp_next_seq(uint8_t seq) {
    return (seq == 255) ? 1 : (seq + 1);
}

static inline uint8_t ktp_prev_seq(uint8_t seq) {
    return (seq <= 1) ? 255 : (seq - 1);
}

static inline int ktp_seq_diff(uint8_t a, uint8_t b) {
    int diff = (int)a - (int)b;
    if (diff < 0) diff += 255;
    return diff;
}

/* Library Function Declarations */
int k_socket(int domain, int type, int protocol);
int k_bind(int sockfd, const struct sockaddr *src_addr, socklen_t src_len,
           const struct sockaddr *dest_addr, socklen_t dest_len);
int k_sendto(int sockfd, const void *buf, size_t len, int flags,
             const struct sockaddr *dest_addr, socklen_t addrlen);
int k_recvfrom(int sockfd, void *buf, size_t len, int flags,
               struct sockaddr *src_addr, socklen_t *addrlen);
int k_close(int sockfd);

/* Unreliable channel simulation function */
int dropMessage(float p);

/* Helper convenience functions */
int k_send(int sockfd, const void *buf, size_t len, int flags);
int k_recv(int sockfd, void *buf, size_t len, int flags);
int k_bind_ip(int sockfd, const char *src_ip, int src_port,
              const char *dest_ip, int dest_port);

#endif /* KSOCKET_H */


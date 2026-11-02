#include "ksocket.h"

/* Definition of global error variable */
int k_error = 0;

/* Static shared memory pointer */
static ktp_sm_t *g_sm = NULL;

/* Helper to get shared memory segment */
static ktp_sm_t *get_shared_memory(void) {
    if (g_sm != NULL) {
        return g_sm;
    }

    key_t key = ftok(SHM_KEY_PATH, SHM_KEY_ID);
    if (key == -1) {
        key = (key_t)DEFAULT_SHM_KEY;
    }

    int shmid = shmget(key, 0, 0666);
    if (shmid < 0) {
        /* If shared memory does not exist, initksocket has not been run yet */
        return NULL;
    }

    void *addr = shmat(shmid, NULL, 0);
    if (addr == (void *)-1) {
        return NULL;
    }

    g_sm = (ktp_sm_t *)addr;
    return g_sm;
}

/* Helper to lock SM with robust mutex handling */
static void sm_lock(ktp_sm_t *sm) {
    if (!sm) return;
    int rc = pthread_mutex_lock(&sm->lock);
    if (rc == EOWNERDEAD) {
        /* Previous owner died while holding the lock. Make state consistent. */
        pthread_mutex_consistent(&sm->lock);
    }
}

/* Helper to unlock SM */
static void sm_unlock(ktp_sm_t *sm) {
    if (!sm) return;
    pthread_mutex_unlock(&sm->lock);
}

/*
 * dropMessage:
 * Generates a random number in [0, 1].
 * Returns 1 if random number < p (message dropped), else 0.
 */
int dropMessage(float p_val) {
    if (p_val <= 0.0f) return 0;
    if (p_val >= 1.0f) return 1;
    float r = (float)rand() / (float)RAND_MAX;
    return (r < p_val) ? 1 : 0;
}

/*
 * k_socket:
 * Allocates a KTP socket entry in SM and initializes state variables,
 * buffers, and flow control windows. The underlying UDP socket is created
 * and bound in the initksocket daemon context when k_bind is called.
 */
int k_socket(int domain, int type, int protocol) {
    (void)domain;
    (void)protocol;

    if (type != SOCK_KTP) {
        errno = EINVAL;
        return -1;
    }

    ktp_sm_t *sm = get_shared_memory();
    if (!sm || !sm->init_done) {
        fprintf(stderr, "[KTP] Error: initksocket daemon is not running. Please start initksocket first.\n");
        k_error = ENOSPACE;
        return -1;
    }

    sm_lock(sm);

    int slot = -1;
    for (int i = 0; i < MAX_SOCKETS; i++) {
        if (!sm->sockets[i].is_allocated) {
            slot = i;
            break;
        }
    }

    if (slot == -1) {
        sm_unlock(sm);
        k_error = ENOSPACE;
        return -1;
    }

    /* Initialize the socket entry in Shared Memory */
    ktp_socket_t *sock = &sm->sockets[slot];
    memset(sock, 0, sizeof(ktp_socket_t));

    sock->is_allocated = 1;
    sock->pid = getpid();
    sock->udp_sock = -1;
    sock->is_bound = 0;
    sock->bind_requested = 0;
    sock->close_requested = 0;

    /* Initialize Sending Window (swnd) */
    sock->swnd.window_size = SWND_MAX_SIZE; /* Initially 10 */
    sock->swnd.start_seq = 1;
    sock->swnd.next_seq = 1;
    sock->swnd.num_unacked = 0;

    /* Initialize Receiving Window (rwnd) */
    sock->rwnd.window_size = RECV_BUFFER_SIZE; /* Initially 10 */
    sock->rwnd.expected_seq = 1;
    sock->rwnd.last_in_order_seq = 0;
    sock->rwnd.app_read_seq = 1;
    sock->rwnd.num_out_of_order = 0;

    sock->nospace = 0;
    sock->nospace_update_pending = 0;
    sock->total_transmissions = 0;
    sock->total_data_msgs = 0;

    sm_unlock(sm);
    return slot;
}

/*
 * k_bind:
 * Configures the source and destination addresses in SM and requests the
 * initksocket daemon to bind the UDP socket.
 * Supports both standard signatures:
 *   k_bind(int sockfd, const struct sockaddr *src, socklen_t src_len,
 *          const struct sockaddr *dest, socklen_t dest_len)
 * and assignment specification signature:
 *   k_bind(int sockfd, const char *src_ip, int src_port,
 *          const char *dest_ip, int dest_port)
 */
int k_bind(int sockfd, const void *src_arg, socklen_t src_param2,
           const void *dest_arg, socklen_t dest_param4) {
    if (sockfd < 0 || sockfd >= MAX_SOCKETS || !src_arg || !dest_arg) {
        errno = EBADF;
        return -1;
    }

    ktp_sm_t *sm = get_shared_memory();
    if (!sm) {
        k_error = ENOSPACE;
        return -1;
    }

    sm_lock(sm);
    ktp_socket_t *sock = &sm->sockets[sockfd];
    if (!sock->is_allocated) {
        sm_unlock(sm);
        errno = EBADF;
        return -1;
    }

    /* Detect whether src_arg is a struct sockaddr_in or string IP */
    const unsigned char *b = (const unsigned char *)src_arg;
    if (b[0] == AF_INET || *(const short *)src_arg == AF_INET) {
        /* Called with struct sockaddr_in pointers */
        memcpy(&sock->src_addr, src_arg, sizeof(struct sockaddr_in));
        memcpy(&sock->dest_addr, dest_arg, sizeof(struct sockaddr_in));
    } else {
        /* Called with IP strings and integer ports */
        const char *src_ip = (const char *)src_arg;
        int src_port = (int)src_param2;
        const char *dest_ip = (const char *)dest_arg;
        int dest_port = (int)dest_param4;

        memset(&sock->src_addr, 0, sizeof(sock->src_addr));
        sock->src_addr.sin_family = AF_INET;
        sock->src_addr.sin_port = htons((uint16_t)src_port);
        inet_pton(AF_INET, src_ip, &sock->src_addr.sin_addr);

        memset(&sock->dest_addr, 0, sizeof(sock->dest_addr));
        sock->dest_addr.sin_family = AF_INET;
        sock->dest_addr.sin_port = htons((uint16_t)dest_port);
        inet_pton(AF_INET, dest_ip, &sock->dest_addr.sin_addr);
    }

    sock->bind_requested = 1;
    sm_unlock(sm);

    /* Wait for the initksocket daemon to create & bind the UDP socket */
    int timeout_ms = 5000;
    while (timeout_ms > 0) {
        sm_lock(sm);
        if (sock->is_bound) {
            sm_unlock(sm);
            return 0;
        }
        if (!sock->bind_requested && !sock->is_bound) {
            /* Daemon reported bind failure */
            sm_unlock(sm);
            errno = EADDRINUSE;
            return -1;
        }
        sm_unlock(sm);
        usleep(5000); /* 5 ms */
        timeout_ms -= 5;
    }

    errno = ETIMEDOUT;
    return -1;
}

/*
 * k_sendto:
 * Writes application message to the sender-side buffer if the destination matches
 * the bound IP/port and space is available.
 */
int k_sendto(int sockfd, const void *buf, size_t len, int flags,
             const struct sockaddr *dest_addr, socklen_t addrlen) {
    (void)flags;
    (void)addrlen;

    if (sockfd < 0 || sockfd >= MAX_SOCKETS) {
        errno = EBADF;
        return -1;
    }

    ktp_sm_t *sm = get_shared_memory();
    if (!sm) {
        k_error = ENOSPACE;
        return -1;
    }

    sm_lock(sm);
    ktp_socket_t *sock = &sm->sockets[sockfd];
    if (!sock->is_allocated || !sock->is_bound) {
        sm_unlock(sm);
        k_error = ENOTBOUND;
        return -1;
    }

    /* Check destination IP and port match bound destination */
    if (dest_addr != NULL) {
        const struct sockaddr_in *target = (const struct sockaddr_in *)dest_addr;
        if (target->sin_addr.s_addr != sock->dest_addr.sin_addr.s_addr ||
            target->sin_port != sock->dest_addr.sin_port) {
            sm_unlock(sm);
            k_error = ENOTBOUND;
            return -1;
        }
    }

    /* Find an empty slot in send_buf */
    int free_slot = -1;
    for (int i = 0; i < SEND_BUFFER_SIZE; i++) {
        if (!sock->send_buf[i].is_occupied) {
            free_slot = i;
            break;
        }
    }

    if (free_slot == -1) {
        sm_unlock(sm);
        k_error = ENOSPACE;
        return -1;
    }

    /* Populate the send buffer entry */
    send_msg_t *msg = &sock->send_buf[free_slot];
    size_t copy_bytes = (len > MSG_SIZE) ? MSG_SIZE : len;

    memset(msg, 0, sizeof(send_msg_t));
    msg->packet.type = KTP_TYPE_DATA;
    msg->packet.seq_num = sock->swnd.next_seq;
    msg->packet.ack_num = 0;
    msg->packet.rwnd = 0;
    msg->packet.length = (uint16_t)copy_bytes;
    if (buf && copy_bytes > 0) {
        memcpy(msg->packet.payload, buf, copy_bytes);
    }

    msg->is_occupied = 1;
    msg->is_sent = 0;
    msg->is_acked = 0;
    msg->last_sent_time = 0;

    /* Advance sender next_seq (1..255 wrapping back to 1) */
    sock->swnd.next_seq = ktp_next_seq(sock->swnd.next_seq);
    sock->total_data_msgs++;

    sm_unlock(sm);
    return (int)copy_bytes;
}

/*
 * k_recvfrom:
 * Retrieves the oldest in-order message from the receiver buffer and frees the slot.
 * Returns -1 with k_error = ENOMESSAGE if no in-order message is ready.
 */
int k_recvfrom(int sockfd, void *buf, size_t len, int flags,
               struct sockaddr *src_addr, socklen_t *addrlen) {
    (void)flags;

    if (sockfd < 0 || sockfd >= MAX_SOCKETS || !buf) {
        errno = EBADF;
        return -1;
    }

    ktp_sm_t *sm = get_shared_memory();
    if (!sm) {
        k_error = ENOMESSAGE;
        return -1;
    }

    sm_lock(sm);
    ktp_socket_t *sock = &sm->sockets[sockfd];
    if (!sock->is_allocated) {
        sm_unlock(sm);
        k_error = ENOMESSAGE;
        return -1;
    }

    /* Look for the next strictly sequential in-order message (matching app_read_seq) */
    int read_slot = -1;
    for (int i = 0; i < RECV_BUFFER_SIZE; i++) {
        if (sock->recv_buf[i].is_valid && sock->recv_buf[i].is_in_order &&
            sock->recv_buf[i].seq_num == sock->rwnd.app_read_seq) {
            read_slot = i;
            break;
        }
    }

    if (read_slot == -1) {
        sm_unlock(sm);
        k_error = ENOMESSAGE;
        return -1;
    }

    recv_msg_t *msg = &sock->recv_buf[read_slot];
    size_t copy_bytes = (len < msg->length) ? len : msg->length;
    if (copy_bytes > 0) {
        memcpy(buf, msg->data, copy_bytes);
    }

    if (src_addr && addrlen && *addrlen >= sizeof(struct sockaddr_in)) {
        memcpy(src_addr, &sock->dest_addr, sizeof(struct sockaddr_in));
        *addrlen = sizeof(struct sockaddr_in);
    }

    /* Advance application sequence counter */
    sock->rwnd.app_read_seq = ktp_next_seq(sock->rwnd.app_read_seq);

    /* Clear the slot */
    memset(msg, 0, sizeof(recv_msg_t));

    /* Recompute rwnd size (available space in receive buffer) */
    int occupied_count = 0;
    for (int i = 0; i < RECV_BUFFER_SIZE; i++) {
        if (sock->recv_buf[i].is_valid) {
            occupied_count++;
        }
    }
    sock->rwnd.window_size = (uint8_t)(RECV_BUFFER_SIZE - occupied_count);

    /*
     * Zero-Window Deadlock resolution:
     * If nospace was previously flagged (receiver advertised rwnd = 0),
     * and space has now opened up, mark nospace_update_pending = 1
     * so Thread R can send a duplicate ACK with updated rwnd on its next tick.
     */
    if (sock->nospace && sock->rwnd.window_size > 0) {
        sock->nospace_update_pending = 1;
    }

    sm_unlock(sm);
    return (int)copy_bytes;
}

/*
 * k_close:
 * Signals the initksocket daemon to close the underlying UDP socket
 * and clears the SM entry.
 */
int k_close(int sockfd) {
    if (sockfd < 0 || sockfd >= MAX_SOCKETS) {
        errno = EBADF;
        return -1;
    }

    ktp_sm_t *sm = get_shared_memory();
    if (!sm) {
        return -1;
    }

    sm_lock(sm);
    ktp_socket_t *sock = &sm->sockets[sockfd];
    if (!sock->is_allocated) {
        sm_unlock(sm);
        return 0;
    }

    sock->close_requested = 1;
    sm_unlock(sm);

    /* Wait briefly for initksocket daemon to close the socket and clean up */
    int wait_ms = 1000;
    while (wait_ms > 0) {
        sm_lock(sm);
        if (!sock->is_allocated) {
            sm_unlock(sm);
            return 0;
        }
        sm_unlock(sm);
        usleep(10000); /* 10 ms */
        wait_ms -= 10;
    }

    /* Direct cleanup fallback if daemon is slow */
    sm_lock(sm);
    sock->is_allocated = 0;
    sm_unlock(sm);
    return 0;
}

/*
 * Helper convenience functions
 */
int k_send(int sockfd, const void *buf, size_t len, int flags) {
    return k_sendto(sockfd, buf, len, flags, NULL, 0);
}

int k_recv(int sockfd, void *buf, size_t len, int flags) {
    return k_recvfrom(sockfd, buf, len, flags, NULL, NULL);
}

int k_bind_ip(int sockfd, const char *src_ip, int src_port,
              const char *dest_ip, int dest_port) {
    return k_bind(sockfd, src_ip, (socklen_t)src_port, dest_ip, (socklen_t)dest_port);
}

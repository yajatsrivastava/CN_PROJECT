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

    int shmid = shmget(key, sizeof(ktp_sm_t), 0666);
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
int dropMessage(float p) {
    if (p <= 0.0f) return 0;
    if (p >= 1.0f) return 1;
    float r = (float)rand() / (float)RAND_MAX;
    return (r < p) ? 1 : 0;
}

/*
 * k_socket:
 * Allocates a KTP socket entry in SM, creates the underlying UDP socket,
 * and initializes state variables, buffers, and flow control windows.
 */
int k_socket(int domain, int type, int protocol) {
    if (type != SOCK_KTP) {
        errno = EINVAL;
        return -1;
    }

    ktp_sm_t *sm = get_shared_memory();
    if (!sm || !sm->init_done) {
        fprintf(stderr, "[KTP] Error: initksocket is not running. Please start initksocket first.\n");
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

    /* Open the underlying UDP socket */
    int udp_fd = socket(domain, SOCK_DGRAM, protocol);
    if (udp_fd < 0) {
        sm_unlock(sm);
        return -1;
    }

    /* Initialize the socket entry in Shared Memory */
    ktp_socket_t *sock = &sm->sockets[slot];
    memset(sock, 0, sizeof(ktp_socket_t));

    sock->is_allocated = 1;
    sock->pid = getpid();
    sock->udp_sock = udp_fd;
    sock->is_bound = 0;

    /* Initialize Sending Window (swnd) */
    sock->swnd.window_size = SWND_MAX_SIZE; /* Initially 10 */
    sock->swnd.start_seq = 1;
    sock->swnd.next_seq = 1;
    sock->swnd.num_unacked = 0;

    /* Initialize Receiving Window (rwnd) */
    sock->rwnd.window_size = RECV_BUFFER_SIZE; /* Initially 10 */
    sock->rwnd.expected_seq = 1;
    sock->rwnd.last_in_order_seq = 0;
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
 * Binds the underlying UDP socket to local src_addr and registers
 * remote dest_addr in the SM entry.
 */
int k_bind(int sockfd, const struct sockaddr *src_addr, socklen_t src_len,
           const struct sockaddr *dest_addr, socklen_t dest_len) {
    if (sockfd < 0 || sockfd >= MAX_SOCKETS || !src_addr || !dest_addr) {
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

    /* Bind the underlying UDP socket */
    if (bind(sock->udp_sock, src_addr, src_len) < 0) {
        int err = errno;
        sm_unlock(sm);
        errno = err;
        return -1;
    }

    /* Save local and remote addresses */
    memcpy(&sock->src_addr, src_addr, sizeof(struct sockaddr_in));
    memcpy(&sock->dest_addr, dest_addr, sizeof(struct sockaddr_in));
    sock->is_bound = 1;

    sm_unlock(sm);
    return 0;
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

    if (sockfd < 0 || sockfd >= MAX_SOCKETS || !buf) {
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
    memcpy(msg->packet.payload, buf, copy_bytes);

    msg->is_occupied = 1;
    msg->is_sent = 0;
    msg->is_acked = 0;
    msg->last_sent_time = 0;

    /* Advance sender next_seq */
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

    /* Look for an in-order message ready for the application */
    int read_slot = -1;
    for (int i = 0; i < RECV_BUFFER_SIZE; i++) {
        if (sock->recv_buf[i].is_valid && sock->recv_buf[i].is_in_order) {
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
    memcpy(buf, msg->data, copy_bytes);

    if (src_addr && addrlen && *addrlen >= sizeof(struct sockaddr_in)) {
        memcpy(src_addr, &sock->dest_addr, sizeof(struct sockaddr_in));
        *addrlen = sizeof(struct sockaddr_in);
    }

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
 * Closes the underlying UDP socket and cleans up the SM entry.
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
    if (sock->is_allocated) {
        if (sock->udp_sock >= 0) {
            close(sock->udp_sock);
            sock->udp_sock = -1;
        }
        memset(sock, 0, sizeof(ktp_socket_t));
        sock->udp_sock = -1;
    }
    sm_unlock(sm);

    return 0;
}

/* Convenience helper: k_send using bound destination address */
int k_send(int sockfd, const void *buf, size_t len, int flags) {
    if (sockfd < 0 || sockfd >= MAX_SOCKETS) return -1;
    ktp_sm_t *sm = get_shared_memory();
    if (!sm) return -1;

    sm_lock(sm);
    struct sockaddr_in dest = sm->sockets[sockfd].dest_addr;
    sm_unlock(sm);

    return k_sendto(sockfd, buf, len, flags, (struct sockaddr *)&dest, sizeof(dest));
}

/* Convenience helper: k_recv without src_addr */
int k_recv(int sockfd, void *buf, size_t len, int flags) {
    return k_recvfrom(sockfd, buf, len, flags, NULL, NULL);
}

/* Convenience helper: k_bind with IP and port parameters directly */
int k_bind_ip(int sockfd, const char *src_ip, int src_port,
              const char *dest_ip, int dest_port) {
    struct sockaddr_in src, dest;
    memset(&src, 0, sizeof(src));
    src.sin_family = AF_INET;
    src.sin_port = htons((uint16_t)src_port);
    inet_pton(AF_INET, src_ip, &src.sin_addr);

    memset(&dest, 0, sizeof(dest));
    dest.sin_family = AF_INET;
    dest.sin_port = htons((uint16_t)dest_port);
    inet_pton(AF_INET, dest_ip, &dest.sin_addr);

    return k_bind(sockfd, (struct sockaddr *)&src, sizeof(src),
                  (struct sockaddr *)&dest, sizeof(dest));
}


#include "ksocket.h"

static ktp_sm_t *g_sm = NULL;
static int g_shmid = -1;
static pid_t g_gc_pid = -1;
static volatile sig_atomic_t g_running = 1;

/* Signal handler for graceful termination */
static void handle_signal(int sig) {
    (void)sig;
    g_running = 0;
}

/* Helper to lock SM with robust mutex recovery */
static void sm_lock(ktp_sm_t *sm) {
    if (!sm) return;
    int rc = pthread_mutex_lock(&sm->lock);
    if (rc == EOWNERDEAD) {
        pthread_mutex_consistent(&sm->lock);
    }
}

static void sm_unlock(ktp_sm_t *sm) {
    if (!sm) return;
    pthread_mutex_unlock(&sm->lock);
}

/*
 * Thread R (Receiver Thread):
 * 1. Checks for socket bind/close requests from application processes.
 * 2. Monitors all active UDP sockets using select().
 * 3. On select timeout:
 *    - Resolves Zero-Window Deadlock: if nospace was set and space is now available,
 *      sends duplicate ACK with updated rwnd.
 * 4. On incoming UDP packet:
 *    - Simulates network drops using dropMessage(p).
 *    - Processes incoming DATA messages, handles in-order & out-of-order buffering,
 *      drops duplicates, and sends cumulative ACKs.
 *    - Processes incoming ACK messages, slides sender window (swnd), and removes
 *      acknowledged messages from send_buf.
 */
static void *thread_r_func(void *arg) {
    (void)arg;
    printf("[Thread R] Receiver thread started.\n");

    while (g_running) {
        sm_lock(g_sm);

        /* 1. Process pending bind or close requests */
        for (int i = 0; i < MAX_SOCKETS; i++) {
            ktp_socket_t *sock = &g_sm->sockets[i];

            if (sock->is_allocated && sock->close_requested) {
                if (sock->udp_sock >= 0) {
                    close(sock->udp_sock);
                    sock->udp_sock = -1;
                }
                memset(sock, 0, sizeof(ktp_socket_t));
                sock->udp_sock = -1;
                printf("[initksocket] Socket %d closed.\n", i);
                continue;
            }

            if (sock->is_allocated && sock->bind_requested && !sock->is_bound) {
                int fd = socket(AF_INET, SOCK_DGRAM, 0);
                if (fd < 0) {
                    perror("[initksocket] UDP socket creation failed");
                    sock->bind_requested = 0;
                    continue;
                }

                int opt = 1;
                setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
                setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

                if (bind(fd, (struct sockaddr *)&sock->src_addr, sizeof(struct sockaddr_in)) < 0) {
                    perror("[initksocket] UDP bind failed");
                    close(fd);
                    sock->bind_requested = 0;
                    continue;
                }

                sock->udp_sock = fd;
                sock->is_bound = 1;
                sock->bind_requested = 0;
                printf("[initksocket] Socket %d bound to UDP fd %d (local port %d)\n",
                       i, fd, ntohs(sock->src_addr.sin_port));
            }
        }

        /* 2. Build fd_set for select() */
        fd_set read_fds;
        FD_ZERO(&read_fds);
        int max_fd = -1;

        for (int i = 0; i < MAX_SOCKETS; i++) {
            ktp_socket_t *sock = &g_sm->sockets[i];
            if (sock->is_allocated && sock->is_bound && sock->udp_sock >= 0) {
                int fd = sock->udp_sock;
                FD_SET(fd, &read_fds);
                if (fd > max_fd) {
                    max_fd = fd;
                }
            }
        }
        sm_unlock(g_sm);

        /* 3. Wait on select() */
        struct timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 50000; /* 50 ms */

        int activity = 0;
        if (max_fd >= 0) {
            activity = select(max_fd + 1, &read_fds, NULL, NULL, &tv);
        } else {
            usleep(50000);
            activity = 0;
        }

        if (activity < 0) {
            if (errno == EINTR) continue;
            perror("[Thread R] select error");
            continue;
        }

        /* 
         * 4. Zero-Window Deadlock resolution check:
         * If nospace_update_pending is set and rwnd > 0, retransmit duplicate ACK
         * until sender resumes transmission.
         */
        sm_lock(g_sm);
        time_t now = time(NULL);
        for (int i = 0; i < MAX_SOCKETS; i++) {
            ktp_socket_t *sock = &g_sm->sockets[i];
            if (sock->is_allocated && sock->is_bound && sock->udp_sock >= 0) {
                if (sock->nospace_update_pending && sock->rwnd.window_size > 0) {
                    if (now - sock->last_ack_sent_time >= 1) {
                        ktp_packet_t ack;
                        memset(&ack, 0, sizeof(ack));
                        ack.type = KTP_TYPE_ACK;
                        ack.ack_num = sock->rwnd.last_in_order_seq;
                        ack.rwnd = sock->rwnd.window_size;
                        ack.length = 0;

                        sendto(sock->udp_sock, &ack, sizeof(ktp_packet_t), 0,
                               (struct sockaddr *)&sock->dest_addr, sizeof(sock->dest_addr));
                        sock->last_ack_sent_time = now;
                    }
                }
            }
        }

        if (activity == 0) {
            sm_unlock(g_sm);
            continue;
        }

        /* 5. Process readable sockets */
        for (int i = 0; i < MAX_SOCKETS; i++) {
            ktp_socket_t *sock = &g_sm->sockets[i];
            if (!sock->is_allocated || !sock->is_bound || sock->udp_sock < 0) {
                continue;
            }

            if (FD_ISSET(sock->udp_sock, &read_fds)) {
                ktp_packet_t packet;
                struct sockaddr_in from_addr;
                socklen_t from_len = sizeof(from_addr);

                ssize_t bytes = recvfrom(sock->udp_sock, &packet, sizeof(ktp_packet_t), 0,
                                         (struct sockaddr *)&from_addr, &from_len);
                if (bytes <= 0) {
                    continue;
                }

                /* 
                 * Unreliable channel simulation:
                 * Call dropMessage(p). If 1, drop packet immediately.
                 */
                if (dropMessage(g_sm->drop_prob)) {
                    continue;
                }

                /* Handle received packet */
                if (packet.type == KTP_TYPE_DATA) {
                    /* Data packet received: clear zero-window pending update */
                    sock->nospace_update_pending = 0;

                    uint8_t seq = packet.seq_num;
                    uint8_t exp = sock->rwnd.expected_seq;
                    int diff = ktp_seq_diff(seq, exp);

                    if (diff == 0) {
                        /* IN-ORDER PACKET RECEIVED */
                        int slot = -1;
                        for (int k = 0; k < RECV_BUFFER_SIZE; k++) {
                            if (!sock->recv_buf[k].is_valid) {
                                slot = k;
                                break;
                            }
                        }

                        if (slot != -1) {
                            recv_msg_t *rmsg = &sock->recv_buf[slot];
                            rmsg->is_valid = 1;
                            rmsg->is_in_order = 1;
                            rmsg->seq_num = seq;
                            rmsg->length = packet.length;
                            if (packet.length > 0) {
                                memcpy(rmsg->data, packet.payload, packet.length);
                            }

                            /* Advance expected sequence number */
                            sock->rwnd.expected_seq = ktp_next_seq(sock->rwnd.expected_seq);

                            /* Cascading delivery: deliver consecutive buffered out-of-order packets */
                            int advanced = 1;
                            while (advanced) {
                                advanced = 0;
                                for (int k = 0; k < RECV_BUFFER_SIZE; k++) {
                                    if (sock->recv_buf[k].is_valid && !sock->recv_buf[k].is_in_order) {
                                        if (sock->recv_buf[k].seq_num == sock->rwnd.expected_seq) {
                                            sock->recv_buf[k].is_in_order = 1;
                                            sock->rwnd.expected_seq = ktp_next_seq(sock->rwnd.expected_seq);
                                            advanced = 1;
                                            break;
                                        }
                                    }
                                }
                            }

                            sock->rwnd.last_in_order_seq = ktp_prev_seq(sock->rwnd.expected_seq);

                            /* Recompute available space in receiver buffer */
                            int valid_count = 0;
                            for (int k = 0; k < RECV_BUFFER_SIZE; k++) {
                                if (sock->recv_buf[k].is_valid) valid_count++;
                            }
                            sock->rwnd.window_size = (uint8_t)(RECV_BUFFER_SIZE - valid_count);

                            if (sock->rwnd.window_size == 0) {
                                sock->nospace = 1;
                            } else {
                                sock->nospace = 0;
                            }

                            /* Send cumulative ACK */
                            ktp_packet_t ack;
                            memset(&ack, 0, sizeof(ack));
                            ack.type = KTP_TYPE_ACK;
                            ack.ack_num = sock->rwnd.last_in_order_seq;
                            ack.rwnd = sock->rwnd.window_size;
                            ack.length = 0;

                            sendto(sock->udp_sock, &ack, sizeof(ktp_packet_t), 0,
                                   (struct sockaddr *)&sock->dest_addr, sizeof(sock->dest_addr));
                            sock->last_ack_sent_time = time(NULL);
                        }
                    } else if (diff > 0 && diff < sock->rwnd.window_size) {
                        /* OUT-OF-ORDER PACKET WITHIN RWND */
                        int already_have = 0;
                        for (int k = 0; k < RECV_BUFFER_SIZE; k++) {
                            if (sock->recv_buf[k].is_valid && sock->recv_buf[k].seq_num == seq) {
                                already_have = 1;
                                break;
                            }
                        }

                        if (!already_have) {
                            for (int k = 0; k < RECV_BUFFER_SIZE; k++) {
                                if (!sock->recv_buf[k].is_valid) {
                                    sock->recv_buf[k].is_valid = 1;
                                    sock->recv_buf[k].is_in_order = 0;
                                    sock->recv_buf[k].seq_num = seq;
                                    sock->recv_buf[k].length = packet.length;
                                    if (packet.length > 0) {
                                        memcpy(sock->recv_buf[k].data, packet.payload, packet.length);
                                    }
                                    break;
                                }
                            }
                            /* Recompute available space */
                            int valid_count = 0;
                            for (int k = 0; k < RECV_BUFFER_SIZE; k++) {
                                if (sock->recv_buf[k].is_valid) valid_count++;
                            }
                            sock->rwnd.window_size = (uint8_t)(RECV_BUFFER_SIZE - valid_count);
                            if (sock->rwnd.window_size == 0) sock->nospace = 1;
                        }
                        /* Specification: do NOT send ACK for out-of-order packets */
                    } else if (diff >= 128) {
                        /* 
                         * DUPLICATE PACKET (seq < expected_seq):
                         * Drop payload and send duplicate ACK so sender can slide window.
                         */
                        ktp_packet_t dup_ack;
                        memset(&dup_ack, 0, sizeof(dup_ack));
                        dup_ack.type = KTP_TYPE_ACK;
                        dup_ack.ack_num = sock->rwnd.last_in_order_seq;
                        dup_ack.rwnd = sock->rwnd.window_size;
                        dup_ack.length = 0;

                        sendto(sock->udp_sock, &dup_ack, sizeof(ktp_packet_t), 0,
                               (struct sockaddr *)&sock->dest_addr, sizeof(sock->dest_addr));
                        sock->last_ack_sent_time = time(NULL);
                    }
                } else if (packet.type == KTP_TYPE_ACK) {
                    /* ACK MESSAGE RECEIVED */
                    uint8_t ack_seq = packet.ack_num;
                    uint8_t rwnd_val = packet.rwnd;

                    /* Update swnd size based on advertised rwnd */
                    sock->swnd.window_size = rwnd_val;

                    if (ack_seq > 0) {
                        /* Check if ack_seq advances swnd.start_seq */
                        int advance = ktp_seq_diff(ack_seq, sock->swnd.start_seq);
                        if (advance < 128) {
                            /* Free acknowledged messages from send_buf */
                            for (int k = 0; k < SEND_BUFFER_SIZE; k++) {
                                if (sock->send_buf[k].is_occupied) {
                                    uint8_t m_seq = sock->send_buf[k].packet.seq_num;
                                    if (ktp_seq_diff(ack_seq, m_seq) < 128 &&
                                        ktp_seq_diff(m_seq, sock->swnd.start_seq) < 128) {
                                        sock->send_buf[k].is_occupied = 0;
                                        sock->send_buf[k].is_sent = 0;
                                        sock->send_buf[k].is_acked = 1;
                                    }
                                }
                            }
                            sock->swnd.start_seq = ktp_next_seq(ack_seq);
                        }
                    }
                }
            }
        }
        sm_unlock(g_sm);
    }

    printf("[Thread R] Receiver thread exited.\n");
    return NULL;
}

/*
 * Thread S (Sender / Retransmission Thread):
 * 1. Checks if timeout period T has expired for unacknowledged messages in swnd;
 *    if yes, retransmits all messages in swnd.
 * 2. Checks zero-window probe condition if swnd == 0.
 * 3. Sends pending messages from send_buf that fit within current swnd.
 */
static void *thread_s_func(void *arg) {
    (void)arg;
    printf("[Thread S] Sender / Retransmission thread started.\n");

    while (g_running) {
        /* Sleep 50ms per iteration for low-latency transmission */
        usleep(50000);
        if (!g_running) break;

        sm_lock(g_sm);
        time_t now = time(NULL);

        for (int i = 0; i < MAX_SOCKETS; i++) {
            ktp_socket_t *sock = &g_sm->sockets[i];
            if (!sock->is_allocated || !sock->is_bound || sock->udp_sock < 0) {
                continue;
            }

            /* Step 1: Check for timeouts on unacknowledged messages in swnd */
            int timeout_occurred = 0;
            for (int k = 0; k < SEND_BUFFER_SIZE; k++) {
                if (sock->send_buf[k].is_occupied && sock->send_buf[k].is_sent && !sock->send_buf[k].is_acked) {
                    if (now - sock->send_buf[k].last_sent_time >= T) {
                        timeout_occurred = 1;
                        break;
                    }
                }
            }

            /* If timeout occurred, retransmit all unACKed messages in swnd */
            if (timeout_occurred) {
                for (int k = 0; k < SEND_BUFFER_SIZE; k++) {
                    if (sock->send_buf[k].is_occupied && sock->send_buf[k].is_sent && !sock->send_buf[k].is_acked) {
                        sendto(sock->udp_sock, &sock->send_buf[k].packet, sizeof(ktp_packet_t), 0,
                               (struct sockaddr *)&sock->dest_addr, sizeof(sock->dest_addr));
                        sock->send_buf[k].last_sent_time = now;
                        sock->total_transmissions++;
                    }
                }
            }

            /* Step 2: Zero-window probe check */
            if (sock->swnd.window_size == 0) {
                int has_pending = 0;
                for (int k = 0; k < SEND_BUFFER_SIZE; k++) {
                    if (sock->send_buf[k].is_occupied && !sock->send_buf[k].is_sent) {
                        has_pending = 1;
                        break;
                    }
                }
                if (has_pending && (now - sock->last_probe_time >= T)) {
                    ktp_packet_t probe;
                    memset(&probe, 0, sizeof(probe));
                    probe.type = KTP_TYPE_ACK;
                    probe.ack_num = 0;
                    probe.rwnd = 0;
                    probe.length = 0;

                    sendto(sock->udp_sock, &probe, sizeof(ktp_packet_t), 0,
                           (struct sockaddr *)&sock->dest_addr, sizeof(sock->dest_addr));
                    sock->last_probe_time = now;
                }
            }

            /* Step 3: Send pending messages from send_buf that fit within current swnd */
            int in_flight = 0;
            for (int k = 0; k < SEND_BUFFER_SIZE; k++) {
                if (sock->send_buf[k].is_occupied && sock->send_buf[k].is_sent && !sock->send_buf[k].is_acked) {
                    in_flight++;
                }
            }

            int effective_window = sock->swnd.window_size;
            if (effective_window > SWND_MAX_SIZE) effective_window = SWND_MAX_SIZE;

            while (in_flight < effective_window) {
                /* Find the next unsent message in sequence order (closest to swnd.start_seq) */
                int next_slot = -1;
                int min_dist = 999;

                for (int k = 0; k < SEND_BUFFER_SIZE; k++) {
                    if (sock->send_buf[k].is_occupied && !sock->send_buf[k].is_sent) {
                        uint8_t s = sock->send_buf[k].packet.seq_num;
                        int dist = ktp_seq_diff(s, sock->swnd.start_seq);
                        if (dist < min_dist) {
                            min_dist = dist;
                            next_slot = k;
                        }
                    }
                }

                if (next_slot == -1) break;

                /* Transmit packet over UDP */
                sendto(sock->udp_sock, &sock->send_buf[next_slot].packet, sizeof(ktp_packet_t), 0,
                       (struct sockaddr *)&sock->dest_addr, sizeof(sock->dest_addr));

                sock->send_buf[next_slot].is_sent = 1;
                sock->send_buf[next_slot].last_sent_time = now;
                sock->total_transmissions++;
                in_flight++;
            }
        }
        sm_unlock(g_sm);
    }

    printf("[Thread S] Sender / Retransmission thread exited.\n");
    return NULL;
}

/*
 * Garbage Collector Process:
 * Periodically checks all allocated sockets in SM.
 * If the creator process has died (kill(pid, 0) == -1 && errno == ESRCH),
 * cleans up the UDP socket and frees the SM entry.
 */
static void run_garbage_collector(void) {
    printf("[Garbage Collector] Process started (PID: %d).\n", getpid());

    while (g_running) {
        sleep(2);
        if (!g_sm) continue;

        sm_lock(g_sm);
        for (int i = 0; i < MAX_SOCKETS; i++) {
            if (g_sm->sockets[i].is_allocated) {
                pid_t owner = g_sm->sockets[i].pid;
                if (owner > 0 && kill(owner, 0) == -1 && errno == ESRCH) {
                    printf("[Garbage Collector] Owner PID %d died. Cleaning up KTP socket %d.\n", owner, i);
                    if (g_sm->sockets[i].udp_sock >= 0) {
                        close(g_sm->sockets[i].udp_sock);
                        g_sm->sockets[i].udp_sock = -1;
                    }
                    memset(&g_sm->sockets[i], 0, sizeof(ktp_socket_t));
                    g_sm->sockets[i].udp_sock = -1;
                }
            }
        }
        sm_unlock(g_sm);
    }

    printf("[Garbage Collector] Process exiting.\n");
}

int main(int argc, char *argv[]) {
    float drop_prob = P_DROP;
    if (argc > 1) {
        drop_prob = atof(argv[1]);
        if (drop_prob < 0.0f) drop_prob = 0.0f;
        if (drop_prob > 1.0f) drop_prob = 1.0f;
    }

    srand((unsigned int)time(NULL));

    /* Setup signal handlers */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_signal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    printf("====================================================\n");
    printf("        KTP Daemon Initialization (initksocket)     \n");
    printf("        Configured Drop Probability p = %.4f        \n", drop_prob);
    printf("        Retransmission Timeout T      = %d seconds   \n", T);
    printf("====================================================\n");

    /* 1. Allocate / Attach Shared Memory */
    key_t key = ftok(SHM_KEY_PATH, SHM_KEY_ID);
    if (key == -1) {
        key = (key_t)DEFAULT_SHM_KEY;
    }

    /* Remove any existing segment to guarantee fresh state */
    int old_shmid = shmget(key, 0, 0666);
    if (old_shmid >= 0) {
        shmctl(old_shmid, IPC_RMID, NULL);
    }

    g_shmid = shmget(key, sizeof(ktp_sm_t), IPC_CREAT | 0666);
    if (g_shmid < 0) {
        perror("Failed to create shared memory segment");
        exit(1);
    }

    g_sm = (ktp_sm_t *)shmat(g_shmid, NULL, 0);
    if (g_sm == (void *)-1) {
        perror("Failed to attach shared memory segment");
        shmctl(g_shmid, IPC_RMID, NULL);
        exit(1);
    }

    /* Initialize shared memory content */
    memset(g_sm, 0, sizeof(ktp_sm_t));
    for (int i = 0; i < MAX_SOCKETS; i++) {
        g_sm->sockets[i].udp_sock = -1;
    }

    /* Initialize process-shared robust mutex */
    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&mattr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&g_sm->lock, &mattr);
    pthread_mutexattr_destroy(&mattr);

    g_sm->drop_prob = drop_prob;
    g_sm->init_done = 1;

    printf("[initksocket] Shared memory initialized (shmid = %d, size = %zu bytes).\n",
           g_shmid, sizeof(ktp_sm_t));

    /* 2. Fork Garbage Collector process */
    g_gc_pid = fork();
    if (g_gc_pid < 0) {
        perror("Failed to fork garbage collector");
    } else if (g_gc_pid == 0) {
        run_garbage_collector();
        exit(0);
    }
    printf("[initksocket] Spawned Garbage Collector (PID: %d).\n", g_gc_pid);

    /* 3. Spawn Thread R and Thread S */
    pthread_t thread_r, thread_s;
    if (pthread_create(&thread_r, NULL, thread_r_func, NULL) != 0) {
        perror("Failed to create Thread R");
        exit(1);
    }

    if (pthread_create(&thread_s, NULL, thread_s_func, NULL) != 0) {
        perror("Failed to create Thread S");
        exit(1);
    }

    printf("[initksocket] KTP daemon running. Press Ctrl+C to terminate.\n");

    /* Wait for termination signal */
    while (g_running) {
        sleep(1);
    }

    printf("\n[initksocket] Shutting down...\n");

    /* Terminate threads */
    pthread_join(thread_r, NULL);
    pthread_join(thread_s, NULL);

    /* Terminate Garbage Collector */
    if (g_gc_pid > 0) {
        kill(g_gc_pid, SIGTERM);
        waitpid(g_gc_pid, NULL, 0);
    }

    /* Close any remaining open UDP sockets */
    sm_lock(g_sm);
    for (int i = 0; i < MAX_SOCKETS; i++) {
        if (g_sm->sockets[i].udp_sock >= 0) {
            close(g_sm->sockets[i].udp_sock);
            g_sm->sockets[i].udp_sock = -1;
        }
    }
    sm_unlock(g_sm);

    pthread_mutex_destroy(&g_sm->lock);
    shmdt(g_sm);
    shmctl(g_shmid, IPC_RMID, NULL);

    printf("[initksocket] Clean shutdown complete.\n");
    return 0;
}

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
 * 1. Monitors all active UDP sockets using select().
 * 2. On select timeout:
 *    - Refreshes fd_set for newly created sockets.
 *    - Resolves Zero-Window Deadlock: if nospace was set and space is now available,
 *      sends duplicate ACK with updated rwnd.
 * 3. On incoming UDP packet:
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
        fd_set read_fds;
        FD_ZERO(&read_fds);
        int max_fd = -1;

        sm_lock(g_sm);
        for (int i = 0; i < MAX_SOCKETS; i++) {
            if (g_sm->sockets[i].is_allocated && g_sm->sockets[i].is_bound && g_sm->sockets[i].udp_sock >= 0) {
                int fd = g_sm->sockets[i].udp_sock;
                FD_SET(fd, &read_fds);
                if (fd > max_fd) {
                    max_fd = fd;
                }
            }
        }
        sm_unlock(g_sm);

        /* select() with a 200ms timeout */
        struct timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 200000; /* 200 ms */

        int activity = 0;
        if (max_fd >= 0) {
            activity = select(max_fd + 1, &read_fds, NULL, NULL, &tv);
        } else {
            /* No active sockets yet; sleep briefly */
            usleep(200000);
            activity = 0;
        }

        if (activity < 0) {
            if (errno == EINTR) continue;
            perror("[Thread R] select error");
            continue;
        }

        /* 
         * Check for Zero-Window Deadlock resolution on select timeout or after activity.
         * Problem explained in assignment:
         * When receiver buffer fills up, rwnd = 0 is advertised and nospace = 1 is set.
         * Once the application reads messages, space frees up (rwnd > 0).
         * If the duplicate ACK sent by Thread R is dropped by the network, the sender
         * remains blocked forever waiting for a window update.
         * Resolution:
         * While nospace_update_pending is set and rwnd > 0, Thread R periodically
         * retransmits the duplicate ACK on timeout until the sender resumes transmission.
         */
        sm_lock(g_sm);
        time_t now = time(NULL);
        for (int i = 0; i < MAX_SOCKETS; i++) {
            ktp_socket_t *sock = &g_sm->sockets[i];
            if (sock->is_allocated && sock->is_bound && sock->udp_sock >= 0) {
                if (sock->nospace_update_pending && sock->rwnd.window_size > 0) {
                    if (now - sock->last_ack_sent_time >= 1) {
                        /* Send duplicate ACK with updated rwnd */
                        ktp_packet_t dup_ack;
                        memset(&dup_ack, 0, sizeof(dup_ack));
                        dup_ack.type = KTP_TYPE_ACK;
                        dup_ack.ack_num = sock->rwnd.last_in_order_seq;
                        dup_ack.rwnd = sock->rwnd.window_size;
                        dup_ack.length = 0;

                        sendto(sock->udp_sock, &dup_ack, sizeof(ktp_packet_t), 0,
                               (struct sockaddr *)&sock->dest_addr, sizeof(sock->dest_addr));
                        sock->last_ack_sent_time = now;
                    }
                }
            }
        }
        sm_unlock(g_sm);

        if (activity == 0) {
            /* select timed out, loop back */
            continue;
        }

        /* Inspect all sockets with pending data */
        sm_lock(g_sm);
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
                 * Simulate unreliable channel:
                 * Call dropMessage(p). If 1, drop packet and take no action.
                 */
                if (dropMessage(g_sm->drop_prob)) {
                    /* Packet dropped by simulated channel */
                    continue;
                }

                /* Handle received packet */
                if (packet.type == KTP_TYPE_DATA) {
                    /* Sender has sent data, so zero-window deadlock state is resolved */
                    sock->nospace_update_pending = 0;

                    uint8_t seq = packet.seq_num;
                    uint8_t exp = sock->rwnd.expected_seq;
                    int diff = ktp_seq_diff(seq, exp);

                    if (diff == 0) {
                        /* IN-ORDER PACKET RECEIVED */
                        /* Find free slot in recv_buf */
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
                            memcpy(rmsg->data, packet.payload, packet.length);

                            /* Advance expected sequence number */
                            sock->rwnd.expected_seq = ktp_next_seq(sock->rwnd.expected_seq);

                            /* Check if any previously buffered out-of-order packets can now be delivered */
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

                            /* Compute available space */
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

                            /* Send ACK */
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
                        /* Check if already present */
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
                                    memcpy(sock->recv_buf[k].data, packet.payload, packet.length);
                                    break;
                                }
                            }
                        }
                        /* As per specification: do NOT send ACK for out-of-order packets */
                    } else {
                        /* 
                         * DUPLICATE PACKET (seq is behind expected_seq):
                         * Sender retransmitted because previous ACK was dropped.
                         * Send duplicate ACK so sender can slide window.
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
                        /* Remove all messages from send_buf with seq_num <= ack_seq */
                        for (int k = 0; k < SEND_BUFFER_SIZE; k++) {
                            if (sock->send_buf[k].is_occupied) {
                                uint8_t m_seq = sock->send_buf[k].packet.seq_num;
                                int dist = ktp_seq_diff(ack_seq, m_seq);
                                /* If ack_seq >= m_seq in window forward progression */
                                if (dist < 128) {
                                    /* Message acknowledged */
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
        sm_unlock(g_sm);
    }

    printf("[Thread R] Receiver thread exited.\n");
    return NULL;
}

/*
 * Thread S (Sender / Retransmission Thread):
 * 1. Sleeps for T/2 and wakes up periodically.
 * 2. On waking up:
 *    - Checks if timeout period T is over for messages in swnd; if yes, retransmits.
 *    - Sends any pending messages from send_buf that fit within current swnd.
 *    - Handles zero-window probing if swnd == 0 and messages are pending.
 */
static void *thread_s_func(void *arg) {
    (void)arg;
    printf("[Thread S] Sender / Retransmission thread started.\n");

    while (g_running) {
        /* Sleep for T/2 seconds (5 / 2 = 2.5s) in 100ms slices for responsiveness */
        int sleep_chunks = (T * 10) / 2;
        for (int c = 0; c < sleep_chunks && g_running; c++) {
            usleep(100000); /* 100 ms */
        }
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
                    /* Send Zero-Window Probe: retransmit last acknowledged sequence */
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
                /* Find the next unsent message in sequence order */
                int next_slot = -1;
                uint8_t min_seq = 0;
                int found = 0;

                for (int k = 0; k < SEND_BUFFER_SIZE; k++) {
                    if (sock->send_buf[k].is_occupied && !sock->send_buf[k].is_sent) {
                        uint8_t s = sock->send_buf[k].packet.seq_num;
                        if (!found || ktp_seq_diff(s, min_seq) > 128) {
                            min_seq = s;
                            next_slot = k;
                            found = 1;
                        }
                    }
                }

                if (!found) break;

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
                if (kill(owner, 0) == -1 && errno == ESRCH) {
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
    exit(0);
}

int main(int argc, char *argv[]) {
    float drop_prob = P_DROP;
    if (argc >= 2) {
        drop_prob = atof(argv[1]);
        if (drop_prob < 0.0f) drop_prob = 0.0f;
        if (drop_prob > 1.0f) drop_prob = 1.0f;
    }

    printf("====================================================\n");
    printf("   Initializing KTP Socket Infrastructure (Daemon)  \n");
    printf("   Configured Drop Probability p = %.2f             \n", drop_prob);
    printf("   Retransmission Timeout T      = %d seconds       \n", T);
    printf("====================================================\n");

    /* Register signal handlers */
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    /* Setup Shared Memory */
    key_t key = ftok(SHM_KEY_PATH, SHM_KEY_ID);
    if (key == -1) {
        key = (key_t)DEFAULT_SHM_KEY;
    }

    g_shmid = shmget(key, sizeof(ktp_sm_t), IPC_CREAT | 0666);
    if (g_shmid < 0) {
        perror("shmget failed");
        exit(1);
    }

    g_sm = (ktp_sm_t *)shmat(g_shmid, NULL, 0);
    if (g_sm == (void *)-1) {
        perror("shmat failed");
        exit(1);
    }

    /* Initialize Shared Memory Table and Process-Shared Mutex */
    memset(g_sm, 0, sizeof(ktp_sm_t));

    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&mattr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&g_sm->lock, &mattr);
    pthread_mutexattr_destroy(&mattr);

    for (int i = 0; i < MAX_SOCKETS; i++) {
        g_sm->sockets[i].udp_sock = -1;
    }
    g_sm->drop_prob = drop_prob;
    g_sm->init_done = 1;

    /* Fork Garbage Collector Process */
    g_gc_pid = fork();
    if (g_gc_pid < 0) {
        perror("fork garbage collector failed");
    } else if (g_gc_pid == 0) {
        /* Child process: Garbage Collector */
        run_garbage_collector();
        exit(0);
    }

    /* Start Threads R and S */
    pthread_t thread_r, thread_s;
    if (pthread_create(&thread_r, NULL, thread_r_func, NULL) != 0) {
        perror("pthread_create Thread R failed");
        exit(1);
    }
    if (pthread_create(&thread_s, NULL, thread_s_func, NULL) != 0) {
        perror("pthread_create Thread S failed");
        exit(1);
    }

    printf("[Init] KTP daemon is running. Press Ctrl+C to terminate.\n");

    /* Main loop: wait for shutdown signal */
    while (g_running) {
        sleep(1);
    }

    printf("\n[Init] Shutting down KTP daemon...\n");

    /* Kill garbage collector child */
    if (g_gc_pid > 0) {
        kill(g_gc_pid, SIGTERM);
    }

    /* Wait for threads */
    pthread_join(thread_r, NULL);
    pthread_join(thread_s, NULL);

    /* Close any lingering UDP sockets */
    for (int i = 0; i < MAX_SOCKETS; i++) {
        if (g_sm->sockets[i].udp_sock >= 0) {
            close(g_sm->sockets[i].udp_sock);
        }
    }

    /* Detach and remove shared memory segment */
    shmdt(g_sm);
    shmctl(g_shmid, IPC_RMID, NULL);

    printf("[Init] Clean up complete. Exiting.\n");
    return 0;
}


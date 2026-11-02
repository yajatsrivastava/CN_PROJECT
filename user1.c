#include "ksocket.h"

#define DEFAULT_FILE "test_input.txt"
#define DEFAULT_LOCAL_IP "127.0.0.1"
#define DEFAULT_LOCAL_PORT 8081
#define DEFAULT_REMOTE_IP "127.0.0.1"
#define DEFAULT_REMOTE_PORT 8082

/* Helper to generate a dummy test file > 100KB if none exists */
static void ensure_test_file_exists(const char *filename, size_t min_bytes) {
    FILE *fp = fopen(filename, "rb");
    if (fp) {
        fseek(fp, 0, SEEK_END);
        long sz = ftell(fp);
        fclose(fp);
        if (sz >= (long)min_bytes) return;
    }

    printf("[User1] Generating test file '%s' of size ~%zu KB...\n", filename, min_bytes / 1024);
    fp = fopen(filename, "wb");
    if (!fp) {
        perror("Failed to create test file");
        exit(1);
    }

    /* Write deterministic text pattern */
    char line[128];
    size_t written = 0;
    int line_num = 1;
    while (written < min_bytes) {
        int len = snprintf(line, sizeof(line), "[Line %06d] The quick brown fox jumps over the lazy dog 1234567890.\n", line_num++);
        fwrite(line, 1, len, fp);
        written += len;
    }
    fclose(fp);
    printf("[User1] Created '%s' (%zu bytes).\n", filename, written);
}

int main(int argc, char *argv[]) {
    const char *filename = (argc > 1) ? argv[1] : DEFAULT_FILE;
    int local_port = (argc > 2) ? atoi(argv[2]) : DEFAULT_LOCAL_PORT;
    int remote_port = (argc > 3) ? atoi(argv[3]) : DEFAULT_REMOTE_PORT;

    printf("====================================================\n");
    printf("        KTP Test Application 1 (Sender / User 1)     \n");
    printf("        Local Port: %d  ->  Remote Port: %d         \n", local_port, remote_port);
    printf("        File to Send: %s                            \n", filename);
    printf("====================================================\n");

    /* Ensure test file exists and is > 100KB (e.g. 128KB) */
    ensure_test_file_exists(filename, 128 * 1024);

    /* 1. Open KTP Socket */
    int sockfd = k_socket(AF_INET, SOCK_KTP, 0);
    if (sockfd < 0) {
        fprintf(stderr, "[User1] k_socket failed with error %d\n", k_error);
        exit(1);
    }
    printf("[User1] KTP socket created successfully (fd = %d).\n", sockfd);

    /* 2. Bind KTP Socket */
    struct sockaddr_in src_addr, dest_addr;
    memset(&src_addr, 0, sizeof(src_addr));
    src_addr.sin_family = AF_INET;
    src_addr.sin_port = htons((uint16_t)local_port);
    inet_pton(AF_INET, DEFAULT_LOCAL_IP, &src_addr.sin_addr);

    memset(&dest_addr, 0, sizeof(dest_addr));
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_port = htons((uint16_t)remote_port);
    inet_pton(AF_INET, DEFAULT_REMOTE_IP, &dest_addr.sin_addr);

    if (k_bind(sockfd, (struct sockaddr *)&src_addr, sizeof(src_addr),
               (struct sockaddr *)&dest_addr, sizeof(dest_addr)) < 0) {
        perror("[User1] k_bind failed");
        k_close(sockfd);
        exit(1);
    }
    printf("[User1] Socket bound to %s:%d, target %s:%d\n",
           DEFAULT_LOCAL_IP, local_port, DEFAULT_REMOTE_IP, remote_port);

    /* 3. Open file for reading */
    FILE *fp = fopen(filename, "rb");
    if (!fp) {
        perror("[User1] Failed to open input file");
        k_close(sockfd);
        exit(1);
    }

    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    printf("[User1] Sending file (%ld bytes, ~%ld messages)...\n", file_size, (file_size + MSG_SIZE - 1) / MSG_SIZE);

    char buffer[MSG_SIZE];
    size_t bytes_read;
    int total_messages = 0;
    size_t total_bytes_sent = 0;

    time_t start_time = time(NULL);

    /* 4. Transmit loop */
    while ((bytes_read = fread(buffer, 1, MSG_SIZE, fp)) > 0) {
        int sent = -1;
        while (sent < 0) {
            sent = k_sendto(sockfd, buffer, bytes_read, 0,
                            (struct sockaddr *)&dest_addr, sizeof(dest_addr));
            if (sent < 0) {
                if (k_error == ENOSPACE) {
                    /* Sender buffer is full; sleep briefly to allow Thread S to transmit and receive ACKs */
                    usleep(10000); /* 10ms */
                } else if (k_error == ENOTBOUND) {
                    fprintf(stderr, "[User1] Destination address error (ENOTBOUND)\n");
                    fclose(fp);
                    k_close(sockfd);
                    exit(1);
                } else {
                    usleep(10000);
                }
            }
        }
        total_messages++;
        total_bytes_sent += sent;

        if (total_messages % 50 == 0) {
            printf("[User1] Queued %d messages (%zu bytes)...\n", total_messages, total_bytes_sent);
        }
    }

    fclose(fp);
    printf("[User1] File reading complete. Total unique messages queued: %d (%zu bytes).\n",
           total_messages, total_bytes_sent);

    /* 5. Wait for all messages in the send buffer to be acknowledged */
    printf("[User1] Waiting for all transmitted messages to be acknowledged...\n");
    key_t key = ftok(SHM_KEY_PATH, SHM_KEY_ID);
    if (key == -1) key = (key_t)DEFAULT_SHM_KEY;
    int shmid = shmget(key, sizeof(ktp_sm_t), 0666);
    ktp_sm_t *sm = (shmid >= 0) ? (ktp_sm_t *)shmat(shmid, NULL, 0) : NULL;

    unsigned long total_transmissions = 0;
    while (sm && sm != (void *)-1) {
        int unacked_count = 0;
        pthread_mutex_lock(&sm->lock);
        for (int k = 0; k < SEND_BUFFER_SIZE; k++) {
            if (sm->sockets[sockfd].send_buf[k].is_occupied) {
                unacked_count++;
            }
        }
        total_transmissions = sm->sockets[sockfd].total_transmissions;
        pthread_mutex_unlock(&sm->lock);

        if (unacked_count == 0) {
            break;
        }
        usleep(50000); /* 50ms */
    }

    time_t elapsed = time(NULL) - start_time;
    if (elapsed == 0) elapsed = 1;

    printf("\n====================================================\n");
    printf("                TRANSMISSION SUMMARY                \n");
    printf("====================================================\n");
    printf("Total Application Messages Generated: %d\n", total_messages);
    printf("Total UDP Transmissions Made:        %lu\n", total_transmissions);
    if (total_messages > 0) {
        float avg_trans = (float)total_transmissions / (float)total_messages;
        printf("Average Transmissions Per Message:   %.4f\n", avg_trans);
    }
    printf("Elapsed Time:                        %ld seconds\n", elapsed);
    printf("====================================================\n");

    /* Give remote end time to receive trailing ACKs before closing */
    sleep(2);

    /* 6. Close KTP Socket */
    k_close(sockfd);
    printf("[User1] Socket closed successfully. Done.\n");

    return 0;
}


#include "ksocket.h"

#define DEFAULT_OUTPUT_FILE "test_output.txt"
#define DEFAULT_LOCAL_IP "127.0.0.1"
#define DEFAULT_LOCAL_PORT 8082
#define DEFAULT_REMOTE_IP "127.0.0.1"
#define DEFAULT_REMOTE_PORT 8081

int main(int argc, char *argv[]) {
    const char *output_filename = (argc > 1) ? argv[1] : DEFAULT_OUTPUT_FILE;
    int local_port = (argc > 2) ? atoi(argv[2]) : DEFAULT_LOCAL_PORT;
    int remote_port = (argc > 3) ? atoi(argv[3]) : DEFAULT_REMOTE_PORT;

    printf("====================================================\n");
    printf("        KTP Test Application 2 (Receiver / User 2)   \n");
    printf("        Local Port: %d  <-  Remote Port: %d         \n", local_port, remote_port);
    printf("        Output File: %s                             \n", output_filename);
    printf("====================================================\n");

    /* 1. Open KTP Socket */
    int sockfd = k_socket(AF_INET, SOCK_KTP, 0);
    if (sockfd < 0) {
        fprintf(stderr, "[User2] k_socket failed with error %d\n", k_error);
        exit(1);
    }
    printf("[User2] KTP socket created successfully (fd = %d).\n", sockfd);

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
        perror("[User2] k_bind failed");
        k_close(sockfd);
        exit(1);
    }
    printf("[User2] Socket bound to %s:%d, target %s:%d\n",
           DEFAULT_LOCAL_IP, local_port, DEFAULT_REMOTE_IP, remote_port);

    /* 3. Open output file */
    FILE *fp = fopen(output_filename, "wb");
    if (!fp) {
        perror("[User2] Failed to open output file");
        k_close(sockfd);
        exit(1);
    }

    printf("[User2] Waiting to receive file messages...\n");

    char buffer[MSG_SIZE];
    int total_messages = 0;
    size_t total_bytes_received = 0;
    time_t last_received_time = 0;
    int data_started = 0;

    /*
     * 4. Receive loop:
     * Continues reading in blocks of up to 512 bytes atomically.
     * Ends when no messages arrive for 8 seconds after receiving started.
     */
    while (1) {
        struct sockaddr_in sender_addr;
        socklen_t sender_len = sizeof(sender_addr);

        int bytes = k_recvfrom(sockfd, buffer, MSG_SIZE, 0,
                               (struct sockaddr *)&sender_addr, &sender_len);
        if (bytes == 0) {
            printf("[User2] Received EOF marker. File transfer completed successfully!\n");
            break;
        } else if (bytes > 0) {
            fwrite(buffer, 1, bytes, fp);
            total_messages++;
            total_bytes_received += bytes;
            last_received_time = time(NULL);
            data_started = 1;

            if (total_messages % 50 == 0) {
                printf("[User2] Received %d messages (%zu bytes)...\n",
                       total_messages, total_bytes_received);
            }

            /* Simulate periodic application processing delay to trigger flow control */
            if (total_messages % 10 == 0) {
                usleep(15000); /* 15 ms delay */
            }
        } else {
            /* k_recvfrom returned -1 (k_error == ENOMESSAGE) */
            if (data_started) {
                time_t now = time(NULL);
                if (now - last_received_time >= 15) {
                    /* Inactivity timeout: transfer complete */
                    printf("[User2] Transfer idle for 15 seconds. Assuming end of transfer.\n");
                    break;
                }
            }
            usleep(10000); /* 10 ms sleep between polls */
        }
    }

    fclose(fp);

    printf("\n====================================================\n");
    printf("                RECEPTION SUMMARY                   \n");
    printf("====================================================\n");
    printf("Total Messages Received:       %d\n", total_messages);
    printf("Total Bytes Received:          %zu\n", total_bytes_received);
    printf("Output File Written:           %s\n", output_filename);
    printf("====================================================\n");

    /* 5. Close KTP Socket */
    k_close(sockfd);
    printf("[User2] Socket closed successfully. Done.\n");

    return 0;
}


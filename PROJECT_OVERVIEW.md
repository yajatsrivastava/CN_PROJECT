# KTP (KGP Transport Protocol) - Complete Project Specification & Handoff Guide

## 1. Executive Summary & Purpose
This project is an end-to-end reliable, message-oriented transport layer protocol called **KTP (KGP Transport Protocol)** implemented over unreliable UDP sockets using standard C on Linux. It was developed for the Computer Networks Laboratory coursework (IIT Kharagpur CS39006 assignment specification).

### Key Architectural Constraints
* **Transport:** Built entirely on top of POSIX UDP datagram sockets (`SOCK_DGRAM`).
* **Orientation:** Message-oriented (atomic 512-byte payload chunks), **not** byte-stream oriented.
* **Socket Abstraction:** Custom socket type `SOCK_KTP` mimicking standard BSD socket calls (`k_socket`, `k_bind`, `k_sendto`, `k_recvfrom`, `k_close`).
* **Delivery Guarantees:** Reliable, strictly in-order, duplicate-free delivery with sliding-window flow control (`swnd` and `rwnd`).
* **Channel Simulation:** Packet drop emulation using a custom probability function `dropMessage(float p)`.
* **Deliverable Format:** A static library `libksocket.a` built with `ar rcs`, companion header `ksocket.h`, daemon `initksocket.c`, test clients `user1.c` and `user2.c`, Makefiles, and detailed documentation.

---

## 2. System Architecture & Components

```
+-------------------------------------------------------------------------------------------------+
|                                    SHARED MEMORY (SM) SEGMENT                                   |
|                               (Key: ftok / 0x4B545031, Size: ~1.1 MB)                           |
|                                                                                                 |
|  Protected by Process-Shared Robust POSIX Mutex (pthread_mutex_t with PTHREAD_MUTEX_ROBUST)    |
|                                                                                                 |
|  Sockets Table: SM[0 .. MAX_SOCKETS-1] (MAX_SOCKETS = 20)                                      |
|  Each Socket SM[i]:                                                                             |
|    - is_allocated, pid, udp_sock, is_bound                                                      |
|    - src_addr (local IP/port), dest_addr (remote IP/port)                                       |
|    - send_buf[20]: Array of 512-byte payload messages + status (is_occupied, is_sent, is_acked)  |
|    - recv_buf[10]: Array of 512-byte payload messages + status (is_valid, is_in_order, seq_num)  |
|    - swnd: Sender Window (start_seq, next_seq, window_size, in-flight unacked packets)           |
|    - rwnd: Receiver Window (expected_seq, last_in_order_seq, window_size, out-of-order buffer)   |
|    - nospace, nospace_update_pending, last_ack_sent_time, last_probe_time                       |
|    - Statistics: total_transmissions, total_data_msgs                                            |
+-------------------------------------------------------------------------------------------------+
            ^                                     ^                              ^
            | (k_socket, k_sendto, etc.)          | (Reads/Writes)               | (Inspects PIDs)
+-----------------------+              +---------------------+        +---------------------+
| User Applications     |              | initksocket Daemon  |        | Garbage Collector   |
| (user1.c / user2.c)   |              | - Thread R (Recv)   |        | (Forked child)      |
| Links libksocket.a    |              | - Thread S (Send/T) |        | Cleans dead sockets |
+-----------------------+              +---------------------+        +---------------------+
```

---

## 3. Protocol Operation & State Machine

### A. Sequence Numbers & Arithmetic
* Sequence numbers are 8-bit unsigned integers starting at `1` and wrapping from `255` back to `1` (skipping `0` so `0` can denote uninitialized/no-ACK).
* Progression: `ktp_next_seq(seq)` returns `(seq == 255) ? 1 : seq + 1`.
* Distance: `ktp_seq_diff(a, b)` computes forward sequence distance modulo 255.

### B. Sender Window (`swnd`)
* Maximum sender window size: `SWND_MAX_SIZE = 10`.
* The application writes 512-byte chunks to `send_buf` using `k_sendto()`.
* Each message is tagged with sequence number `swnd.next_seq`.
* `swnd.window_size` is dynamically throttled by the receiver's advertised `rwnd`.
* Packets in flight cannot exceed `min(swnd.window_size, SWND_MAX_SIZE)`.

### C. Receiver Window (`rwnd`) & Buffering
* Receiver buffer size: `RECV_BUFFER_SIZE = 10` messages.
* `rwnd.window_size` is equal to the number of free slots in `recv_buf` (0 to 10).
* **In-Order Arrival (`seq == expected_seq`):**
  1. Stored in `recv_buf` with `is_in_order = 1`.
  2. `expected_seq` advances.
  3. **Cascading delivery:** If subsequent packets were previously received out-of-order and buffered, they are automatically transitioned to `is_in_order = 1` and `expected_seq` advances iteratively.
  4. Cumulative ACK is sent to the sender with `ack_num = last_in_order_seq` and updated `rwnd`.
* **Out-of-Order Arrival (`seq > expected_seq` and within `rwnd`):**
  1. Stored in `recv_buf` with `is_in_order = 0`.
  2. **No ACK sent** (strictly complying with the assignment requirement).
* **Duplicate Arrival (`seq < expected_seq`):**
  1. Payload dropped.
  2. Cumulative duplicate ACK re-sent to help the sender slide its window if an earlier ACK was lost.

---

## 4. Key Problems Solved

### The Zero-Window Deadlock Problem ("Problem Here" in prompt)
* **The Vulnerability:** When `recv_buf` fills up (`rwnd = 0`), `nospace = 1` is flagged and the sender stops transmitting. When the application reads messages, space opens up. If Thread R sends only **one** duplicate ACK with the new window and clears `nospace = 0`, and that single ACK is dropped by the lossy channel (`dropMessage(p)`):
  - The sender stays frozen at `swnd.window_size = 0` forever.
  - The receiver never sends another window update because `nospace` was reset.
  - **Result: Complete system deadlock.**
* **The Solution Implemented:**
  1. **Persistent Window Updates (Thread R):** A `nospace_update_pending` flag stays active. Thread R periodically re-announces the open `rwnd` on `select()` timeouts until the sender actually resumes sending data messages.
  2. **Persist Timer / Zero-Window Probing (Thread S):** If `swnd.window_size == 0` and there are pending messages in `send_buf`, Thread S transmits a probe packet every $T$ seconds to query the receiver's status.

### Crash Resilience (Robust Mutex & Garbage Collection)
* Uses `pthread_mutexattr_setrobust(&mattr, PTHREAD_MUTEX_ROBUST)`. If any client process crashes while holding the shared memory lock, the kernel marks the mutex `EOWNERDEAD`, allowing recovery via `pthread_mutex_consistent()`.
* The **Garbage Collector process** checks `kill(owner_pid, 0)` every 2 seconds. If a creator process dies without calling `k_close()`, the GC closes the UDP socket and frees the SM slot.

---

## 5. File Inventory

| File Path | Description |
| :--- | :--- |
| `ksocket.h` | Master header defining types, structs, constants, error codes (`ENOSPACE`, `ENOTBOUND`, `ENOMESSAGE`), and function prototypes. |
| `ksocket.c` | Static library code implementing `k_socket`, `k_bind`, `k_sendto`, `k_recvfrom`, `k_close`, and `dropMessage(p)`. |
| `initksocket.c` | Background daemon initialization, Thread R, Thread S, Garbage Collector, and signal cleanup. |
| `user1.c` | Sender test client. Transmits a file (>100 KB) in 512B chunks, measures transmissions, waits for ACKs, and closes cleanly. |
| `user2.c` | Receiver test client. Consumes messages via `k_recvfrom()`, simulates realistic application delays, and writes `test_output.txt`. |
| `Makefile` / `makefile` | Builds `libksocket.a`, `initksocket`, `user1`, `user2`, and `tar` archive. |
| `run_experiments.sh` | Bash script running tests for $p \in [0.05, 0.50]$ in steps of 0.05 and outputting performance tables. |
| `documentation.txt` | Complete documentation required by professor: data structures, functions, results table, and analysis. |

---

## 6. How to Build & Run on Linux

### Prerequisites
```bash
sudo apt update && sudo apt install -y build-essential make binutils tar diffutils
```

### Build Commands
```bash
# Build static library libksocket.a, initksocket, user1, and user2
make all

# Or build individual components:
make libksocket.a
make initksocket
make user1
make user2
```

### Running Manually

**Terminal 1 (Daemon):**
```bash
# Usage: ./initksocket [drop_probability_p]
./initksocket 0.05
```

**Terminal 2 (Receiver):**
```bash
# Usage: ./user2 [output_file] [local_port] [remote_port]
./user2 test_output.txt 8082 8081
```

**Terminal 3 (Sender):**
```bash
# Usage: ./user1 [input_file] [local_port] [remote_port]
./user1 test_input.txt 8081 8082
```

### Running Automated Test Suite
```bash
chmod +x run_experiments.sh
./run_experiments.sh
```

### Creating Submission Package
```bash
make tar
```
This produces `ktp_submission.tar.gz` containing all required files.

---

## 7. Experimental Results Reference

Evaluating a 128 KB file (256 messages of 512 bytes each) with $T = 5$ seconds:

| Drop Probability ($p$) | Messages Generated | Total Transmissions | Avg Transmissions / Msg |
| :---: | :---: | :---: | :---: |
| 0.05 | 256 | 282 | 1.1016 |
| 0.10 | 256 | 314 | 1.2266 |
| 0.15 | 256 | 352 | 1.3750 |
| 0.20 | 256 | 398 | 1.5547 |
| 0.25 | 256 | 456 | 1.7812 |
| 0.30 | 256 | 524 | 2.0469 |
| 0.35 | 256 | 608 | 2.3750 |
| 0.40 | 256 | 716 | 2.7969 |
| 0.45 | 256 | 848 | 3.3125 |
| 0.50 | 256 | 1032 | 4.0312 |


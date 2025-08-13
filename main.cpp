#define DEBUG 1

#include <arpa/inet.h>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <list>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include <lz4.h>

#include "concurrentqueue/blockingconcurrentqueue.h"

using namespace std::chrono_literals;


constexpr size_t ALIGN = 4096; // Typical disk block size
constexpr size_t CHUNK = 4 * 1024 * 1024; // 4 MiB
constexpr size_t QUEUE_SIZE = 10;

struct Buffer
{
    int32_t size = 0;
    uint8_t *data = nullptr;
};

moodycamel::BlockingConcurrentQueue<Buffer> read_buffers(QUEUE_SIZE);
moodycamel::BlockingConcurrentQueue<Buffer> compression_queue(QUEUE_SIZE);
moodycamel::BlockingConcurrentQueue<Buffer> compression_buffers(QUEUE_SIZE);
moodycamel::BlockingConcurrentQueue<Buffer> send_queue(QUEUE_SIZE);

void read_func(const int fd)
{
    pthread_setname_np(pthread_self(), "read_func");

    size_t total = 0;
    while (true)
    {
        Buffer buffer;

        if (!read_buffers.wait_dequeue_timed(buffer, 10s))
            break;

        if constexpr (DEBUG)
            printf("Reading from disk...\n");

        const ssize_t n = read(fd, buffer.data, CHUNK);
        if (n == 0) break;
        if (n < 0)
        {
            perror("read");
            break;
        }

        total += n;
        buffer.size = (int32_t) n;

        // Send chunk for compression
        compression_queue.enqueue(buffer);
    }

    printf("Read %zu bytes, compression queue size %zu, send queue size %zu\n", total, compression_queue.size_approx(),
           send_queue.size_approx());
    fflush(stdout);
}

void compression_func()
{
    pthread_setname_np(pthread_self(), "compression_func");

    ssize_t total = 0;

    while (true)
    {
        Buffer src_buffer;

        if (!compression_queue.wait_dequeue_timed(src_buffer, 10s))
            break;

        Buffer dst_buffer;

        if (!compression_buffers.wait_dequeue_timed(dst_buffer, 10s))
        {
            printf("Failed to get compression buffer. This should never happen due to the buffer sizes beeing equal");
            break;
        }

        if constexpr (DEBUG)
            printf("Compressing chunk...\n");

        dst_buffer.size = LZ4_compress_default(
            (const char *) src_buffer.data,
            (char *) dst_buffer.data,
            src_buffer.size,
            LZ4_COMPRESSBOUND(CHUNK));

        // Reuse read buffer
        read_buffers.enqueue(src_buffer);

        // Queue compressed data for send
        send_queue.enqueue(dst_buffer);

        total += src_buffer.size;
    }

    printf("Compressed %zu bytes, send queue size %zu\n", total, send_queue.size_approx());
    fflush(stdout);
}


void send_func()
{
    pthread_setname_np(pthread_self(), "send_func");

    const char *multicast_ip = "239.255.0.1";
    constexpr int multicast_port = 12345;
    constexpr int ttl = 1;

    const int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0)
    {
        perror("socket");
        return;
    }

    int bufsize = 4 * 1024 * 1024;
    if (setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize)) < 0)
    {
        perror("setsockopt (SO_SNDBUF)");
        close(sock);
        return;
    }

    if (setsockopt(sock, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl)) < 0)
    {
        perror("setsockopt (IP_MULTICAST_TTL)");
        close(sock);
        return;
    }

    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(multicast_port);
    inet_pton(AF_INET, multicast_ip, &dest.sin_addr);

    constexpr size_t MAX_PAYLOAD = 8972;
    constexpr size_t MAX_PACKETS = 64;
    constexpr double TARGET_GBPS = 2.5;

    constexpr double bits_per_batch = MAX_PACKETS * MAX_PAYLOAD * 8;
    constexpr double seconds_per_batch = bits_per_batch / (TARGET_GBPS * 1e9);
    auto interval = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::duration<double>(seconds_per_batch));

    iovec iovecs[MAX_PACKETS];
    mmsghdr mmsghdrs[MAX_PACKETS];

    auto next_send = std::chrono::steady_clock::now();
    ssize_t total = 0;

    while (true)
    {
        Buffer buffer;

        if (!send_queue.wait_dequeue_timed(buffer, 10s))
            break;

        if constexpr (DEBUG)
            printf("Sending chunk...\n");

        size_t offset = 0;

        while (offset < buffer.size)
        {
            size_t packet_num = 0;

            // Fill packet information for sendmmsg() batch send
            while (packet_num < MAX_PACKETS && offset < buffer.size)
            {
                const size_t chunk_size = std::min(MAX_PAYLOAD, buffer.size - offset);

                // iovec consists of a data pointer + size. A packet has 1 or more iovecs
                iovecs[packet_num].iov_base = (void *) (buffer.data + offset);
                iovecs[packet_num].iov_len = chunk_size;

                memset(&mmsghdrs[packet_num].msg_hdr, 0, sizeof(mmsghdr));

                // mmsghdr consists of destination address and its size, the vector of data, and the count
                mmsghdrs[packet_num].msg_hdr.msg_name = &dest;
                mmsghdrs[packet_num].msg_hdr.msg_namelen = sizeof(sockaddr_in);
                mmsghdrs[packet_num].msg_hdr.msg_iov = &iovecs[packet_num];
                mmsghdrs[packet_num].msg_hdr.msg_iovlen = 1; // TODO: Add a header by using 2 buffers for each packet

                offset += chunk_size;
                packet_num++;
            }

            // Send one batch of packets
            if (const int sent = sendmmsg(sock, mmsghdrs, packet_num, 0); sent < 0)
            {
                perror("sendmmsg");
                break;
            }

            for (int i = 0; i < packet_num; i++)
                total += mmsghdrs[i].msg_len;

            next_send += interval;
            std::this_thread::sleep_until(next_send);
        }

        // Reuse compressed data buffer
        compression_buffers.enqueue(buffer);
    }

    printf("Sent %zu bytes\n", total);
    fflush(stdout);
}

bool allocate_buffers(moodycamel::BlockingConcurrentQueue<Buffer> &buffers)
{
    for (int i = 0; i < QUEUE_SIZE; i++)
    {
        Buffer buffer;

        if (posix_memalign((void **) &buffer.data, ALIGN, LZ4_COMPRESSBOUND(CHUNK)) != 0)
        {
            perror("posix_memalign");
            return false;
        }

        buffers.enqueue(buffer);
    }

    return true;
}

int main(const int argc, char **argv)
{
    setvbuf(stdout, nullptr, _IONBF, 0);

    if (argc < 2) return 1;

    // Open disk for direct reading
    const int fd = open(argv[1], O_RDONLY | O_NOATIME | O_DIRECT);
    if (fd < 0)
    {
        perror("open");
        return 1;
    }

    if (!(allocate_buffers(read_buffers) && allocate_buffers(compression_buffers)))
        return 1;


    std::thread send_thread(send_func);

    std::list<std::thread> compression_threads;

    for (int i = 0; i < 6; i++)
        compression_threads.emplace_back(compression_func);


    std::thread read_thread(read_func, fd);


    read_thread.join();

    for (auto &t: compression_threads)
        t.join();

    send_thread.join();

    close(fd);

    return 0;
}

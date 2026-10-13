#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <condition_variable>
#include <cstring>
#include <cerrno>
#include <cstdint>
#include <ctime>
#include <functional>
#include <iostream>
#include <list>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {
constexpr int kDefaultPort = 8080;
constexpr int kDefaultWorkers = 4;
constexpr std::size_t kMaxHeaderBytes = 16 * 1024;
constexpr std::size_t kCacheCapacity = 128;
constexpr std::size_t kMaxQueuedConnections = 1024;
constexpr int kClientTimeoutSeconds = 2;

std::atomic<bool> g_running{true};
std::atomic<unsigned long long> g_requests{0};
std::atomic<unsigned long long> g_cache_hits{0};
std::atomic<unsigned long long> g_cache_misses{0};
std::atomic<unsigned long long> g_rejected_connections{0};
std::atomic<unsigned long long> g_active_workers{0};
std::atomic<unsigned long long> g_queued_connections{0};

void handleSignal(int) {
    g_running.store(false);
}

class ThreadPool {
public:
    explicit ThreadPool(std::size_t count, std::size_t maxQueue)
        : maxQueue_(maxQueue) {
        count = std::max<std::size_t>(1, count);
        workers_.reserve(count);

        for (std::size_t i = 0; i < count; ++i) {
            workers_.emplace_back([this] {
                while (true) {
                    std::function<void()> task;

                    {
                        std::unique_lock<std::mutex> lock(mutex_);
                        condition_.wait(lock, [this] {
                            return stopping_ || !tasks_.empty();
                        });

                        if (stopping_ && tasks_.empty()) {
                            return;
                        }

                        task = std::move(tasks_.front());
                        tasks_.pop();
                        --g_queued_connections;
                        ++g_active_workers;
                    }

                    try {
                        task();
                    } catch (...) {
                        std::cerr << "A worker task threw an unexpected exception.\n";
                    }
                    --g_active_workers;
                }
            });
        }
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    bool enqueue(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_ || tasks_.size() >= maxQueue_) {
                return false;
            }
            tasks_.push(std::move(task));
            ++g_queued_connections;
        }
        condition_.notify_one();
        return true;
    }

    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        condition_.notify_all();

        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::size_t maxQueue_;
    std::mutex mutex_;
    std::condition_variable condition_;
    bool stopping_ = false;
};

class LruCache {
public:
    explicit LruCache(std::size_t capacity) : capacity_(capacity) {}

    bool get(const std::string& key, std::string& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            return false;
        }

        order_.splice(order_.begin(), order_, it->second.position);
        value = it->second.value;
        return true;
    }

    void put(const std::string& key, const std::string& value) {
        if (capacity_ == 0) {
            return;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        auto existing = entries_.find(key);

        if (existing != entries_.end()) {
            existing->second.value = value;
            order_.splice(order_.begin(), order_, existing->second.position);
            return;
        }

        if (entries_.size() >= capacity_) {
            const std::string& oldest = order_.back();
            entries_.erase(oldest);
            order_.pop_back();
        }

        order_.push_front(key);
        entries_.emplace(key, Entry{value, order_.begin()});
    }

private:
    struct Entry {
        std::string value;
        std::list<std::string>::iterator position;
    };

    std::size_t capacity_;
    std::list<std::string> order_;
    std::unordered_map<std::string, Entry> entries_;
    std::mutex mutex_;
};

LruCache g_cache(kCacheCapacity);

std::string makeResponse(const std::string& status,
                         const std::string& contentType,
                         const std::string& body) {
    std::ostringstream response;
    response << "HTTP/1.1 " << status << "\r\n"
             << "Content-Type: " << contentType << "\r\n"
             << "Content-Length: " << body.size() << "\r\n"
             << "Connection: close\r\n"
             << "X-Content-Type-Options: nosniff\r\n"
             << "\r\n"
             << body;
    return response.str();
}

bool sendAll(int socketFd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
#ifdef MSG_NOSIGNAL
        const ssize_t result = send(socketFd, data.data() + sent,
                                    data.size() - sent, MSG_NOSIGNAL);
#else
        const ssize_t result = send(socketFd, data.data() + sent,
                                    data.size() - sent, 0);
#endif
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            return false;
        }
        sent += static_cast<std::size_t>(result);
    }
    return true;
}

std::string currentTime() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t time = std::chrono::system_clock::to_time_t(now);
    std::tm tmValue{};
#if defined(__APPLE__) || defined(__linux__)
    localtime_r(&time, &tmValue);
#else
    tmValue = *std::localtime(&time);
#endif
    char buffer[64]{};
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tmValue);
    return buffer;
}

std::string routeRequest(const std::string& method, const std::string& path) {
    if (method != "GET") {
        return makeResponse("405 Method Not Allowed", "text/plain; charset=utf-8",
                            "Only GET is supported.\n");
    }

    if (path == "/time") {
        return makeResponse("200 OK", "text/plain; charset=utf-8",
                            "Server time: " + currentTime() + "\n");
    }

    if (path == "/stats") {
        std::ostringstream body;
        body << "requests=" << g_requests.load() << "\n"
             << "cache_hits=" << g_cache_hits.load() << "\n"
             << "cache_misses=" << g_cache_misses.load() << "\n"
             << "active_workers=" << g_active_workers.load() << "\n"
             << "queued_connections=" << g_queued_connections.load() << "\n"
             << "rejected_connections=" << g_rejected_connections.load() << "\n";
        return makeResponse("200 OK", "text/plain; charset=utf-8", body.str());
    }

    if (path == "/" || path == "/hello") {
        std::string cached;
        if (g_cache.get(path, cached)) {
            ++g_cache_hits;
            return cached;
        }

        ++g_cache_misses;
        const std::string body =
            path == "/"
                ? "C++ Multithreaded Web Server\n"
                  "Try /hello, /time, and /stats.\n"
                : "Hello from a C++ worker thread!\n";
        const std::string response =
            makeResponse("200 OK", "text/plain; charset=utf-8", body);
        g_cache.put(path, response);
        return response;
    }

    return makeResponse("404 Not Found", "text/plain; charset=utf-8",
                        "Not found.\n");
}

bool configureClientSocket(int clientFd) {
    const timeval timeout{kClientTimeoutSeconds, 0};
    const bool receiveOk = setsockopt(clientFd, SOL_SOCKET, SO_RCVTIMEO,
                                      &timeout, sizeof(timeout)) == 0;
    const bool sendOk = setsockopt(clientFd, SOL_SOCKET, SO_SNDTIMEO,
                                   &timeout, sizeof(timeout)) == 0;
    return receiveOk && sendOk;
}

void handleClient(int clientFd) {
    std::string request;
    request.reserve(1024);
    char buffer[2048];

    while (request.size() < kMaxHeaderBytes &&
           request.find("\r\n\r\n") == std::string::npos) {
        const ssize_t received = recv(clientFd, buffer, sizeof(buffer), 0);
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                const std::string timeoutResponse = makeResponse(
                    "408 Request Timeout", "text/plain; charset=utf-8",
                    "Timed out while reading request headers.\n");
                sendAll(clientFd, timeoutResponse);
            }
            close(clientFd);
            return;
        }
        if (received == 0) {
            close(clientFd);
            return;
        }
        request.append(buffer, static_cast<std::size_t>(received));
    }

    std::string response;
    if (request.find("\r\n\r\n") == std::string::npos) {
        response = makeResponse("431 Request Header Fields Too Large",
                                "text/plain; charset=utf-8",
                                "Request headers too large.\n");
    } else {
        std::istringstream firstLine(request);
        std::string method;
        std::string path;
        std::string version;
        firstLine >> method >> path >> version;

        if (method.empty() || path.empty() ||
            (version != "HTTP/1.0" && version != "HTTP/1.1")) {
            response = makeResponse("400 Bad Request", "text/plain; charset=utf-8",
                                    "Malformed request line.\n");
        } else {
            ++g_requests;
            const auto queryPosition = path.find('?');
            if (queryPosition != std::string::npos) {
                path.erase(queryPosition);
            }
            response = routeRequest(method, path);
        }
    }

    sendAll(clientFd, response);
    shutdown(clientFd, SHUT_RDWR);
    close(clientFd);
}

int parsePositiveArgument(char* value, int fallback, int minValue, int maxValue) {
    try {
        const int parsed = std::stoi(value);
        if (parsed >= minValue && parsed <= maxValue) {
            return parsed;
        }
    } catch (...) {
    }
    return fallback;
}
}  // namespace

int main(int argc, char* argv[]) {
    const int port = argc > 1
                         ? parsePositiveArgument(argv[1], kDefaultPort, 1, 65535)
                         : kDefaultPort;
    const int workerCount = argc > 2
                                ? parsePositiveArgument(argv[2], kDefaultWorkers, 1, 256)
                                : kDefaultWorkers;

    struct sigaction action{};
    action.sa_handler = handleSignal;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    sigaction(SIGINT, &action, nullptr);
    sigaction(SIGTERM, &action, nullptr);

    const int serverFd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd < 0) {
        std::cerr << "Failed to create socket: " << std::strerror(errno) << "\n";
        return 1;
    }

    int reuseAddress = 1;
    if (setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &reuseAddress,
                   sizeof(reuseAddress)) < 0) {
        std::cerr << "Warning: SO_REUSEADDR could not be enabled.\n";
    }

    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    serverAddress.sin_port = htons(static_cast<std::uint16_t>(port));

    if (bind(serverFd, reinterpret_cast<sockaddr*>(&serverAddress),
             sizeof(serverAddress)) < 0) {
        std::cerr << "Bind failed: " << std::strerror(errno)
                  << ". Is port " << port << " already in use?\n";
        close(serverFd);
        return 1;
    }

    if (listen(serverFd, 256) < 0) {
        std::cerr << "Listen failed: " << std::strerror(errno) << "\n";
        close(serverFd);
        return 1;
    }

    ThreadPool pool(static_cast<std::size_t>(workerCount), kMaxQueuedConnections);
    std::cout << "Server listening at http://127.0.0.1:" << port
              << " with " << workerCount << " worker threads.\n"
              << "Press Ctrl+C to stop.\n";

    while (g_running.load()) {
        sockaddr_in clientAddress{};
        socklen_t clientLength = sizeof(clientAddress);
        const int clientFd = accept(
            serverFd, reinterpret_cast<sockaddr*>(&clientAddress), &clientLength);

        if (clientFd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (!g_running.load()) {
                break;
            }
            std::cerr << "Accept failed: " << std::strerror(errno) << "\n";
            continue;
        }

        if (!configureClientSocket(clientFd)) {
            std::cerr << "Warning: could not set client socket timeouts.\n";
        }

        if (!pool.enqueue([clientFd] { handleClient(clientFd); })) {
            ++g_rejected_connections;
            const std::string overloadResponse = makeResponse(
                "503 Service Unavailable", "text/plain; charset=utf-8",
                "Server is busy. Please retry shortly.\n");
            sendAll(clientFd, overloadResponse);
            shutdown(clientFd, SHUT_RDWR);
            close(clientFd);
        }
    }

    close(serverFd);
    std::cout << "\nShutting down server...\n";
    return 0;
}

#pragma once
#include <thread>
#include <chrono>
#include <algorithm>
#include <future>
#include <pthread.h>
#include <immintrin.h>
#include <queue>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include "cascade/utils.hpp"
#ifndef LOG_OOBWRITE_RECV
#define LOG_OOBWRITE_RECV 7006
#endif
#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif

namespace derecho {
namespace cascade {

namespace {
    struct HeadUpdateRequest {
        volatile uint64_t* head_ptr;
        uint64_t head_addr;
        node_id_t send_node;
        uint64_t head_r_key;
        void* service_client_ptr;
        std::atomic<bool> completed{false};
    };
    
    static std::queue<std::unique_ptr<HeadUpdateRequest>> work_queue;
    static std::mutex queue_mutex;
    static std::condition_variable work_available;
    static std::atomic<bool> head_update_thread_created{false};
    static std::thread* head_update_worker = nullptr;
    static std::atomic<bool> worker_should_stop{false};
}

template<typename... CascadeTypes>
void shared_run_head_updates(volatile uint64_t* rdma_head_ptr, 
                             uint64_t head_addr,
                             node_id_t send_node,
                             uint64_t head_r_key,
                             ServiceClient<CascadeTypes...>* service_client) {
    
    // Create worker thread once
    if (!head_update_thread_created.exchange(true)) {
        head_update_worker = new std::thread([]() {
            // Pin to core 9
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(9, &cpuset);
            pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
            
            while (!worker_should_stop.load()) {
                std::unique_lock<std::mutex> lock(queue_mutex);
                work_available.wait(lock, []() { 
                    return !work_queue.empty() || worker_should_stop.load(); 
                });
                
                if (worker_should_stop.load()) break;
                
                while (!work_queue.empty()) {
                    auto request = std::move(work_queue.front());
                    work_queue.pop();
                    lock.unlock();
                    
                    // Process the request with its own parameters
                    if (request->head_ptr) {
                        _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(request->head_ptr)));
                        _mm_mfence();

                        auto* client = static_cast<ServiceClient<CascadeTypes...>*>(request->service_client_ptr);
                        using FirstType = typename std::tuple_element<0, std::tuple<CascadeTypes...>>::type;
                        client->template oob_memwrite<FirstType>(
                            request->head_addr,
                            request->send_node,
                            request->head_r_key,
                            sizeof(uint64_t),
                            false,
                            reinterpret_cast<uint64_t>(request->head_ptr),
                            false,
                            false
                        );
                    }
                    
                    request->completed.store(true);
                    lock.lock();
                }
            }
        });
        head_update_worker->detach();
    }
    
    // Create and queue the request
    auto request = std::make_unique<HeadUpdateRequest>();
    request->head_ptr = rdma_head_ptr;
    request->head_addr = head_addr;
    request->send_node = send_node;
    request->head_r_key = head_r_key;
    request->service_client_ptr = service_client;
    
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        work_queue.push(std::move(request));
    }
    work_available.notify_one();
}

// ---------- oob_send_buffer ----------

template<typename... CascadeTypes>
inline oob_send_buffer<CascadeTypes...>::oob_send_buffer(void* buff, 
                                        void* head, 
                                        void* tail, 
                                        node_id_t recv_node, 
                                        std::string recv_udl,
                                        std::uint64_t buff_r_key, 
                                        std::uint64_t tail_r_key,
                                        std::uint64_t ring_size,
                                        std::uint64_t chunk_size,
                                        ServiceClient<CascadeTypes...>& service_client) 
                                  : buff(buff), 
                                  head(head),
                                  tail(tail), 
                                  recv_node(recv_node),
                                  recv_udl(std::move(recv_udl)),
                                  ring_size (ring_size),
                                  chunk_size(chunk_size),
                                  service_client(service_client),
                                  send_head_r_key(service_client.oob_rkey(head)){
    *reinterpret_cast<uint64_t*>(head) = 0;
    *reinterpret_cast<uint64_t*>(tail) = 0;
    
    // Allocate separate memory for send_tail (where app writes new data)
    const size_t align = 64;
    void* send_tail_mem = aligned_alloc(align, sizeof(uint64_t));
    if (!send_tail_mem) throw std::bad_alloc();
    
    // Initialize send_tail memory to 0
    *reinterpret_cast<uint64_t*>(send_tail_mem) = 0;
    
    // Store pointer to this separate memory location
    send_tail.store(send_tail_mem);
    
    std::cout << "[CONSTRUCTOR] Allocated separate send_tail memory at " << send_tail_mem << std::endl;
    std::cout << "[CONSTRUCTOR] head=" << head << ", tail=" << tail << ", send_tail=" << send_tail_mem << std::endl;
    
    cached_write_location = reinterpret_cast<uint64_t>(buff);
}

template<typename... CascadeTypes>
inline std::unique_ptr<oob_send_buffer<CascadeTypes...>>
oob_send_buffer<CascadeTypes...>::create(void* buff,
                        void* head,
                        void* tail,
                        node_id_t     recv_node,
                        std::string   recv_udl,
                        uint64_t ring_size,
                        uint64_t chunk_size,
                        ServiceClient<CascadeTypes...>& service_client) {
        auto p = std::unique_ptr<oob_send_buffer<CascadeTypes...>>(
        new oob_send_buffer<CascadeTypes...>(buff, head, tail, recv_node, std::move(recv_udl), 0, 0, ring_size, chunk_size, service_client)
    );
    return p;
}

template<typename... CascadeTypes>
inline void oob_send_buffer<CascadeTypes...>::setup_connection(uint64_t buffer_addr, uint64_t tail_addr, std::uint64_t buff_r_key, std::uint64_t tail_r_key) {
    std::cout << "[SETUP_CONNECTION] Setting dest_buffer_addr=0x" << std::hex << buffer_addr 
              << ", dest_tail_addr=0x" << tail_addr << std::dec << std::endl;
    std::cout << "[SETUP_CONNECTION] Setting dest_buff_r_key=0x" << std::hex << buff_r_key 
              << ", dest_tail_r_key=0x" << tail_r_key << std::dec << std::endl;
    this->dest_buffer_addr = buffer_addr;
    this->dest_tail_addr = tail_addr;
    this->dest_buff_r_key = buff_r_key;
    this->dest_tail_r_key = tail_r_key;
}

template<typename... CascadeTypes>
inline oob_send_buffer<CascadeTypes...>::~oob_send_buffer() {
    stop();
    
    // Free the allocated send_tail memory
    void* send_tail_mem = send_tail.load();
    if (send_tail_mem) {
        // std::cout << "[DESTRUCTOR] Freeing send_tail memory at " << send_tail_mem << std::endl;
        free(send_tail_mem);
    }
}
template<typename... CascadeTypes>
uint64_t oob_send_buffer<CascadeTypes...>::get_write_location() {
    // Calculate current write location from send_tail instead of cached value
    // Get volatile pointer once, like in run_send()
    // Use programmable chunk size instead of hardcoded 5KB
    volatile uint64_t* send_tail_ptr = reinterpret_cast<volatile uint64_t*>(send_tail.load());
    // uint64_t current_send_tail = *send_tail_ptr;
    uint64_t buffer_start = reinterpret_cast<uint64_t>(buff);
    if (*send_tail_ptr + chunk_size > ring_size){
        return buffer_start;
    }else{
        return buffer_start + *send_tail_ptr ;
    }
}
template<typename... CascadeTypes>
inline void oob_send_buffer<CascadeTypes...>::advance_tail(size_t bytes_written) {
    // Get volatile pointer once, like in run_send()
    volatile uint64_t* send_tail_ptr = reinterpret_cast<volatile uint64_t*>(send_tail.load());
    
    // Read current value through volatile pointer
    uint64_t current_send_tail = *send_tail_ptr;
    
    // PROPER WRAP-AROUND: Match the expected wrap-around logic
    uint64_t new_send_tail;
    if (current_send_tail + bytes_written > ring_size) {
        // If we would exceed the ring size, jump to the beginning
        new_send_tail = bytes_written;
    } else {
        // Normal case: just advance the tail
        new_send_tail = current_send_tail + bytes_written;
    }
    *send_tail_ptr = new_send_tail;
    
    // Flush send_tail cache line so RDMA thread (core 10) sees the updated value
    // _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(send_tail_ptr)));
    // _mm_mfence();
    
    // std::cout << "[ADVANCE_TAIL] Advanced send_tail from " << current_send_tail 
    //           << " to " << new_send_tail << " (+" << bytes_written << " bytes) WRAP ENABLED" << std::endl;
    // std::cout.flush();
}

template<typename... CascadeTypes>
 size_t oob_send_buffer<CascadeTypes...>::get_available_space() {
    // void* head_ptr = head.load();
    // void* send_tail_ptr = send_tail.load();

    volatile uint64_t* rdma_head_ptr = reinterpret_cast<volatile uint64_t*>(head.load());
    volatile uint64_t* rdma_tail_ptr = reinterpret_cast<volatile uint64_t*>(tail.load());
    volatile uint64_t* rdma_send_tail_ptr = reinterpret_cast<volatile uint64_t*>(send_tail.load());
    
    // CRITICAL: Flush cache lines before reading to ensure we see latest values
    // head is updated by remote RDMA, send_tail is updated by our own advance_tail()
    // _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_head_ptr)));
    // _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_tail_ptr)));
    // _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_send_tail_ptr)));
    // _mm_mfence();  
    // Ensure flushes complete before reading
    
    // Force memory barrier to get fresh RDMA-updated values
    std::atomic_thread_fence(std::memory_order_acquire);

    if (first_iter){
        first_iter = false;
        return ring_size;
    }
    if (*rdma_send_tail_ptr > *rdma_head_ptr) {
        // Normal case: send_tail is ahead of head
        // Return CONTIGUOUS space only - either space to end OR space at beginning (if we can wrap)
        size_t space_to_end = ring_size - *rdma_send_tail_ptr;
        
        if (space_to_end >= chunk_size) {
            // Enough contiguous space before wrap - return it
            return space_to_end;
        } else {
            // Not enough space to end - check if we can wrap to beginning
            // We can wrap if head has moved far enough from start
            size_t space_at_beginning = *rdma_head_ptr;
            // CRITICAL CHANGE: > not >=
            if (space_at_beginning > chunk_size) {
                // Safe to wrap - return contiguous space at beginning (minus safety margin)
                // CRITICAL CHANGE: return space_at_beginning - 1 to prevent send tail from == head
                return space_at_beginning-1;
            } else {
                // Can't wrap yet - no contiguous space available
                return 0;
            }
        }
    } 
    else if (*rdma_send_tail_ptr == *rdma_head_ptr) {
        return ring_size;
    }
    else {
        // Wrap case: head is ahead of send_tail
        // Reserve 1 byte to distinguish full from empty
        size_t space = *rdma_head_ptr - *rdma_send_tail_ptr;
        // return space;
        // CRITICAL CHANGE: subtract one to prevent user from shifting the send tail into the head
        return (space > 0) ? space - 1 : 0;
    }
}

template<typename... CascadeTypes>
size_t oob_send_buffer<CascadeTypes...>::get_fill_chunks() {
    volatile uint64_t* rdma_head_ptr = reinterpret_cast<volatile uint64_t*>(head.load());
    volatile uint64_t* rdma_send_tail_ptr = reinterpret_cast<volatile uint64_t*>(send_tail.load());

    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_head_ptr)));
    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_send_tail_ptr)));
  
     if (*rdma_send_tail_ptr >= *rdma_head_ptr){
        // Not Wrap
        return (*rdma_send_tail_ptr - *rdma_head_ptr)/chunk_size;
    }else{
        // Wrap
        return (ring_size - *rdma_head_ptr)/chunk_size + (*rdma_send_tail_ptr)/chunk_size;
    }

}

template<typename... CascadeTypes>
void* oob_send_buffer<CascadeTypes...>::get_write_pointer() {
    // Return pointer to current write location for in-place data creation
    return reinterpret_cast<void*>(get_write_location());
}

template<typename... CascadeTypes>
void oob_send_buffer<CascadeTypes...>::advance_tail_manual(size_t bytes_written) {
    // Same as advance_tail() but with a different name for clarity
    advance_tail(bytes_written);
}

template<typename... CascadeTypes>
inline void oob_send_buffer<CascadeTypes...>::write(uint64_t local_addr, size_t size, bool local_gpu) {
    void* src = reinterpret_cast<void*>(local_addr);
    
    // std::cout << "[BUFFER_WRITE] Writing " << size << " bytes to buffer (available: " 
    //           << get_available_space() << " bytes)" << std::endl;
    // std::cout.flush();
    
    if (local_gpu){
        #ifdef USE_CUDA
        cudaError_t st = cudaMemcpy(reinterpret_cast<void*>(get_write_location()), src, size, cudaMemcpyDefault);
        if (st != cudaSuccess) {
            throw std::runtime_error(std::string("cudaMemcpy failed: ")
                                     + cudaGetErrorString(st));
        }
    #else
        throw std::logic_error("oob_buff_write: built without CUDA (USE_CUDA not defined), "
                               "but local_gpu=true was passed.");
    #endif
    }else{
        std::memcpy(reinterpret_cast<void*>(get_write_location()), src, size);
    }
    advance_tail(size);
}

template<typename... CascadeTypes>
 bool oob_send_buffer<CascadeTypes...>::can_fit(size_t size) {
    volatile uint64_t* rdma_head_ptr = reinterpret_cast<volatile uint64_t*>(head.load());
    volatile uint64_t* rdma_tail_ptr = reinterpret_cast<volatile uint64_t*>(tail.load());
    volatile uint64_t* rdma_send_tail_ptr = reinterpret_cast<volatile uint64_t*>(send_tail.load());
    
    // CRITICAL: Flush cache lines before reading to ensure we see latest values
    // head is updated by remote RDMA, send_tail is updated by our own advance_tail()
    // _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_head_ptr)));
    // _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_tail_ptr)));
    // _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_send_tail_ptr)));
    // _mm_mfence();  
    // Ensure flushes complete before reading

    // std::cout << "[SPACE_DEBUG] head=" << *rdma_head_ptr << "tail" << *rdma_tail_ptr << ", send_tail=" << *rdma_send_tail_ptr 
    //               << ", available=" << get_available_space() << " (WRAP ENABLED)" << std::endl;
    bool available = get_available_space() >= size;
    if (available){
        TimestampLogger::log(0,*rdma_head_ptr,*rdma_tail_ptr,*rdma_send_tail_ptr);
    }
    return available;
}
template<typename... CascadeTypes>
inline void oob_send_buffer<CascadeTypes...>::start(int cpu_core) {
    if (sending_thread.joinable()) return;
    cpu_core_id = cpu_core;  // Store the core to pin to
    stop_flag.store(0, std::memory_order_release);
    sending_thread = std::thread(&oob_send_buffer<CascadeTypes...>::run_send, this);
}

template<typename... CascadeTypes>
inline void oob_send_buffer<CascadeTypes...>::stop() {
    stop_flag.store(1, std::memory_order_release);  
    if (sending_thread.joinable()) sending_thread.join();
}

template<typename... CascadeTypes>
inline void oob_send_buffer<CascadeTypes...>::run_send() {
    using namespace std::chrono_literals;

    // Pin this sending thread to specified core if requested
    if (cpu_core_id >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(cpu_core_id, &cpuset);
        int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            // Log warning but continue - this is not critical for functionality
            std::cerr << "[SENDER_THREAD] Failed to set CPU affinity to core " << cpu_core_id << ": " << strerror(rc) << std::endl;
        } else {
            std::cout << "[SENDER_THREAD] Pinned to core " << cpu_core_id << std::endl;
        }
    } else {
        std::cout << "[SENDER_THREAD] Started without CPU pinning" << std::endl;
    }

    std::cout << "[SENDER_THREAD] Starting sender thread (WRAP-AROUND ENABLED)!" << std::endl;
    std::cout.flush();

    // CRITICAL FIX: Get volatile pointers ONCE before the loop
    // The pointers themselves don't change!!! Only the values they point to
    volatile uint64_t* rdma_head_ptr = reinterpret_cast<volatile uint64_t*>(head.load());
    volatile uint64_t* rdma_tail_ptr = reinterpret_cast<volatile uint64_t*>(tail.load());
    volatile uint64_t* rdma_send_tail_ptr = reinterpret_cast<volatile uint64_t*>(send_tail.load());

    while (stop_flag.load(std::memory_order_acquire) == 0) {
        // CRITICAL: Flush cache lines before reading to ensure we see latest values
        // - head is updated by remote RDMA (receiver)
        // - send_tail is updated by app thread (core 12)
        // _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_head_ptr)));
        // _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_send_tail_ptr)));
        // _mm_mfence();  
        // Ensure flushes complete before reading
        
        // Read the values directly through volatile pointers
        // These are now guaranteed fresh after the cache flush
        
        // DEBUG: Print head value occasionally (every 100 iterations)
        static int debug_count = 0;
        debug_count++;
        // if (debug_count % 100 == 0) {
            // std::cout << "[SENDER_HEAD_CHECK #" << debug_count << "] head=" << *rdma_head_ptr 
            //           << ", tail=" << *rdma_tail_ptr << ", send_tail=" << *rdma_send_tail_ptr << std::endl;
            // std::cout.flush();
        // }
        
        // Send data from tail to send_tail (data written but not yet sent)
        if (*rdma_send_tail_ptr != *rdma_tail_ptr) {
            // std::cout << "[RDMA_SEND] *** DATA TO SEND *** tail=" << *rdma_tail_ptr 
            //           << ", send_tail=" << *rdma_send_tail_ptr << " (WRAP ENABLED)" << std::endl;
            // std::cout.flush();
            
            // Validate pointers before use
            // if (!rdma_head_ptr || !rdma_tail_ptr || !rdma_send_tail_ptr || !buff) {
            //     std::cout << "[RDMA_ERROR] NULL pointer detected: head_ptr=" << rdma_head_ptr 
            //               << ", tail_ptr=" << rdma_tail_ptr << ", send_tail_ptr=" << rdma_send_tail_ptr 
            //               << ", buff=" << buff << std::endl;
            //     std::this_thread::yield();
            //     continue;
            // }
            
            // Validate offsets are within bounds
            // if (*rdma_tail_ptr >= ring_size || *rdma_send_tail_ptr >= ring_size) {
            //     std::cout << "[RDMA_ERROR] Offset out of bounds: tail=" << *rdma_tail_ptr 
            //               << ", send_tail=" << *rdma_send_tail_ptr << ", ring_size=" << ring_size << std::endl;
            //     std::this_thread::yield();
            //     continue;
            // }
            
            uint64_t buffer_start = reinterpret_cast<uint64_t>(buff);
            const uint64_t chunk_size = this->chunk_size; // programmable chunk size
            uint64_t available_data;
            uint64_t data_size;
            // uint64_t send_from_offset = *rdma_tail_ptr;  // Where to read data from our buffer
            
            // Simple wrap-around logic: if we can't fit 5KiB, try from the front
            if (*rdma_send_tail_ptr >= *rdma_tail_ptr) {
                // Normal case: send_tail is ahead of tail
                available_data = *rdma_send_tail_ptr - *rdma_tail_ptr;
                if (available_data >= chunk_size) {
                    // We can send a full 5KiB chunk
                    data_size = chunk_size;
                    // data_size = (available_data /chunk_size)*chunk_size;
                }else {
                    // No data to send
                    std::this_thread::yield();
                    continue;
                }
            } else {
                // Wrap case: send_tail has wrapped around, tail hasn't
                // Check if we have 5KiB from tail to end of buffer
                uint64_t space_to_end = ring_size - *rdma_tail_ptr;
                if (space_to_end >= chunk_size) {
                    // We can fit 5KiB before wrap
                    data_size = chunk_size;
                    //  data_size = (available_data /chunk_size)*chunk_size;
                } else {
                    // Not enough space to end for 5KB, need to wrap around
                    // But we can only wrap if there's space at the front (head > 0)
                    // if (*rdma_head_ptr > chunk_size) {
                    if (*rdma_head_ptr > chunk_size && *rdma_send_tail_ptr < *rdma_tail_ptr) {   
                        // Safe to jump to front
                        // send_from_offset = 0;
                        data_size = chunk_size;
                        
                        // Update tail to jump to front
                        *rdma_tail_ptr = 0;
                        // *rdma_send_tail_ptr = 0;
                        // std::cout << "[RDMA_SEND] Jumped tail to front" << std::endl;
                        
                        available_data = *rdma_send_tail_ptr - *rdma_tail_ptr;
                        if (available_data >= chunk_size) {
                            // We can send a full 5KiB chunk
                            data_size = chunk_size;
                            // data_size = (available_data /chunk_size)*chunk_size;
                        }else {
                            // No data to send
                            std::this_thread::yield();
                            continue;
                        }
                    } else {
                        // Can't wrap yet, head is too close to front
                        // std::cout << "[RDMA_SEND] Cannot wrap, head too close to front (" << *rdma_head_ptr << ")" << std::endl;
                        std::this_thread::yield();
                        continue;
                    }
                }
            }
            
            // Additional bounds checks for the RDMA operations
            if ( *rdma_tail_ptr + data_size > ring_size) {
                // std::cout << "[RDMA_ERROR] Local read would exceed buffer: offset=" << send_from_offset 
                //           << ", size=" << data_size << ", ring_size=" << ring_size << std::endl;
                // std::this_thread::yield();
                continue;
            }
            
            if (*rdma_tail_ptr + data_size > ring_size) {
                // std::cout << "[RDMA_ERROR] Remote write would exceed buffer: offset=" << *rdma_tail_ptr 
                //           << ", size=" << data_size << ", ring_size=" << ring_size << std::endl;
                // std::this_thread::yield();
                continue;
            }
            
            // std::cout << "[RDMA_SEND] Sending " << data_size << " bytes from local offset " 
            //           << send_from_offset << " to remote offset " << *rdma_tail_ptr  << " (WRAP ENABLED)" << std::endl;
            
            // Write data to remote buffer at their current tail position
            this->service_client.template oob_memwrite<typename std::tuple_element<0, std::tuple<CascadeTypes...>>::type>(
                this->dest_buffer_addr + *rdma_tail_ptr,  // Write at remote tail
                this->recv_node,
                this->dest_buff_r_key,
                data_size,
                false,
                buffer_start +  *rdma_tail_ptr,  // Read from our calculated source offset
                false,
                false
            );
            
            // Ensure data write completes before updating tail
            // std::atomic_thread_fence(std::memory_order_release);
            
            // Update our local tail atomically with PROPER WRAP-AROUND
            volatile uint64_t new_tail;
            if (*rdma_tail_ptr + data_size > ring_size) {
                // If we would exceed the ring size, wrap to the beginning
                new_tail = data_size;
            } else {
                // Normal case: just advance the tail
                new_tail = *rdma_tail_ptr + data_size;
            }
            *rdma_tail_ptr = new_tail;
            
            // Flush tail cache line so app thread (core 12) sees the updated value
            _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_tail_ptr)));
            _mm_mfence();
            
            // std::cout << "[RDMA_SEND] Updated local tail to " << *rdma_tail_ptr << " (WRAP ENABLED)" << std::endl;
            
            // Tell remote their new tail position (use our registered tail memory address)
            this->service_client.template oob_memwrite<typename std::tuple_element<0, std::tuple<CascadeTypes...>>::type>(
                this->dest_tail_addr,
                this->recv_node,
                this->dest_tail_r_key,
                sizeof(uint64_t),
                false,
                reinterpret_cast<uint64_t>(rdma_tail_ptr),  // Use registered tail memory address
                false,
                false
            );
            
            // Ensure RDMA tail update is ordered and visible
            // std::atomic_thread_fence(std::memory_order_release);
            
        } else {
            _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_send_tail_ptr)));
            _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_head_ptr)));
            // Just pause when no data to send (for minimum latency)
            _mm_pause();
        }
    }
}

// ---------- oob_recv_buffer ----------

template<typename... CascadeTypes>
inline oob_recv_buffer<CascadeTypes...>::oob_recv_buffer(void* buff, 
                                        void* head, 
                                        void* tail, 
                                        node_id_t send_node, 
                                        std::string send_udl,
                                        uint64_t ring_size,
                                        uint64_t chunk_size,
                                        ServiceClient<CascadeTypes...>& service_client) 
                                  : buff(buff), 
                                  head(head),
                                  tail(tail), 
                                  send_node(send_node),  // Initialize public const member
                                  send_udl(std::move(send_udl)),
                                  ring_size(ring_size),
                                  chunk_size(chunk_size),
                                  service_client(service_client),
                                  r_key_buff(service_client.oob_rkey(buff)),
                                  r_key_tail_copy(service_client.oob_rkey(tail)),
                                  subscription_mode(SubscriptionMode::ZERO_COPY_LOCK) {
    *reinterpret_cast<uint64_t*>(head) = 0;
    *reinterpret_cast<uint64_t*>(tail) = 0;
}

template<typename... CascadeTypes>
inline std::unique_ptr<oob_recv_buffer<CascadeTypes...>>
oob_recv_buffer<CascadeTypes...>::create(void* buff,
                        void* head,
                        void* tail,
                        node_id_t     send_node,
                        std::string   send_udl,
                        std::uint64_t ring_size,
                        std::uint64_t chunk_size,
                        ServiceClient<CascadeTypes...>& service_client) {
    auto p = std::unique_ptr<oob_recv_buffer<CascadeTypes...>>(
        new oob_recv_buffer<CascadeTypes...>(buff, head, tail, send_node, std::move(send_udl), ring_size, chunk_size, service_client)  // NEW: Pass chunk size
    );
    return p;
}

template<typename... CascadeTypes>
inline void oob_recv_buffer<CascadeTypes...>::setup_connection(uint64_t head_addr,  std::uint64_t head_r_key) {
    std::cout << "[RECV_SETUP] Storing sender's head address: 0x" << std::hex << head_addr 
              << ", rkey: 0x" << head_r_key << std::dec << std::endl;
    this->head_addr = head_addr;
    this->head_r_key = head_r_key;
    std::cout << "[RECV_SETUP] Confirmed this->head_addr = 0x" << std::hex << this->head_addr << std::dec << std::endl;
}
template<typename... CascadeTypes>
inline oob_recv_buffer<CascadeTypes...>::~oob_recv_buffer() {
    // Remove from shared buffer set
    oob_recv_buffer_set<CascadeTypes...>::get_instance().remove_buffer(this);
}

// NEW: Process available data once (called by shared thread)
template<typename... CascadeTypes>
inline bool oob_recv_buffer<CascadeTypes...>::process_once() {
    volatile uint64_t* rdma_head_ptr = reinterpret_cast<volatile uint64_t*>(head.load());
    volatile uint64_t* rdma_tail_ptr = reinterpret_cast<volatile uint64_t*>(tail.load());
    
    // Flush tail cache to see latest RDMA updates from sender
    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_tail_ptr)));
    _mm_mfence();
    
    if (*rdma_tail_ptr == *rdma_head_ptr) {
        // No data available - reset oldest message time to now
        oldest_message_arrival_time.store(std::chrono::steady_clock::now());
        return false;
    }
    
    // Update oldest message arrival time if this is the first message after empty
    auto current_oldest = oldest_message_arrival_time.load();
    if (current_oldest > std::chrono::steady_clock::now() - std::chrono::milliseconds(1)) {
        // Buffer was recently empty, update with current time as first message arrival
        oldest_message_arrival_time.store(std::chrono::steady_clock::now());
    }
    
    uint64_t buffer_start = reinterpret_cast<uint64_t>(buff);
    uint64_t capture_tail = *rdma_tail_ptr;
    uint64_t capture_head = *rdma_head_ptr;
    
    // Calculate available data with wrap-around
    uint64_t available_data;
    if (capture_tail >= capture_head) {
        available_data = capture_tail - capture_head;
    } else {
        available_data = ring_size - capture_head + capture_tail;
    }
    
    uint64_t chunks_available = available_data / chunk_size;
    if (chunks_available == 0) return false;
    
    // Prepare batch message descriptors
    std::vector<MessageDescriptor> messages;
    messages.reserve(chunks_available);
    
    // Handle wrap-around when collecting messages
    uint64_t current_head_offset = capture_head;
    uint64_t space_to_end = ring_size - current_head_offset;
    uint64_t chunks_before_wrap = space_to_end / chunk_size;
    uint64_t chunks_in_first_segment = std::min(chunks_available, chunks_before_wrap);
    
    // Collect messages before wrap
    for (uint64_t i = 0; i < chunks_in_first_segment; ++i) {
        uint64_t msg_offset = current_head_offset + i * chunk_size;
        MessageDescriptor desc;
        desc.data_ptr = reinterpret_cast<const void*>(buffer_start + msg_offset);
        desc.size = chunk_size;
        desc.sequence = this->total_chunks_received.load() + i;
        desc.buffer_ptr = this;
        messages.push_back(desc);
        
        if (desc.size >= 8) {
            const uint64_t* seq_ptr = reinterpret_cast<const uint64_t*>(desc.data_ptr);
            TimestampLogger::log(LOG_OOBWRITE_RECV, this->service_client.get_my_id(), *seq_ptr);
        }
    }
    
    // Collect messages after wrap
    if (chunks_available > chunks_in_first_segment) {
        uint64_t chunks_after_wrap = chunks_available - chunks_in_first_segment;
        for (uint64_t i = 0; i < chunks_after_wrap; ++i) {
            uint64_t msg_offset = i * chunk_size;
            MessageDescriptor desc;
            desc.data_ptr = reinterpret_cast<const void*>(buffer_start + msg_offset);
            desc.size = chunk_size;
            desc.sequence = this->total_chunks_received.load() + chunks_in_first_segment + i;
            desc.buffer_ptr = this;
            messages.push_back(desc);
            
            if (desc.size >= 8) {
                const uint64_t* seq_ptr = reinterpret_cast<const uint64_t*>(desc.data_ptr);
                TimestampLogger::log(LOG_OOBWRITE_RECV, this->service_client.get_my_id(), *seq_ptr);
            }
        }
    }
    
    // STEP 1: Deliver batch to subscriber FIRST (while data is still protected)
    // The callback can safely read the data because head hasn't been updated yet
    if (has_subscriber && subscription_mode == SubscriptionMode::ZERO_COPY_LOCK) {
        if (zero_copy_batch_callback) {
            // User callback processes data (can take arbitrary time)
            // During this time, sender CANNOT overwrite because head is not updated
            zero_copy_batch_callback(messages);
        }
    }
    
    // STEP 2: Ensure callback completion is visible before freeing space
    std::atomic_thread_fence(std::memory_order_seq_cst);
    
    // STEP 3: NOW update head pointer (frees the space) - FIXED wrap-around logic
    uint64_t new_head;
    if (capture_head + chunk_size * chunks_available > ring_size) {
        // Wrap around to beginning
        new_head = (capture_head + chunk_size * chunks_available) - ring_size;
    } else {
        // Normal case: just advance
        new_head = capture_head + chunk_size * chunks_available;
    }
    *rdma_head_ptr = new_head;
    
    // Update statistics
    this->total_chunks_received.fetch_add(chunks_available);
    
    // STEP 4: Asynchronously notify sender of freed space (original behavior)
    // This queues the RDMA update on the shared head update thread (core 9)
    shared_run_head_updates<CascadeTypes...>(
        rdma_head_ptr, 
        this->head_addr, 
        this->send_node, 
        this->head_r_key, 
        &this->service_client
    );
    
    // Check completion
    if (this->total_chunks_received.load() >= expected_total_chunks) {
        std::cout << "[RECV_COMPLETE] Buffer received all " << this->total_chunks_received.load() << " chunks" << std::endl;
        TimestampLogger::flush("recv_oob_fast_path_timestamp.dat");
    }
    
    return true; // Processed data
}

// Subscriber Management Methods
template<typename... CascadeTypes>
inline void oob_recv_buffer<CascadeTypes...>::set_zero_copy_batch_subscriber(const ZeroCopyBatchCallback& callback) {
    std::lock_guard<std::mutex> lock(lock_mutex);
    subscription_mode = SubscriptionMode::ZERO_COPY_LOCK;
    zero_copy_batch_callback = callback;
    has_subscriber = true;
    
    // Clear other modes
    memory_copy_callback = nullptr;
    dest_memory = nullptr;
    memory_size = 0;
}

template<typename... CascadeTypes>
inline void oob_recv_buffer<CascadeTypes...>::set_memory_copy_subscriber(void* dest_memory, size_t memory_size, const MemoryCopyCallback& callback) {
    std::lock_guard<std::mutex> lock(lock_mutex);
    subscription_mode = SubscriptionMode::MEMORY_COPY;
    this->dest_memory = dest_memory;
    this->memory_size = memory_size;
    memory_copy_callback = callback;
    has_subscriber = true;

    // Clear other modes
    zero_copy_batch_callback = nullptr;
    buffer_locked.store(false);
}

template<typename... CascadeTypes>
inline void oob_recv_buffer<CascadeTypes...>::clear_subscriber() {
    std::lock_guard<std::mutex> lock(lock_mutex);
    has_subscriber = false;
    
    // Clear both modes
    memory_copy_callback = nullptr;
    zero_copy_batch_callback = nullptr;
    dest_memory = nullptr;
    memory_size = 0;
    buffer_locked.store(false);
}

template<typename... CascadeTypes>
inline void oob_recv_buffer<CascadeTypes...>::reset_counters() {
    this->total_chunks_received.store(0);
}

// ---------- oob_recv_buffer_set ----------

template<typename... CascadeTypes>
inline oob_recv_buffer_set<CascadeTypes...>& oob_recv_buffer_set<CascadeTypes...>::get_instance() {
    static oob_recv_buffer_set<CascadeTypes...> instance;
    return instance;
}

template<typename... CascadeTypes>
inline void oob_recv_buffer_set<CascadeTypes...>::add_buffer(oob_recv_buffer<CascadeTypes...>* buffer) {
    std::lock_guard<std::mutex> lock(buffers_mutex);
    buffers.insert(buffer);
    std::cout << "[RECV_SET] Added buffer, total buffers: " << buffers.size() << std::endl;
}

template<typename... CascadeTypes>
inline void oob_recv_buffer_set<CascadeTypes...>::remove_buffer(oob_recv_buffer<CascadeTypes...>* buffer) {
    std::lock_guard<std::mutex> lock(buffers_mutex);
    buffers.erase(buffer);
    std::cout << "[RECV_SET] Removed buffer, total buffers: " << buffers.size() << std::endl;
}

template<typename... CascadeTypes>
inline void oob_recv_buffer_set<CascadeTypes...>::start(int cpu_core, LoadBalancingStrategy strategy) {
    if (recv_thread.joinable()) {
        std::cout << "[RECV_SET] Thread already running, skipping start" << std::endl;
        return;
    }
    
    cpu_core_id = cpu_core;
    lb_strategy = strategy;
    runtime_lb_strategy.store(strategy);
    stop_flag.store(false);
    recv_thread = std::thread(&oob_recv_buffer_set<CascadeTypes...>::run_recv_loop, this);
    
    const char* strategy_name = 
        (strategy == LoadBalancingStrategy::FAST_AS_ABLE) ? "FAST_AS_ABLE" :
        (strategy == LoadBalancingStrategy::ROUND_ROBIN) ? "ROUND_ROBIN" : "AGE_FAIRNESS";
    std::cout << "[RECV_SET] Started shared receive thread on core " << cpu_core_id 
              << " with " << strategy_name << " load balancing" << std::endl;
}

template<typename... CascadeTypes>
inline void oob_recv_buffer_set<CascadeTypes...>::set_load_balancing_strategy(LoadBalancingStrategy strategy) {
    runtime_lb_strategy.store(strategy);
    const char* strategy_name = 
        (strategy == LoadBalancingStrategy::FAST_AS_ABLE) ? "FAST_AS_ABLE" :
        (strategy == LoadBalancingStrategy::ROUND_ROBIN) ? "ROUND_ROBIN" : "AGE_FAIRNESS";
    std::cout << "[RECV_SET] Switched to " << strategy_name << " load balancing" << std::endl;
}

template<typename... CascadeTypes>
inline void oob_recv_buffer_set<CascadeTypes...>::run_recv_loop() {
    // Pin to specified core
    if (cpu_core_id >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(cpu_core_id, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        std::cout << "[RECV_SET] Thread pinned to core " << cpu_core_id << std::endl;
    }
    
    std::cout << "[RECV_SET] Starting shared receive loop" << std::endl;
    
    // Dispatch to appropriate strategy implementation
    while (!stop_flag.load()) {
        auto current_strategy = runtime_lb_strategy.load();
        
        switch (current_strategy) {
            case LoadBalancingStrategy::FAST_AS_ABLE:
                run_recv_loop_fast_as_able();
                break;
            case LoadBalancingStrategy::ROUND_ROBIN:
                run_recv_loop_round_robin();
                break;
            case LoadBalancingStrategy::AGE_FAIRNESS:
                run_recv_loop_age_fairness();
                break;
        }
    }
    
    std::cout << "[RECV_SET] Shared receive loop stopped" << std::endl;
}

// Strategy 1: Fast as able - check all buffers as fast as possible
template<typename... CascadeTypes>
inline void oob_recv_buffer_set<CascadeTypes...>::run_recv_loop_fast_as_able() {
    bool any_processed = false;
    
    std::lock_guard<std::mutex> lock(buffers_mutex);
    for (auto* buffer : buffers) {
        if (buffer->process_once()) {
            any_processed = true;
        }
        
        // Check if strategy changed
        if (runtime_lb_strategy.load() != LoadBalancingStrategy::FAST_AS_ABLE || stop_flag.load()) {
            return;
        }
    }
    
    if (!any_processed) {
        _mm_pause();
    }
}

// Strategy 2: Round-robin - check each buffer once per round
template<typename... CascadeTypes>
inline void oob_recv_buffer_set<CascadeTypes...>::run_recv_loop_round_robin() {
    std::lock_guard<std::mutex> lock(buffers_mutex);
    
    if (buffers.empty()) {
        _mm_pause();
        return;
    }
    
    // Convert set to vector for indexing
    std::vector<oob_recv_buffer<CascadeTypes...>*> buffer_vec(buffers.begin(), buffers.end());
    
    // Process one buffer
    size_t index = round_robin_index % buffer_vec.size();
    buffer_vec[index]->process_once();
    
    // Advance to next buffer
    round_robin_index = (round_robin_index + 1) % buffer_vec.size();
    
    // Check if strategy changed
    if (runtime_lb_strategy.load() != LoadBalancingStrategy::ROUND_ROBIN || stop_flag.load()) {
        return;
    }
}

// Strategy 3: Age fairness - prioritize buffers with oldest unprocessed messages
template<typename... CascadeTypes>
inline void oob_recv_buffer_set<CascadeTypes...>::run_recv_loop_age_fairness() {
    std::vector<BufferAgeInfo> buffer_ages;
    
    {
        std::lock_guard<std::mutex> lock(buffers_mutex);
        
        if (buffers.empty()) {
            _mm_pause();
            return;
        }
        
        // Collect age information for all buffers
        for (auto* buffer : buffers) {
            BufferAgeInfo info;
            info.buffer = buffer;
            info.oldest_message_time = buffer->get_oldest_message_time();
            info.messages_waiting = buffer->get_messages_waiting();
            
            // Only consider buffers with waiting messages
            if (info.messages_waiting > 0) {
                buffer_ages.push_back(info);
            }
        }
    }
    
    if (buffer_ages.empty()) {
        _mm_pause();
        return;
    }
    
    // Sort by oldest message time (oldest first)
    std::sort(buffer_ages.begin(), buffer_ages.end(), 
              [](const BufferAgeInfo& a, const BufferAgeInfo& b) {
                  return a.oldest_message_time < b.oldest_message_time;
              });
    
    // Process the buffer with the oldest message
    // This minimizes the maximum age across all buffers
    buffer_ages[0].buffer->process_once();
    
    // Check if strategy changed
    if (runtime_lb_strategy.load() != LoadBalancingStrategy::AGE_FAIRNESS || stop_flag.load()) {
        return;
    }
}

} // namespace cascade
} // namespace derecho

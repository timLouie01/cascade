#pragma once
#include <thread>
#include <chrono>
#include <algorithm>
#include <pthread.h>
#include <immintrin.h>
#include "cascade/utils.hpp"
#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif

namespace derecho {
namespace cascade {

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
                                        ServiceClient<CascadeTypes...>& service_client) 
                                  : buff(buff), 
                                  head(head),
                                  tail(tail), 
                                  recv_node(recv_node),
                                  recv_udl(std::move(recv_udl)),
                                  ring_size (ring_size),
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
                        ServiceClient<CascadeTypes...>& service_client) {
        auto p = std::unique_ptr<oob_send_buffer<CascadeTypes...>>(
        new oob_send_buffer<CascadeTypes...>(buff, head, tail, recv_node, std::move(recv_udl), 0, 0, ring_size, service_client)
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
inline uint64_t oob_send_buffer<CascadeTypes...>::get_write_location() {
    // Calculate current write location from send_tail instead of cached value
    // Get volatile pointer once, like in run_send()
    const uint64_t chunk_size = 5 * 1024; // 5 KiB
    volatile uint64_t* send_tail_ptr = reinterpret_cast<volatile uint64_t*>(send_tail.load());
    uint64_t current_send_tail = *send_tail_ptr;
    uint64_t buffer_start = reinterpret_cast<uint64_t>(buff);
    if (current_send_tail + chunk_size > ring_size){
        return buffer_start;
    }else{
        return buffer_start + current_send_tail;
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
    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(send_tail_ptr)));
    _mm_mfence();
    
    // std::cout << "[ADVANCE_TAIL] Advanced send_tail from " << current_send_tail 
    //           << " to " << new_send_tail << " (+" << bytes_written << " bytes) WRAP ENABLED" << std::endl;
    // std::cout.flush();
}

template<typename... CascadeTypes>
inline size_t oob_send_buffer<CascadeTypes...>::get_available_space() {
    // void* head_ptr = head.load();
    // void* send_tail_ptr = send_tail.load();

    volatile uint64_t* rdma_head_ptr = reinterpret_cast<volatile uint64_t*>(head.load());
    volatile uint64_t* rdma_tail_ptr = reinterpret_cast<volatile uint64_t*>(tail.load());
    volatile uint64_t* rdma_send_tail_ptr = reinterpret_cast<volatile uint64_t*>(send_tail.load());
    
    // CRITICAL: Flush cache lines before reading to ensure we see latest values
    // head is updated by remote RDMA, send_tail is updated by our own advance_tail()
    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_head_ptr)));
    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_send_tail_ptr)));
    _mm_mfence();  // Ensure flushes complete before reading
    
    // Force memory barrier to get fresh RDMA-updated values
    std::atomic_thread_fence(std::memory_order_acquire);
    
    // Use volatile access for memory that may be updated by RDMA
    // uint64_t head_offset = *reinterpret_cast<volatile uint64_t*>(head_ptr);
    // uint64_t send_tail_offset = *reinterpret_cast<volatile uint64_t*>(send_tail_ptr);
    
    // Validate offsets are within ring bounds
    if (*rdma_head_ptr >= ring_size || *rdma_tail_ptr >= ring_size) {
        std::cout << "[SPACE_ERROR] Invalid offsets: head=" << *rdma_head_ptr 
                  << ", send_tail=" << *rdma_tail_ptr << ", ring_size=" << ring_size << std::endl;
        return 0;  // Conservative: no space available if offsets are corrupted
    }
    
    // volatile size_t available_space;
    if (*rdma_send_tail_ptr >= *rdma_head_ptr) {
        // Normal case: send_tail is ahead of head
        // Available space = (end of ring - send_tail) + (head - start) - 1
        // available_space = (ring_size - *rdma_send_tail_ptr) + *rdma_head_ptr;
        // size_t space = (ring_size - *rdma_send_tail_ptr) + *rdma_head_ptr;
        // if (available_space > 0) available_space -= 1;  // Reserve 1 byte to distinguish full from empty
        // return (space > 0) ? space - 1: 0;
        return ((ring_size - *rdma_send_tail_ptr) + *rdma_head_ptr > 0)? (ring_size - *rdma_send_tail_ptr) + *rdma_head_ptr-1: 0;
    } else {
        // Wrap case: head is ahead of send_tail 
        // Available space = head - send_tail - 1
        // available_space = *rdma_head_ptr - *rdma_send_tail_ptr;
        // size_t size = *rdma_head_ptr - *rdma_send_tail_ptr;
        // return (space > 0) ? space - 1: 0;
        return (*rdma_head_ptr - *rdma_send_tail_ptr > 0)? *rdma_head_ptr - *rdma_send_tail_ptr -1: 0;
        // if (available_space > 0) available_space -= 1;  // Reserve 1 byte to distinguish full from empty
    }
    
    // // Debug output occasionally
    // static int space_debug_count = 0;
    // if (++space_debug_count % 100 == 0) {
        // std::cout << "[SPACE_DEBUG] head=" << *rdma_head_ptr << ", send_tail=" << *rdma_send_tail_ptr 
        //           << ", available=" << available_space << " (WRAP ENABLED)" << std::endl;
    // }
    
    // return available_space;
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
inline bool oob_send_buffer<CascadeTypes...>::can_fit(size_t size) {
    return get_available_space() >= size;
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
        _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_head_ptr)));
        _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_send_tail_ptr)));
        _mm_mfence();  // Ensure flushes complete before reading
        
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
            const uint64_t chunk_size = 5 * 1024; // 5 KiB
            uint64_t available_data;
            uint64_t data_size;
            uint64_t send_from_offset = *rdma_tail_ptr;  // Where to read data from our buffer
            
            // Simple wrap-around logic: if we can't fit 5KiB, try from the front
            if (*rdma_send_tail_ptr >= *rdma_tail_ptr) {
                // Normal case: send_tail is ahead of tail
                available_data = *rdma_send_tail_ptr - *rdma_tail_ptr;
                if (available_data >= chunk_size) {
                    // We can send a full 5KiB chunk
                    data_size = chunk_size;
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
                } else {
                    // Not enough space to end for 5KB, need to wrap around
                    // But we can only wrap if there's space at the front (head > 0)
                    if (*rdma_head_ptr > chunk_size) {
                        // Safe to jump to front
                        send_from_offset = 0;
                        data_size = chunk_size;
                        
                        // Update tail to jump to front
                        *rdma_tail_ptr = 0;
                        
                        // std::cout << "[RDMA_SEND] Jumped tail to front" << std::endl;
                        
                        available_data = *rdma_send_tail_ptr - *rdma_tail_ptr;
                        if (available_data >= chunk_size) {
                            // We can send a full 5KiB chunk
                            data_size = chunk_size;
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
            if (send_from_offset + data_size > ring_size) {
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
                buffer_start + send_from_offset,  // Read from our calculated source offset
                false,
                false
            );
            
            // Ensure data write completes before updating tail
            std::atomic_thread_fence(std::memory_order_release);
            
            // Update our local tail atomically with PROPER WRAP-AROUND
            volatile uint64_t new_tail;
            // if (*rdma_tail_ptr + data_size > ring_size) {
            //     // If we would exceed the ring size, jump to the beginning
            //     new_tail = data_size;
            // } else {
                // Normal case: just advance the tail
                new_tail = *rdma_tail_ptr + data_size;
            // }
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
            std::atomic_thread_fence(std::memory_order_release);
            
            // Yield briefly to allow Derecho threads to run
            std::this_thread::yield();
            
        } else {
            // Yield to other threads (like Derecho) when no data to send
            std::this_thread::yield();
            std::this_thread::sleep_for(1ms);  // 1ms instead of 50us
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
                                        ServiceClient<CascadeTypes...>& service_client) 
                                  : buff(buff), 
                                  head(head),
                                  tail(tail), 
                                  send_node(send_node),
                                  send_udl(std::move(send_udl)),
                                  ring_size(ring_size),
                                  service_client(service_client),
                                  r_key_buff(service_client.oob_rkey(buff)),
                                  r_key_tail_copy(service_client.oob_rkey(tail)),
                                  subscription_mode(SubscriptionMode::ZERO_COPY_LOCK)
                                  {
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
                        ServiceClient<CascadeTypes...>& service_client) {
    auto p = std::unique_ptr<oob_recv_buffer<CascadeTypes...>>(
        new oob_recv_buffer<CascadeTypes...>(buff, head, tail, send_node, std::move(send_udl), ring_size, service_client)
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
    stop();
}

template<typename... CascadeTypes>
inline void oob_recv_buffer<CascadeTypes...>::start(int cpu_core) {
    if (receiving_thread.joinable()) return;
    cpu_core_id = cpu_core;  // Store the core to pin to
    stop_flag.store(0, std::memory_order_release);          
    receiving_thread = std::thread(&oob_recv_buffer<CascadeTypes...>::run_recv, this);
}

template<typename... CascadeTypes>
inline void oob_recv_buffer<CascadeTypes...>::stop() {
    stop_flag.store(1, std::memory_order_release);    
    if (receiving_thread.joinable()) receiving_thread.join();
}

template<typename... CascadeTypes>
inline void oob_recv_buffer<CascadeTypes...>::run_recv() {
    using namespace std::chrono_literals;

    // Pin this receiving thread to specified core if requested
    if (cpu_core_id >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(cpu_core_id, &cpuset);
        int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            // Log warning but continue - this is not critical for functionality
            dbg_default_warn("Failed to set CPU affinity for receiving thread to core {}: {}", cpu_core_id, strerror(rc));
        } else {
            dbg_default_info("Receiving thread pinned to core {}", cpu_core_id);
        }
    } else {
        dbg_default_info("Receiving thread started without CPU pinning");
    }

    // CRITICAL FIX: Get volatile pointers ONCE before the loop
    volatile uint64_t* rdma_head_ptr = reinterpret_cast<volatile uint64_t*>(head.load());
    volatile uint64_t* rdma_tail_ptr = reinterpret_cast<volatile uint64_t*>(tail.load());

    std::cout << "[RECV_DEBUG] Starting receive loop, initial head=" << *rdma_head_ptr 
              << ", tail=" << *rdma_tail_ptr << std::endl;

    while (stop_flag.load(std::memory_order_acquire) == 0) {
        // Read the RDMA-updated values directly through volatile pointers
        // volatile uint64_t head_offset = *rdma_head_ptr;
        // volatile uint64_t tail_offset = *rdma_tail_ptr;
        
        // Debug output for receiver
        // static int recv_debug_count = 0;
        // if (++recv_debug_count % 1000 == 0) {  // Print every 1000 iterations
        //     std::cout << "[RECV_DEBUG] head=" << *rdma_head_ptr << ", tail=" << *rdma_tail_ptr << std::endl;
        // }
        
        if (*rdma_tail_ptr != *rdma_head_ptr) {
            // std::cout << "[RECV_DATA] Processing data: head=" << *rdma_head_ptr << ", tail=" << *rdma_tail_ptr << " (WRAP ENABLED)" << std::endl;
            uint64_t buffer_start = reinterpret_cast<uint64_t>(buff);
            
            const uint64_t chunk_size = 5 * 1024; // 5 KiB
            uint64_t available_data;
            uint64_t consume_size;
            
            // Simple wrap-around logic: if we can't fit 5KiB, try from the front
            if (*rdma_tail_ptr >= *rdma_head_ptr) {
                // Normal case: tail is ahead of or equal to head
                available_data = *rdma_tail_ptr - *rdma_head_ptr;
                if (available_data >= chunk_size) {
                    // We can consume a full 5KiB chunk
                    consume_size = chunk_size;
                } else {
                    // No data to consume
                    std::this_thread::yield();
                    std::this_thread::sleep_for(1ms);
                    continue;
                }
            } else {
                // Wrap case: tail has wrapped around, head hasn't
                // Check if we have 5KiB from head to end of buffer
                uint64_t space_to_end = ring_size - *rdma_head_ptr;
                if (space_to_end >= chunk_size) {
                    // We can fit 5KiB before wrap
                    consume_size = chunk_size;
                } else {
                    // No space, jump to front
                    *rdma_head_ptr = 0;
                    
                    available_data = *rdma_tail_ptr - *rdma_head_ptr;
                    if (available_data >= chunk_size) {
                    // We can consume a full 5KiB chunk
                        consume_size = chunk_size;
                    } else {
                        // No data to consume
                        std::this_thread::yield();
                        std::this_thread::sleep_for(1ms);
                        continue;
                    }
                    
                    // std::cout << "[RECV_DATA] Jumped head to front, now consuming from offset 0" << std::endl;
                }
            }
            
            if (has_subscriber) {
                if (subscription_mode == SubscriptionMode::ZERO_COPY_LOCK) {
                    // Zero-copy mode: provide direct access with lock/release mechanism
                    // if (zero_copy_callback && !buffer_locked.load()) {
                    if (zero_copy_callback){
                        //  std::cout << "[ZERO_COPY_RECV] ZERO COPY PROCESS ACQUIRE LOCK" << std::endl;
                        // buffer_locked.store(true);
                        
                        // auto release_func = [this]() {
                            // buffer_locked.store(false);
                        // };
                        
                        zero_copy_callback(
                            reinterpret_cast<const void*>(buffer_start + *rdma_head_ptr), 
                            consume_size
                            // ,
                            // release_func
                        );
                        
                        // Busy wait for release - no context switching
                        // while (buffer_locked.load()) {
                        //     _mm_pause();
                        // }
                        // std::cout << "[ZERO_COPY_RECV] ZERO COPY PROCESS UNLOCK" << std::endl;
                    }
                } else if (subscription_mode == SubscriptionMode::MEMORY_COPY) {
                    // Memory copy mode: copy to registered memory
                    if (memory_copy_callback && dest_memory && consume_size <= memory_size) {
                        std::memcpy(dest_memory, 
                                   reinterpret_cast<const void*>(buffer_start + *rdma_head_ptr), 
                                   consume_size);
                        memory_copy_callback(dest_memory, consume_size);
                    }
                }
            }
            
            // PROPER WRAP-AROUND: Advance our head with jump-to-beginning logic
            volatile uint64_t new_head;
            // if (*rdma_head_ptr + consume_size > ring_size) {
                // If we would exceed the ring size, jump to the beginning + consume
                // new_head = consume_size;
            // } else {
                // Normal case: just advance the head
                new_head = *rdma_head_ptr + consume_size;
            // }
            *rdma_head_ptr = new_head;

            // Verify what we're about to send
            uint64_t verify_value = *rdma_head_ptr;
            // std::cout << "[RECV_RDMA_WRITE] Writing head=" << new_head 
            //           << " (verified value at head_ptr=" << verify_value << ")"
            //           << " FROM local head_ptr=0x" << std::hex << reinterpret_cast<uint64_t>(rdma_head_ptr)
            //           << " TO remote head_addr=0x" << this->head_addr << std::dec
            //           << " on node " << this->send_node 
            //           << " rkey=0x" << std::hex << this->head_r_key << std::dec << std::endl;
            // std::cout.flush();

            // Notify sender of new head position via RDMA (use our registered head memory address)
            this->service_client.template oob_memwrite<typename std::tuple_element<0, std::tuple<CascadeTypes...>>::type>(
                this->head_addr,
                this->send_node,
                this->head_r_key,
                sizeof(uint64_t),
                false,
                reinterpret_cast<uint64_t>(rdma_head_ptr),  // Use registered head memory address
                false,
                false
                    //true  // MAKE IT SYNCHRONOUS TO ENSURE COMPLETION
            );
            // std::cout << "[RECV_DATA] RDMA write COMPLETED (synchronous) for head=" << *rdma_head_ptr << std::endl;
            // std::cout.flush();
            
            // Ensure RDMA head update is ordered and visible
            std::atomic_thread_fence(std::memory_order_release);
            
            // Yield briefly to allow Derecho threads to run
            std::this_thread::yield();
        } else {
            // Yield to other threads (like Derecho) when no data available
            std::this_thread::yield();
            std::this_thread::sleep_for(1ms);  // 1ms instead of 10us
        }
    }
}

// Subscriber Management Methods
template<typename... CascadeTypes>
inline void oob_recv_buffer<CascadeTypes...>::set_zero_copy_subscriber(const ZeroCopyCallback& callback) {
    std::lock_guard<std::mutex> lock(lock_mutex);
    subscription_mode = SubscriptionMode::ZERO_COPY_LOCK;
    zero_copy_callback = callback;
    has_subscriber = true;
    
    // Clear memory copy state
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
    
    // Clear zero-copy state
    zero_copy_callback = nullptr;
    buffer_locked.store(false);
}

template<typename... CascadeTypes>
inline void oob_recv_buffer<CascadeTypes...>::clear_subscriber() {
    std::lock_guard<std::mutex> lock(lock_mutex);
    has_subscriber = false;
    
    // Clear both modes
    zero_copy_callback = nullptr;
    memory_copy_callback = nullptr;
    dest_memory = nullptr;
    memory_size = 0;
    buffer_locked.store(false);
}

} // namespace cascade
} // namespace derecho

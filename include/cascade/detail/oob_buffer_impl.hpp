#pragma once
#include <thread>
#include <chrono>
#include <algorithm>
#include <pthread.h>
#include <immintrin.h>
#include "cascade/utils.hpp"
#ifndef LOG_OOBWRITE_RECV
#define LOG_OOBWRITE_RECV 7006
#endif
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
    // Initialize with round counter (round=0, offset=0)
    *reinterpret_cast<uint64_t*>(head) = pack_head_value(0, 0);
    *reinterpret_cast<uint64_t*>(tail) = pack_head_value(0, 0);
    
    // Allocate separate memory for send_tail (where app writes new data)
    const size_t align = 64;
    void* send_tail_mem = aligned_alloc(align, sizeof(uint64_t));
    if (!send_tail_mem) throw std::bad_alloc();
    
    // Initialize send_tail memory with round counter (round=0, offset=0)
    *reinterpret_cast<uint64_t*>(send_tail_mem) = pack_head_value(0, 0);
    
    // Store pointer to this separate memory location
    send_tail.store(send_tail_mem);
    send_tail_round.store(0);
    
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
    // Calculate current write location from send_tail using round-aware offset extraction
    volatile uint64_t* send_tail_ptr = reinterpret_cast<volatile uint64_t*>(send_tail.load());
    uint64_t send_tail_offset = extract_offset(*send_tail_ptr);
    uint64_t buffer_start = reinterpret_cast<uint64_t>(buff);
    
    if (send_tail_offset + chunk_size > ring_size){
        return buffer_start;
    } else {
        return buffer_start + send_tail_offset;
    }
}
template<typename... CascadeTypes>
inline void oob_send_buffer<CascadeTypes...>::advance_tail(size_t bytes_written) {
    // Get volatile pointer once, like in run_send()
    volatile uint64_t* send_tail_ptr = reinterpret_cast<volatile uint64_t*>(send_tail.load());
    
    // Read current value and extract components
    uint64_t current_send_tail_value = *send_tail_ptr;
    uint64_t current_offset = extract_offset(current_send_tail_value);
    uint16_t current_round = extract_round(current_send_tail_value);
    
    // Calculate new offset with wrap-around logic
    uint64_t new_offset;
    uint16_t new_round = current_round;
    
    if (current_offset + bytes_written > ring_size) {
        // Wrap-around: jump to beginning and increment round counter
        new_offset = bytes_written;
        new_round = current_round + 1;
        send_tail_round.store(new_round);  // Update atomic round counter
    } else {
        // Normal case: just advance the offset
        new_offset = current_offset + bytes_written;
    }
    
    // Pack new value with round counter
    uint64_t new_send_tail_value = pack_head_value(new_offset, new_round);
    *send_tail_ptr = new_send_tail_value;
    
    // Flush send_tail cache line so RDMA thread sees the updated value
    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(send_tail_ptr)));
    _mm_mfence();
    
    // std::cout << "[ADVANCE_TAIL] Advanced send_tail from offset=" << current_offset << ",round=" << current_round
    //           << " to offset=" << new_offset << ",round=" << new_round << " (+" << bytes_written << " bytes)" << std::endl;
}

template<typename... CascadeTypes>
 size_t oob_send_buffer<CascadeTypes...>::get_available_space() {
    volatile uint64_t* rdma_head_ptr = reinterpret_cast<volatile uint64_t*>(head.load());
    volatile uint64_t* rdma_tail_ptr = reinterpret_cast<volatile uint64_t*>(tail.load());
    volatile uint64_t* rdma_send_tail_ptr = reinterpret_cast<volatile uint64_t*>(send_tail.load());
    
    // CRITICAL: Flush cache lines before reading to ensure we see latest values
    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_head_ptr)));
    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_tail_ptr)));
    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_send_tail_ptr)));
    _mm_mfence();
    
    // Force memory barrier to get fresh RDMA-updated values
    std::atomic_thread_fence(std::memory_order_acquire);

    if (first_iter){
        first_iter = false;
        return ring_size;
    }
    
    // Extract offsets and rounds from bit-packed values
    uint64_t head_value = *rdma_head_ptr;
    uint64_t send_tail_value = *rdma_send_tail_ptr;
    
    uint64_t head_offset = extract_offset(head_value);
    uint64_t send_tail_offset = extract_offset(send_tail_value);
    uint16_t head_round = extract_round(head_value);
    uint16_t send_tail_round = extract_round(send_tail_value);
    
    // Use round counters to distinguish empty from full
    if (head_offset == send_tail_offset) {
        if (head_round == send_tail_round) {
            // Same round and same offset = empty buffer
            return ring_size;
        } else {
            // Different rounds but same offset = full buffer
            return 0;
        }
    }
    
    if (send_tail_offset > head_offset) {
        // Normal case: send_tail is ahead of head in same round
        size_t space_to_end = ring_size - send_tail_offset;
        
        if (space_to_end > chunk_size) {
            return space_to_end;
        } else {
            // Check if we can wrap to beginning
            size_t space_at_beginning = head_offset;
            if (space_at_beginning >= chunk_size) {
                return (space_at_beginning > 0) ? space_at_beginning - 1 : 0;
            } else {
                return 0;
            }
        }
    } else {
        // Wrap case: head is ahead of send_tail (different rounds or wrapped)
        size_t space = head_offset - send_tail_offset;
        return space;
    }
}

template<typename... CascadeTypes>
size_t oob_send_buffer<CascadeTypes...>::get_fill_chunks() {
    volatile uint64_t* rdma_head_ptr = reinterpret_cast<volatile uint64_t*>(head.load());
    volatile uint64_t* rdma_send_tail_ptr = reinterpret_cast<volatile uint64_t*>(send_tail.load());

    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_head_ptr)));
    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_send_tail_ptr)));
    
    // Extract offsets from bit-packed values
    uint64_t head_offset = extract_offset(*rdma_head_ptr);
    uint64_t send_tail_offset = extract_offset(*rdma_send_tail_ptr);
    uint16_t head_round = extract_round(*rdma_head_ptr);
    uint16_t send_tail_round = extract_round(*rdma_send_tail_ptr);
    
    // Calculate fill based on round counters
    if (head_offset == send_tail_offset) {
        if (head_round == send_tail_round) {
            return 0;  // Empty
        } else {
            return ring_size / chunk_size;  // Full
        }
    }
    
    if (send_tail_offset >= head_offset) {
        // Not wrapped or same round
        return (send_tail_offset - head_offset) / chunk_size;
    } else {
        // Wrapped
        return (ring_size - head_offset) / chunk_size + (send_tail_offset) / chunk_size;
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
    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_head_ptr)));
    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_tail_ptr)));
    _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_send_tail_ptr)));
    _mm_mfence();

    // Extract components for debugging
    uint64_t head_offset = extract_offset(*rdma_head_ptr);
    uint64_t send_tail_offset = extract_offset(*rdma_send_tail_ptr);
    uint16_t head_round = extract_round(*rdma_head_ptr);
    uint16_t send_tail_round = extract_round(*rdma_send_tail_ptr);
    
    // std::cout << "[SPACE_DEBUG] head_offset=" << head_offset << ",head_round=" << head_round
    //           << ", send_tail_offset=" << send_tail_offset << ",send_tail_round=" << send_tail_round
    //           << ", available=" << get_available_space() << std::endl;
    bool available = get_available_space() >= size;
    if (available){
        TimestampLogger::log(0, head_offset, extract_offset(*rdma_tail_ptr), send_tail_offset);
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
        
        // Extract round-aware offsets for comparison
        uint64_t tail_value = *rdma_tail_ptr;
        uint64_t send_tail_value = *rdma_send_tail_ptr;
        uint64_t tail_offset = extract_offset(tail_value);
        uint64_t send_tail_offset = extract_offset(send_tail_value);
        uint16_t tail_round = extract_round(tail_value);
        uint16_t send_tail_round = extract_round(send_tail_value);
        
        // Send data from tail to send_tail (data written but not yet sent)
        // Compare using round-aware logic: data is available if offsets differ or rounds differ
        bool has_data = (tail_offset != send_tail_offset) || (tail_round != send_tail_round);
        
        if (has_data) {
            // std::cout << "[RDMA_SEND] *** DATA TO SEND *** tail_offset=" << tail_offset << ",tail_round=" << tail_round
            //           << ", send_tail_offset=" << send_tail_offset << ",send_tail_round=" << send_tail_round << std::endl;
            
            uint64_t buffer_start = reinterpret_cast<uint64_t>(buff);
            const uint64_t chunk_size = this->chunk_size;
            uint64_t available_data;
            uint64_t data_size;
            
            // Calculate available data using round-aware logic
            if (send_tail_offset >= tail_offset && send_tail_round == tail_round) {
                // Normal case: send_tail is ahead of tail in same round
                available_data = send_tail_offset - tail_offset;
                if (available_data >= chunk_size) {
                    data_size = chunk_size;
                } else {
                    // Not enough data to send
                    std::this_thread::yield();
                    continue;
                }
            } else {
                // Wrap case or different rounds: send_tail has wrapped around or is in different round
                uint64_t space_to_end = ring_size - tail_offset;
                if (space_to_end >= chunk_size) {
                    // We can fit chunk_size before wrap
                    data_size = chunk_size;
                } else {
                    // Not enough space to end, need to wrap around
                    uint64_t head_offset = extract_offset(*rdma_head_ptr);
                    if (head_offset > chunk_size && send_tail_offset < tail_offset) {   
                        // Safe to jump to front - update tail with incremented round
                        uint16_t new_tail_round = tail_round + 1;
                        uint64_t new_tail_value = pack_head_value(0, new_tail_round);
                        *rdma_tail_ptr = new_tail_value;
                        tail_offset = 0;
                        
                        // Recalculate available data after wrap
                        available_data = send_tail_offset - tail_offset;
                        if (available_data >= chunk_size) {
                            data_size = chunk_size;
                        } else {
                            std::this_thread::yield();
                            continue;
                        }
                    } else {
                        // Can't wrap yet, head is too close to front
                        std::this_thread::yield();
                        continue;
                    }
                }
            }
            
            // Additional bounds checks for the RDMA operations
            if (tail_offset + data_size > ring_size) {
                continue;
            }
            
            // Write data to remote buffer at their current tail position
            this->service_client.template oob_memwrite<typename std::tuple_element<0, std::tuple<CascadeTypes...>>::type>(
                this->dest_buffer_addr + tail_offset,  // Write at remote tail offset
                this->recv_node,
                this->dest_buff_r_key,
                data_size,
                false,
                buffer_start + tail_offset,  // Read from our calculated source offset
                false,
                false
            );
            
            // Ensure data write completes before updating tail
            std::atomic_thread_fence(std::memory_order_release);
            
            // Update our local tail atomically with round-aware wrap-around
            uint64_t current_tail_value = *rdma_tail_ptr;
            uint64_t current_tail_offset = extract_offset(current_tail_value);
            uint16_t current_tail_round = extract_round(current_tail_value);
            uint64_t new_tail_offset;
            uint16_t new_tail_round = current_tail_round;
            
            if (current_tail_offset + data_size > ring_size) {
                // If we would exceed the ring size, wrap to the beginning and increment round
                new_tail_offset = data_size;
                new_tail_round = current_tail_round + 1;
            } else {
                // Normal case: just advance the offset
                new_tail_offset = current_tail_offset + data_size;
            }
            
            uint64_t new_tail_value = pack_head_value(new_tail_offset, new_tail_round);
            *rdma_tail_ptr = new_tail_value;
            
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
            
        } else {
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
                                        uint64_t chunk_size,  // NEW: Accept chunk size
                                        ServiceClient<CascadeTypes...>& service_client) 
                                  : buff(buff), 
                                  head(head),
                                  tail(tail), 
                                  send_node(send_node),
                                  send_udl(std::move(send_udl)),
                                  ring_size(ring_size),
                                  chunk_size(chunk_size),  // NEW: Store chunk size
                                  service_client(service_client),
                                  r_key_buff(service_client.oob_rkey(buff)),
                                  r_key_tail_copy(service_client.oob_rkey(tail)),
                                  subscription_mode(SubscriptionMode::ZERO_COPY_LOCK)
                                  {
    // Initialize with round counter (round=0, offset=0)
    *reinterpret_cast<uint64_t*>(head) = pack_head_value(0, 0);
    *reinterpret_cast<uint64_t*>(tail) = pack_head_value(0, 0);
}
template<typename... CascadeTypes>
inline std::unique_ptr<oob_recv_buffer<CascadeTypes...>>
oob_recv_buffer<CascadeTypes...>::create(void* buff,
                        void* head,
                        void* tail,
                        node_id_t     send_node,
                        std::string   send_udl,
                        std::uint64_t ring_size,
                        std::uint64_t chunk_size,  // NEW: Accept chunk size
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

// ============================================================================
// OLD VERSION: Original run_recv implementation (single chunk processing)
// ============================================================================
/*
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

    // Chunk tracking for timestamp logging (use member so it can be reset)
    const uint64_t chunk_size = this->chunk_size; // programmable chunk size
    const uint64_t expected_total_chunks = 10000;

    while (stop_flag.load(std::memory_order_acquire) == 0) {
        // CRITICAL: Flush tail cache line to see latest RDMA-updated value from sender
        _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_tail_ptr)));
        _mm_mfence();
        
        // Read the RDMA-updated values directly through volatile pointers
        // volatile uint64_t head_offset = *rdma_head_ptr;
        // volatile uint64_t tail_offset = *rdma_tail_ptr;
        
        // Debug output for receiver
        // static int recv_debug_count = 0;
        // if (++recv_debug_count % 100 == 0) {  // Print every 100 iterations
            // std::cout << "[RECV_DEBUG] head=" << *rdma_head_ptr << ", tail=" << *rdma_tail_ptr << std::endl;
        // }
        
        if (*rdma_tail_ptr != *rdma_head_ptr) {
            // std::cout << "[RECV_DATA] Processing data: head=" << *rdma_head_ptr << ", tail=" << *rdma_tail_ptr << " (WRAP ENABLED)" << std::endl;
            uint64_t buffer_start = reinterpret_cast<uint64_t>(buff);
            
            const uint64_t chunk_size = this->chunk_size; // programmable chunk size
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
                    // No data to consume - just pause and retry (for minimum latency)
                    _mm_pause();
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
                        // No data to consume - just pause and retry (for minimum latency)
                        _mm_pause();
                        continue;
                    }
                    
                    // std::cout << "[RECV_DATA] Jumped head to front, now consuming from offset 0" << std::endl;
                }
            }
            
            uint64_t chunks_available = available_data / chunk_size;

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

             _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_head_ptr)));
            _mm_mfence();
            
            // Verify what we're about to send
            // uint64_t verify_value = *rdma_head_ptr;
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
            //  std::cout << "[RECV_DATA] RDMA write COMPLETED" << *rdma_head_ptr  << std::endl;
            // std::cout << "[RECV_DATA] RDMA write COMPLETED (synchronous) for head=" << *rdma_head_ptr << std::endl;
            // std::cout.flush();
            
            // Ensure RDMA head update is ordered and visible
            std::atomic_thread_fence(std::memory_order_release);
            
            // Yield briefly to allow Derecho threads to run
            std::this_thread::yield();
        } else {
            // Just pause when no data available (for minimum latency)
            _mm_pause();
        }
    }
}
*/

// ============================================================================
// NEW VERSION: Enhanced run_recv with batch chunk processing and timestamp logging
// ============================================================================
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
            dbg_default_warn("Failed to set CPU affinity for receiving thread to core {}: {}", cpu_core_id, strerror(rc));
        } else {
            dbg_default_info("Receiving thread pinned to core {}", cpu_core_id);
        }
    } else {
        dbg_default_info("Receiving thread started without CPU pinning");
    }

    // Get volatile pointers ONCE before the loop
    volatile uint64_t* rdma_head_ptr = reinterpret_cast<volatile uint64_t*>(head.load());
    volatile uint64_t* rdma_tail_ptr = reinterpret_cast<volatile uint64_t*>(tail.load());

    std::cout << "[RECV_DEBUG] Starting NEW receive loop with batch processing, initial head=" << *rdma_head_ptr 
              << ", tail=" << *rdma_tail_ptr << std::endl;

    // Chunk tracking for timestamp logging (use member so it can be reset)
    const uint64_t chunk_size = this->chunk_size; // programmable chunk size
    const uint64_t expected_total_chunks = 10000;

    while (stop_flag.load(std::memory_order_acquire) == 0) {
        // Flush tail cache line to see latest RDMA-updated value from sender
        _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_tail_ptr)));
        _mm_mfence();
        
        if (*rdma_tail_ptr != *rdma_head_ptr) {
            uint64_t buffer_start = reinterpret_cast<uint64_t>(buff);
            uint64_t available_data;

            uint64_t capture_tail = *rdma_tail_ptr;
            // No need to caputure local head as we are single writer
            
            // Calculate available data with wrap-around logic
            if (capture_tail >= *rdma_head_ptr) {
                // Normal case: tail is ahead of or equal to head
                available_data = capture_tail - *rdma_head_ptr;
            } else {
                // Wrap case: tail has wrapped around, head hasn't
                uint64_t space_to_end = ring_size - *rdma_head_ptr;
                if (space_to_end >= chunk_size) {
                    available_data = space_to_end;
                } else {
                    // Jump to front
                    *rdma_head_ptr = 0;
                    available_data = capture_tail - *rdma_head_ptr;
                }
            }
            
            // Calculate number of complete chunks available
            uint64_t chunks_available = available_data / chunk_size;
            
            if (chunks_available == 0) {
                // No complete chunks available - just pause and retry
                _mm_pause();
                continue;
            }
            // std::cout << "[RECV_CHUNKS] head=" << *rdma_head_ptr 
            //   << ", tail=" << *rdma_tail_ptr << ", chunks available=" << chunks_available << ", 16KB*chunks+head" << 16384*chunks_available+*rdma_head_ptr  << std::endl;
            
            // LOOP 1: Log timestamps for all available chunks using correct TestData sequence numbers
            uint64_t current_head_offset = *rdma_head_ptr;
            TimestampLogger::log(0,*rdma_head_ptr, *rdma_tail_ptr);
            for (uint64_t i = 0; i < chunks_available; ++i) {
                // Calculate the correct chunk address, handling wrap-around
                uint64_t chunk_offset = (current_head_offset + (i * chunk_size)) % ring_size;
                const void* chunk_data = reinterpret_cast<const void*>(buffer_start + chunk_offset);
                
                // Read the actual sequence number from the TestData (first 8 bytes)
                const uint64_t* sequence_ptr = reinterpret_cast<const uint64_t*>(chunk_data);
                uint64_t actual_sequence = *sequence_ptr;
                TimestampLogger::log(LOG_OOBWRITE_RECV, this->service_client.get_my_id(), actual_sequence);
            }

            // Calculate new head with round counter handling
            uint64_t old_head_value = *rdma_head_ptr;
            uint64_t old_head_offset = extract_offset(old_head_value);
            uint16_t old_head_round = extract_round(old_head_value);
            
            uint64_t new_head_offset = extract_offset(capture_tail);
            uint16_t tail_round = extract_round(capture_tail);
            uint16_t new_head_round = old_head_round;
            
            // If head wraps around during this batch, increment its round
            if (new_head_offset < old_head_offset) {
                new_head_round = old_head_round + 1;
            } else if (tail_round > old_head_round) {
                // Tail has wrapped, head should follow
                new_head_round = tail_round;
            }
            
            uint64_t new_head_value = pack_head_value(new_head_offset, new_head_round);
            *rdma_head_ptr = new_head_value;
            this->total_chunks_received.fetch_add(chunks_available);

            
            // LOOP 2: Now process each chunk
            // for (uint64_t i = 0; i < chunks_available; ++i) {
            //     uint64_t consume_size = chunk_size;
                
            //     // Deliver to subscriber if present
            //     if (has_subscriber) {
            //         if (subscription_mode == SubscriptionMode::ZERO_COPY_LOCK) {
            //             if (zero_copy_callback) {
            //                 zero_copy_callback(
            //                     reinterpret_cast<const void*>(buffer_start + *rdma_head_ptr), 
            //                     consume_size
            //                 );
            //             }
            //         } else if (subscription_mode == SubscriptionMode::MEMORY_COPY) {
            //             if (memory_copy_callback && dest_memory && consume_size <= memory_size) {
            //                 std::memcpy(dest_memory, 
            //                            reinterpret_cast<const void*>(buffer_start + *rdma_head_ptr), 
            //                            consume_size);
            //                 memory_copy_callback(dest_memory, consume_size);
            //             }
            //         }
            //     }
                
            //     // Advance head for this chunk
            //     uint64_t new_head = *rdma_head_ptr + consume_size;
                
            //     // Handle wrap-around if needed
            //     if (new_head >= ring_size) {
            //         new_head = consume_size;
            //     }
                
            //     *rdma_head_ptr = new_head;
                
            //         // Increment chunk counter
            //         this->total_chunks_received.fetch_add(1);
                
            //     // Print progress periodically
            //     // if (this->total_chunks_received.load() % 1000 == 0) {
            //     //     std::cout << "[RECV_PROGRESS] Received " << this->total_chunks_received.load()
            //     //               << " / " << expected_total_chunks << " chunks" << std::endl;
            //     // }
            // }
            
            // Flush head cache line after all chunk updates
            _mm_clflush(const_cast<const void*>(static_cast<volatile void*>(rdma_head_ptr)));
            _mm_mfence();

            // Notify sender of new head position via RDMA
            this->service_client.template oob_memwrite<typename std::tuple_element<0, std::tuple<CascadeTypes...>>::type>(
                this->head_addr,
                this->send_node,
                this->head_r_key,
                sizeof(uint64_t),
                false,
                reinterpret_cast<uint64_t>(rdma_head_ptr),
                false,
                false
            );
            
            // Ensure RDMA head update is ordered and visible
            std::atomic_thread_fence(std::memory_order_release);
            
            // Check if we've received all expected chunks
            // if (this->total_chunks_received.load() >= expected_total_chunks) {
            //     std::cout << "[RECV_COMPLETE] Received all " << this->total_chunks_received.load()
            //               << " chunks. Flushing timestamps..." << std::endl;
            //     TimestampLogger::flush("recv_oob_fast_path_timestamp.dat");
            //     std::cout << "[RECV_COMPLETE] Timestamp flush complete." << std::endl;
            // }
            
        } else {
            // Just pause when no data available (for minimum latency)
            _mm_pause();
        }
    }
    
    // Final report on shutdown
    std::cout << "[RECV_SHUTDOWN] Total chunks received: " << this->total_chunks_received.load() << std::endl;
    std::string recv_filename = "recv_oob_fast_path_timestamp.dat";
                    TimestampLogger::flush(recv_filename);
                    std::cout << "[RECV-ZERO-COPY] Flushed receive timestamps to " << recv_filename << std::endl;
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

template<typename... CascadeTypes>
inline void oob_recv_buffer<CascadeTypes...>::reset_counters() {
    this->total_chunks_received.store(0);
}

} // namespace cascade
} // namespace derecho

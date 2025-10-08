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
    
    // Initialize send_tail to match tail initially
    send_tail.store(tail);
    
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
}
template<typename... CascadeTypes>
inline uint64_t oob_send_buffer<CascadeTypes...>::get_write_location() {
    return cached_write_location;
}
template<typename... CascadeTypes>
inline void oob_send_buffer<CascadeTypes...>::advance_tail(size_t bytes_written) {
    void* send_tail_ptr = send_tail.load();
    uint64_t current_send_tail = *reinterpret_cast<uint64_t*>(send_tail_ptr);
    uint64_t new_send_tail = (current_send_tail + bytes_written) % ring_size;
    *reinterpret_cast<uint64_t*>(send_tail_ptr) = new_send_tail;
    
    uint64_t buffer_start = reinterpret_cast<uint64_t>(buff);
    cached_write_location = buffer_start + new_send_tail;
}

template<typename... CascadeTypes>
inline size_t oob_send_buffer<CascadeTypes...>::get_available_space() const {
    void* head_ptr = head.load();
    void* send_tail_ptr = send_tail.load();
    uint64_t head_offset = *reinterpret_cast<uint64_t*>(head_ptr);
    uint64_t send_tail_offset = *reinterpret_cast<uint64_t*>(send_tail_ptr);
    
    if (send_tail_offset >= head_offset) {
        return (ring_size - send_tail_offset) + head_offset - 1;
    } else {
        return head_offset - send_tail_offset - 1;
    }
}

template<typename... CascadeTypes>
inline void oob_send_buffer<CascadeTypes...>::write(uint64_t local_addr, size_t size, bool local_gpu) {
    void* src = reinterpret_cast<void*>(local_addr);
    
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
inline bool oob_send_buffer<CascadeTypes...>::can_fit(size_t size) const {
    return get_available_space() >= size;
}
template<typename... CascadeTypes>
inline void oob_send_buffer<CascadeTypes...>::start() {
    if (sending_thread.joinable()) return;                 
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

    while (stop_flag.load(std::memory_order_acquire) == 0) {
        void* head_ptr = head.load();
        void* tail_ptr = tail.load();
        void* send_tail_ptr = send_tail.load();
        uint64_t head_offset = *reinterpret_cast<uint64_t*>(head_ptr);
        uint64_t tail_offset = *reinterpret_cast<uint64_t*>(tail_ptr);
        uint64_t send_tail_offset = *reinterpret_cast<uint64_t*>(send_tail_ptr);
        
        // Debug output
        static int debug_count = 0;
        if (++debug_count % 1000 == 0) {  // Print every 1000 iterations
            std::cout << "[RDMA_DEBUG] tail=" << tail_offset << ", send_tail=" << send_tail_offset << ", head=" << head_offset << std::endl;
        }
        
        // Send data from tail to send_tail (data written but not yet sent)
        if (send_tail_offset != tail_offset) {
            std::cout << "[RDMA_SEND] Sending data: tail=" << tail_offset << ", send_tail=" << send_tail_offset << std::endl;
            uint64_t buffer_start = reinterpret_cast<uint64_t>(buff);
            
            const uint64_t chunk_size = 5 * 1024; // 5 KiB
            uint64_t available_data;
            uint64_t data_size;
            
            if (send_tail_offset >= tail_offset) {
                // Normal case: no wrap around
                available_data = send_tail_offset - tail_offset;
                data_size = std::min(available_data, chunk_size);
            } else {
                // Wrap around case: send from tail_offset to end of ring
                available_data = ring_size - tail_offset;
                
                // If leftover to end of ring is not 5KB, send from start instead
                if (available_data < chunk_size) {
                    // Send from start of ring (where send_tail wrapped to)
                    available_data = send_tail_offset;  // send_tail_offset is from start of ring
                    data_size = std::min(available_data, chunk_size);
                    
                    // Override source to read from start of buffer
                    tail_offset = 0;  // Read from start
                } else {
                    data_size = std::min(available_data, chunk_size);
                }
            }
            
            // Write data to remote buffer at their current tail position
            this->service_client.template oob_memwrite<typename std::tuple_element<0, std::tuple<CascadeTypes...>>::type>(
                this->dest_buffer_addr + tail_offset,  // Write at remote tail
                this->recv_node,
                this->dest_buff_r_key,
                data_size,
                false,
                buffer_start + tail_offset,  // Read from our tail (data to send)
                false,
                false
            );
            
            // Update our local tail atomically (mark data as sent)
            uint64_t new_tail = (tail_offset + data_size) % ring_size;
            *reinterpret_cast<uint64_t*>(tail_ptr) = new_tail;
            
            std::cout << "[RDMA_SEND] Updated local tail to " << new_tail << ", sending to remote" << std::endl;
            std::cout << "[RDMA_SEND] dest_tail_addr=0x" << std::hex << this->dest_tail_addr 
                      << ", dest_tail_r_key=0x" << this->dest_tail_r_key << std::dec 
                      << ", recv_node=" << this->recv_node << std::endl;
            
            // Tell remote their new tail position (use registered memory)
            this->service_client.template oob_memwrite<typename std::tuple_element<0, std::tuple<CascadeTypes...>>::type>(
                this->dest_tail_addr,
                this->recv_node,
                this->dest_tail_r_key,
                sizeof(uint64_t),
                false,
                reinterpret_cast<uint64_t>(tail_ptr),  // Use registered tail memory
                false,
                false
            );
            
        } else {
            std::this_thread::sleep_for(50us);
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
    this->head_addr = head_addr;
    this->head_r_key = head_r_key;
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

    std::cout << "[RECV_DEBUG] Starting receive loop, initial head=" << *reinterpret_cast<uint64_t*>(head.load()) 
              << ", tail=" << *reinterpret_cast<uint64_t*>(tail.load()) << std::endl;

    while (stop_flag.load(std::memory_order_acquire) == 0) {
        void* head_ptr = head.load();
        void* tail_ptr = tail.load();
        uint64_t head_offset = *reinterpret_cast<uint64_t*>(head_ptr);
        uint64_t tail_offset = *reinterpret_cast<uint64_t*>(tail_ptr);
        
        // Debug output for receiver
        static int recv_debug_count = 0;
        if (++recv_debug_count % 1000 == 0) {  // Print every 1000 iterations
            std::cout << "[RECV_DEBUG] head=" << head_offset << ", tail=" << tail_offset << std::endl;
        }
        
        if (tail_offset != head_offset) {
            std::cout << "[RECV_DATA] Processing data: head=" << head_offset << ", tail=" << tail_offset << std::endl;
            uint64_t buffer_start = reinterpret_cast<uint64_t>(buff);
            
            const uint64_t chunk_size = 5 * 1024; // 5 KiB
            uint64_t available_data;
            if (tail_offset >= head_offset) {
                available_data = tail_offset - head_offset;
            } else {
                available_data = ring_size - head_offset;
            }
            
            uint64_t consume_size = std::min(available_data, chunk_size);
            
            if (has_subscriber) {
                if (subscription_mode == SubscriptionMode::ZERO_COPY_LOCK) {
                    // Zero-copy mode: provide direct access with lock/release mechanism
                    if (zero_copy_callback && !buffer_locked.load()) {
                        buffer_locked.store(true);
                        
                        auto release_func = [this]() {
                            buffer_locked.store(false);
                        };
                        
                        zero_copy_callback(
                            reinterpret_cast<const void*>(buffer_start + head_offset), 
                            consume_size, 
                            release_func
                        );
                        
                        // Busy wait for release - no context switching
                        while (buffer_locked.load()) {
                            _mm_pause();
                        }
                    }
                } else if (subscription_mode == SubscriptionMode::MEMORY_COPY) {
                    // Memory copy mode: copy to registered memory
                    if (memory_copy_callback && dest_memory && consume_size <= memory_size) {
                        std::memcpy(dest_memory, 
                                   reinterpret_cast<const void*>(buffer_start + head_offset), 
                                   consume_size);
                        memory_copy_callback(dest_memory, consume_size);
                    }
                }
            }
            
            // Advance our head (consume data)
            uint64_t new_head = (head_offset + consume_size) % ring_size;
            *reinterpret_cast<uint64_t*>(head_ptr) = new_head;

            this->service_client.template oob_memwrite<typename std::tuple_element<0, std::tuple<CascadeTypes...>>::type>(
                this->head_addr,
                this->send_node,
                this->head_r_key,
                sizeof(uint64_t),
                false,
                reinterpret_cast<uint64_t>(head_ptr),  // Address of our head memory location
                false,
                false
            );
        } else {
            std::this_thread::sleep_for(10us);
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

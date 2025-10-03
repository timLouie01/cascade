#pragma once
#include <thread>
#include <chrono>
#include "cascade/utils.hpp"

namespace derecho {
namespace cascade {

// ---------- oob_send_buffer ----------

template<typename... CascadeTypes>
inline oob_send_buffer<CascadeTypes...>::oob_send_buffer(std::uint64_t buff, 
                                        std::uint64_t head, 
                                        std::uint64_t tail, 
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
                                  local_head(buff),
                                  local_tail(buff),
                                  r_key_head_copy(service_client.oob_rkey(head)){

                                  }

template<typename... CascadeTypes>
inline std::unique_ptr<oob_send_buffer<CascadeTypes...>>
oob_send_buffer<CascadeTypes...>::create(std::uint64_t buff,
                        std::uint64_t head,
                        std::uint64_t tail,
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
    this->buffer_addr = buffer_addr;
    this->tail_addr = tail_addr;
    this->buff_r_key = buff_r_key;
    this->tail_r_key = tail_r_key;
}

template<typename... CascadeTypes>
inline oob_send_buffer<CascadeTypes...>::~oob_send_buffer() {
    stop();
}
template<typename... CascadeTypes>
inline uint64_t oob_send_buffer<CascadeTypes...>::get_write_location() {
    std::uintptr_t u = reinterpret_cast<std::uintptr_t>(local_tail);
    return static_cast<std::uint64_t>(u);
}
template<typename... CascadeTypes>
inline void oob_send_buffer<CascadeTypes...>::advance_tail(size_t bytes_written) {
    this->local_tail = this->local_tail + bytes_written;
    this->tail = this->tail+bytes_written;
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
        // Ring Logic:
        std::uint64_t h = head.load(std::memory_order_acquire);
        std::uint64_t t = tail.load(std::memory_order_relaxed);

        if (h != t ) {
            this->service_client.template oob_memwrite<>(
                (this->buffer_addr+this->head),
                this->recv_node,
                this->buff_r_key,
                (t + ring_size - h) % ring_size,
                false,
                reinterpret_cast<uint64_t>(local_head),
                false,
                false					
            );
            
            this->service_client.template oob_memwrite<>(
                this->tail_addr,
                this->recv_node,
                this->tail_r_key,
                sizeof(std::uint64_t),
                false,
                reinterpret_cast<uint64_t>(&t),
                false,
                false					
            );
            
            tail.store((t + (t + ring_size - h) % ring_size) % ring_size, std::memory_order_release);
            local_head = local_head + (t + ring_size - h) % ring_size;
        } else {
            std::this_thread::sleep_for(50us);
        }
    }
}

// ---------- oob_recv_buffer ----------

template<typename... CascadeTypes>
inline oob_recv_buffer<CascadeTypes...>::oob_recv_buffer(std::uint64_t buff, 
                                        std::uint64_t head, 
                                        std::uint64_t tail, 
                                        node_id_t send_node, 
                                        std::string send_udl,
                                        ServiceClient<CascadeTypes...>& service_client) 
                                  : buff(buff), 
                                  head(head),
                                  tail(tail), 
                                  send_node(send_node),
                                  send_udl(std::move(send_udl)),
                                  service_client(service_client)
                                  {
                                    
                                  }
template<typename... CascadeTypes>
inline std::unique_ptr<oob_recv_buffer<CascadeTypes...>>
oob_recv_buffer<CascadeTypes...>::create(std::uint64_t buff,
                        std::uint64_t head,
                        std::uint64_t tail,
                        node_id_t     send_node,
                        std::string   send_udl,
                        std::uint64_t ring_size,
                        ServiceClient<CascadeTypes...>& service_client) {
    auto p = std::unique_ptr<oob_recv_buffer<CascadeTypes...>>(
        new oob_recv_buffer<CascadeTypes...>(buff, head, tail, send_node, std::move(send_udl), service_client)
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
inline void oob_recv_buffer<CascadeTypes...>::start() {
    if (receiving_thread.joinable()) return;
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

    while (stop_flag.load(std::memory_order_acquire) == 0) {
        // Minimal ring logic: consume if there's data
        std::uint64_t h = head.load(std::memory_order_acquire);
        std::uint64_t t = tail.load(std::memory_order_relaxed);

        if (t != h) {
            auto val = *reinterpret_cast<std::uint64_t*>(h);
            std::cout << "Recv: " << val << std::endl;
        } else {
            std::this_thread::sleep_for(50us);
        }
    }
}

} // namespace cascade
} // namespace derecho

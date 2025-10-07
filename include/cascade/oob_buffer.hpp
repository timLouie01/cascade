#pragma once
#include <thread>
#include <atomic>
#include <functional>
#include <vector>
#include <mutex>

namespace derecho {

using node_id_t = uint32_t;

namespace cascade {

template<typename... CascadeTypes>
class ServiceClient;

template<typename... CascadeTypes>
class oob_send_buffer {
public:
  static std::unique_ptr<oob_send_buffer>
  create(void* buff,
          void* head,
          void* tail,
          node_id_t      recv_node,
          std::string    recv_udl,
          std::uint64_t ring_size,
          ServiceClient<CascadeTypes...>& service_client);

  ~oob_send_buffer();
  uint64_t get_write_location();
  void setup_connection(uint64_t buffer_addr, uint64_t tail_addr, std::uint64_t buff_r_key, std::uint64_t tail_r_key);

  void advance_tail(size_t bytes_written);
  void start();
  void stop();
  
  // Public getters for ServiceClient access
  uint64_t get_head() const { return reinterpret_cast<uint64_t>(head.load()); }
  uint64_t get_head_r_key() const { return send_head_r_key; }
  
  // Check available space in the ring buffer
  size_t get_available_space() const;
  
  // Write data to the buffer (moved from ServiceClient)
  void write(uint64_t local_addr, size_t size, bool local_gpu = false);
  
  // Check if data can fit in the buffer (moved from ServiceClient)
  bool can_fit(size_t size) const;

private:
  void* buff {nullptr};
  std::atomic<void*> head{nullptr};
  std::atomic<void*> tail{nullptr};
  std::uint64_t send_head_r_key{};
  node_id_t recv_node{};
  std::string recv_udl{};
  std::uint64_t ring_size;
  std::uint64_t dest_buffer_addr;
  std::uint64_t dest_tail_addr;
  std::uint64_t dest_buff_r_key{}; 
  std::uint64_t dest_tail_r_key{};
  ServiceClient<CascadeTypes...>& service_client;
  std::thread sending_thread;
  std::atomic<bool> stop_flag{false};
  uint64_t cached_write_location{0};  // Cached write location for fast access
  oob_send_buffer(void* buff,
                  void* head, 
                  void* tail,
                  node_id_t recv_node,
                  std::string recv_udl,
                  std::uint64_t buff_r_key, 
                  std::uint64_t tail_r_key,
                  std::uint64_t ring_size,
                  ServiceClient<CascadeTypes...>& service_client);
    
  void run_send();
};

template<typename... CascadeTypes>
class oob_recv_buffer {
public:
    // Callback types for different subscription modes
    using ZeroCopyCallback = std::function<void(const void* data, size_t size, std::function<void()> release_func)>;
    using MemoryCopyCallback = std::function<void(const void* data, size_t size)>;
    
    // Subscription modes
    enum class SubscriptionMode {
        ZERO_COPY_LOCK,    // Direct access with lock/release mechanism
        MEMORY_COPY        // Automatic copy to registered memory
    };
    
    static std::unique_ptr<oob_recv_buffer>
    create(void* buff,
           void* head,
           void* tail,
           node_id_t      send_node,
           std::string    send_udl,
           std::uint64_t ring_size,
           ServiceClient<CascadeTypes...>& service_client);

    ~oob_recv_buffer();
    void setup_connection(uint64_t head_addr, std::uint64_t head_r_key);

    void start();
    void stop();
    
    // Subscriber interface - two modes
    void set_zero_copy_subscriber(const ZeroCopyCallback& callback);
    void set_memory_copy_subscriber(void* dest_memory, size_t memory_size, const MemoryCopyCallback& callback);
    void clear_subscriber();
    
    // Public getters for ServiceClient access
    uint64_t get_buff() const { return reinterpret_cast<uint64_t>(buff); }
    uint64_t get_tail() const { return reinterpret_cast<uint64_t>(tail.load()); }
    uint64_t get_r_key_buff() const { return r_key_buff; }
    uint64_t get_r_key_tail_copy() const { return r_key_tail_copy; }

private:
  void* buff;
  std::atomic<void*> head{nullptr};  // Points to memory location storing head byte offset
  std::atomic<void*> tail{nullptr};  // Points to memory location storing tail byte offset
  std::uint64_t r_key_tail_copy;
  std::uint64_t r_key_buff;
  node_id_t send_node{};
  std::string send_udl{};
  std::uint64_t ring_size;
  std::uint64_t head_addr;
  std::uint64_t head_r_key;
  ServiceClient<CascadeTypes...>& service_client;
  std::thread receiving_thread;
  std::atomic<bool> stop_flag{false};
  
  // Subscriber state
  SubscriptionMode subscription_mode;
  bool has_subscriber{false};
  
  // Zero-copy mode state
  ZeroCopyCallback zero_copy_callback;
  std::atomic<bool> buffer_locked{false};
  std::mutex lock_mutex;
  
  // Memory copy mode state  
  MemoryCopyCallback memory_copy_callback;
  void* dest_memory{nullptr};
  size_t memory_size{0};
  
  oob_recv_buffer(void* buff, 
                  void* head, 
                  void* tail, 
                  node_id_t send_node, 
                  std::string send_udl,
                  std::uint64_t ring_size,
                  ServiceClient<CascadeTypes...>& service_client);

  void run_recv();
};

}
}

#include "detail/oob_buffer_impl.hpp"
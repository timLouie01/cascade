#pragma once
#include <thread>
#include <atomic>
#include <functional>
#include <vector>
#include <mutex>
#include <set>

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
          std::uint64_t chunk_size,
          ServiceClient<CascadeTypes...>& service_client);

  ~oob_send_buffer();
  uint64_t get_write_location();
  void setup_connection(uint64_t buffer_addr, uint64_t tail_addr, std::uint64_t buff_r_key, std::uint64_t tail_r_key);

  void advance_tail(size_t bytes_written);
  void start(int cpu_core = -1);  // -1 means no CPU pinning, >=0 pins to that core
  void stop();
  
  // Public getters for ServiceClient access
  uint64_t get_head() const { return reinterpret_cast<uint64_t>(head.load()); }
  void* get_head_actual_ptr() const { return head.load(); }  // For debugging
  uint64_t get_head_r_key() const { return send_head_r_key; }
  
  // Check available space in the ring buffer
  size_t get_available_space();
  
  // Write data to the buffer (moved from ServiceClient)
  void write(uint64_t local_addr, size_t size, bool local_gpu = false);
  
  // Check if data can fit in the buffer (moved from ServiceClient)
  bool can_fit(size_t size);

  /**
   * Returns the number of chunks currently in the buffer
   */
  size_t get_fill_chunks();

  /**
   * Get pointer to current write location for in-place payload creation
   * Use this to avoid memory copy - write directly to the buffer
   */
  void* get_write_pointer();

  /**
   * Manually advance the tail after writing data in-place
   * Call this after writing data using get_write_pointer()
   */
  void advance_tail_manual(size_t bytes_written);

private:
  void* buff {nullptr};
  std::atomic<void*> head{nullptr};  // Points to RDMA-registered memory for remote updates
  std::atomic<uint64_t> head_offset_cache{0};  // Local cache of head value for fast CPU reads
  std::atomic<void*> tail{nullptr};
  std::atomic<void*> send_tail{nullptr};  // New: where app writes new data
  std::uint64_t send_head_r_key{};
  node_id_t recv_node{};
  std::string recv_udl{};
  std::uint64_t ring_size;
  std::uint64_t chunk_size;  // NEW: Store chunk size
  std::uint64_t dest_buffer_addr;
  std::uint64_t dest_tail_addr;
  std::uint64_t dest_buff_r_key{}; 
  std::uint64_t dest_tail_r_key{};
  ServiceClient<CascadeTypes...>& service_client;
  std::thread sending_thread;
  std::atomic<bool> stop_flag{false};
  bool first_iter{true};
  int cpu_core_id{-1};  // CPU core to pin sending thread to (-1 = no pinning)
  uint64_t cached_write_location{0};  // Cached write location for fast access
  oob_send_buffer(void* buff,
                  void* head, 
                  void* tail,
                  node_id_t recv_node,
                  std::string recv_udl,
                  std::uint64_t buff_r_key, 
                  std::uint64_t tail_r_key,
                  std::uint64_t ring_size,
                  std::uint64_t chunk_size,  // NEW: Chunk size parameter
                  ServiceClient<CascadeTypes...>& service_client);
    
  void run_send();
};

template<typename... CascadeTypes>
class oob_recv_buffer {
public:
    // Batch message descriptor for zero-copy processing
    struct MessageDescriptor {
        const void* data_ptr;
        size_t size;
        uint64_t sequence;  // For ordering/tracking
        oob_recv_buffer<CascadeTypes...>* buffer_ptr;  // Which buffer this message came from
    };
    
    // Updated callback types
    using ZeroCopyBatchCallback = std::function<void(const std::vector<MessageDescriptor>& messages)>;
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
           std::uint64_t chunk_size,  // NEW: Programmable chunk size
           ServiceClient<CascadeTypes...>& service_client);

    ~oob_recv_buffer();
    void setup_connection(uint64_t head_addr, std::uint64_t head_r_key);

    // REMOVED: Individual start/stop - now managed by buffer set
    // void start(int cpu_core = -1);
    // void stop();
    
    // Process available data (called by shared thread)
    bool process_once();
    
    // Subscriber interface - two modes
    void set_zero_copy_batch_subscriber(const ZeroCopyBatchCallback& callback);
    void set_memory_copy_subscriber(void* dest_memory, size_t memory_size, const MemoryCopyCallback& callback);
    void clear_subscriber();
    
    // Reset internal counters (call between runs)
    void reset_counters();
    
    // Public getters for ServiceClient access
    uint64_t get_buff() const { return reinterpret_cast<uint64_t>(buff); }
    uint64_t get_tail() const { return reinterpret_cast<uint64_t>(tail.load()); }
    uint64_t get_r_key_buff() const { return r_key_buff; }
    uint64_t get_r_key_tail_copy() const { return r_key_tail_copy; }
    
    // NEW: Public member for accessing sender node ID
    const node_id_t send_node;

    // Age tracking for load balancing
    std::chrono::steady_clock::time_point get_oldest_message_time() const {
        return oldest_message_arrival_time.load();
    }
    
    uint64_t get_messages_waiting() const {
        volatile uint64_t* rdma_head_ptr = reinterpret_cast<volatile uint64_t*>(head.load());
        volatile uint64_t* rdma_tail_ptr = reinterpret_cast<volatile uint64_t*>(tail.load());
        
        if (*rdma_tail_ptr >= *rdma_head_ptr) {
            return (*rdma_tail_ptr - *rdma_head_ptr) / chunk_size;
        } else {
            return (ring_size - *rdma_head_ptr + *rdma_tail_ptr) / chunk_size;
        }
    }

private:
  void* buff;
  std::atomic<void*> head{nullptr};
  std::atomic<void*> tail{nullptr};
  std::uint64_t r_key_tail_copy;
  std::uint64_t r_key_buff;
  std::string send_udl{};
  std::uint64_t ring_size;
  std::uint64_t chunk_size;  // NEW: Store chunk size
  std::uint64_t head_addr;
  std::uint64_t head_r_key;
  ServiceClient<CascadeTypes...>& service_client;
  
  // REMOVED: Individual thread management
  // std::thread receiving_thread;
  // std::atomic<bool> stop_flag{false};
  // int cpu_core_id{-1};
  
  // Subscriber state
  SubscriptionMode subscription_mode;
  bool has_subscriber{false};
  
  // Zero-copy mode state
  ZeroCopyBatchCallback zero_copy_batch_callback;
  std::atomic<bool> buffer_locked{false};
  std::mutex lock_mutex;
  
  // Memory copy mode state  
  MemoryCopyCallback memory_copy_callback;
  void* dest_memory{nullptr};
  size_t memory_size{0};
 
  // Counters that can be reset between runs
  std::atomic<uint64_t> total_chunks_received{0};
  const uint64_t expected_total_chunks = 2*10000;

  // Age tracking for load balancing (atomic for thread-safe access)
  std::atomic<std::chrono::steady_clock::time_point> oldest_message_arrival_time{std::chrono::steady_clock::now()};
  
  oob_recv_buffer(void* buff, 
                  void* head, 
                  void* tail, 
                  node_id_t send_node, 
                  std::string send_udl,
                  std::uint64_t ring_size,
                  std::uint64_t chunk_size,  // NEW: Chunk size parameter
                  ServiceClient<CascadeTypes...>& service_client);
};

// NEW: Shared recv buffer set manager
template<typename... CascadeTypes>
class oob_recv_buffer_set {
public:
    // Load balancing strategies
    enum class LoadBalancingStrategy {
        FAST_AS_ABLE,      // Check all buffers as fast as possible (current behavior)
        ROUND_ROBIN,       // Check each buffer once per round
        AGE_FAIRNESS       // Prioritize buffers with oldest unprocessed messages
    };
    
    static oob_recv_buffer_set& get_instance();
    
    void add_buffer(oob_recv_buffer<CascadeTypes...>* buffer);
    void remove_buffer(oob_recv_buffer<CascadeTypes...>* buffer);
    
    void start(int cpu_core = -1, LoadBalancingStrategy strategy = LoadBalancingStrategy::FAST_AS_ABLE);
    void stop();
    
    // Change load balancing strategy at runtime
    void set_load_balancing_strategy(LoadBalancingStrategy strategy);
    
private:
    oob_recv_buffer_set() = default;
    
    std::set<oob_recv_buffer<CascadeTypes...>*> buffers;
    std::mutex buffers_mutex;
    
    std::thread recv_thread;
    std::atomic<bool> stop_flag{false};
    int cpu_core_id{-1};
    
    // Load balancing state
    LoadBalancingStrategy lb_strategy{LoadBalancingStrategy::FAST_AS_ABLE};
    std::atomic<LoadBalancingStrategy> runtime_lb_strategy{LoadBalancingStrategy::FAST_AS_ABLE};
    
    // Round-robin state
    size_t round_robin_index{0};
    
    // Age tracking state (for AGE_FAIRNESS)
    struct BufferAgeInfo {
        oob_recv_buffer<CascadeTypes...>* buffer;
        std::chrono::steady_clock::time_point oldest_message_time;
        uint64_t messages_waiting;
        
        // For priority queue - older messages have higher priority
        bool operator<(const BufferAgeInfo& other) const {
            return oldest_message_time > other.oldest_message_time; // Reverse for min-heap of oldest
        }
    };
    
    void run_recv_loop();
    void run_recv_loop_fast_as_able();
    void run_recv_loop_round_robin();
    void run_recv_loop_age_fairness();
};

}
}

#include "detail/oob_buffer_impl.hpp"
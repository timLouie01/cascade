#include <atomic>

namespace derecho {

using node_id_t = uint32_t;

namespace cascade {

template<typename... CascadeTypes>
class ServiceClient;

template<typename... CascadeTypes>
class oob_send_buffer {
public:
  static std::unique_ptr<oob_send_buffer>
  create(std::uint64_t buff,
          std::uint64_t head,
          std::uint64_t tail,
          node_id_t      recv_node,
          std::string    recv_udl,
          std::uint64_t ring_size,
          ServiceClient<CascadeTypes...>& service_client);
  // oob_send_buffer(const oob_send_buffer&) = delete;
  // oob_send_buffer& operator=(const oob_send_buffer&) = delete;

  // oob_send_buffer(oob_send_buffer&&) noexcept;
  // oob_send_buffer& operator=(oob_send_buffer&&) noexcept;

  ~oob_send_buffer();
  uint64_t get_write_location();
  void setup_connection(uint64_t buffer_addr, uint64_t tail_addr, std::uint64_t buff_r_key, std::uint64_t tail_r_key);

  void advance_tail(size_t bytes_written);
  void start();
  void stop();
private:
  uint64_t buff;
  std::atomic<uint64_t> head{0};
  std::atomic<uint64_t> tail{0};
  std::uint64_t r_key_head_copy;
  node_id_t recv_node{};
  std::string recv_udl{};
  std::uint64_t local_head;
  std::uint64_t local_tail;
  std::uint64_t ring_size;
  std::uint64_t buffer_addr;
  std::uint64_t tail_addr;
  std::uint64_t buff_r_key{}; 
  std::uint64_t tail_r_key{};
  ServiceClient<CascadeTypes...>& service_client;
  std::thread sending_thread;
  std::atomic<bool> stop_flag{false};
  oob_send_buffer(uint64_t buff,
                  std::uint64_t head, 
                  std::uint64_t tail,
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
    static std::unique_ptr<oob_recv_buffer>
    create(std::uint64_t buff,
           std::uint64_t head,
           std::uint64_t tail,
           node_id_t      send_node,
           std::string    send_udl,
           std::uint64_t ring_size,
           ServiceClient<CascadeTypes...>& service_client);

    ~oob_recv_buffer();
    void setup_connection(uint64_t head_addr, std::uint64_t head_r_key);

    // oob_recv_buffer(const oob_recv_buffer&)            = delete;
    // oob_recv_buffer& operator=(const oob_recv_buffer&) = delete;
    // oob_recv_buffer(oob_recv_buffer&&)                 = delete;
    // oob_recv_buffer& operator=(oob_recv_buffer&&)      = delete;

    void start();
    void stop();

private:
  uint64_t buff;
  std::atomic<uint64_t> head{0};
  std::atomic<uint64_t> tail{0};
  std::uint64_t r_key_tail_copy;
  std::uint64_t r_key_buff;
  node_id_t send_node{};
  std::string send_udl{};
  std::uint64_t  head_addr;
  std::uint64_t  head_r_key;
  ServiceClient<CascadeTypes...>& service_client;
  std::thread receiving_thread;
  std::atomic<bool> stop_flag{false};
  oob_recv_buffer(uint64_t buff,
                  std::uint64_t head, 
                  std::uint64_t tail,
                  node_id_t send_node,
                  std::string send_udl,
                  ServiceClient<CascadeTypes...>& service_client);
    
  void run_recv();
  };

} // namespace cascade
} // namespace derecho

#include "detail/oob_buffer_impl.hpp"

#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <cascade/utils.hpp>
#include <memory>
#include <sys/mman.h>
#include <thread>
#include <chrono>
#include <numa.h>
#include <string>
#include <immintrin.h>
#include <cstring>
#include <atomic>

#ifndef LOG_OOBWRITE_RECV
#define LOG_OOBWRITE_RECV 7006
#endif
#ifndef LOG_OOBWRITE_SEND
#define LOG_OOBWRITE_SEND 7005
#endif
 
namespace derecho{
namespace cascade{

#define MY_UUID     "48e60f7c-8500-11eb-8755-0242ac110008"
#define MY_DESC     "Demo DLL UDL that demonstrates utilizing Out Of Band Fast Path"

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class OOBOCDPO: public OffCriticalDataPathObserver {
private:
    // Type aliases for ServiceClient structs
    using ServiceClientType = ServiceClient<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>;
    using Buffer = ServiceClientType::Buffer;
    using Tail = ServiceClientType::Tail;
    using Head = ServiceClientType::Head;
    
    // State: OOB Buffer pointers
    std::unique_ptr<oob_send_buffer<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>> send_buf;
    std::unique_ptr<oob_recv_buffer<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>> recv_buf;
    
    // Receiver tracking state (moved from static locals to member variables)
    std::atomic<int> received_count{0};
    std::chrono::high_resolution_clock::time_point recv_start_time;
    static constexpr int expected_messages = 2*10000;
    bool recv_timer_started{false};
    
    // Store client pointer for callbacks (avoid dangling references)
    ServiceClientType* client_ptr = nullptr;
    
    // Configuration for sending data
    uint32_t sleep_time_us = 0; // Sleep time between consecutive writes
    
    // Data structure for sending meaningful data
    struct TestData {
        uint64_t sequence_number;
        char message[16376];  // 16384 - 8 = 16376 bytes to exactly match 16KB
    };
    
    // Connection payload using POD structs (no std::optional)
    struct ConnectionPayload {
        Buffer buffer_info;
        Tail tail_info;
        Head head_info;
        uint32_t dest_node;
        uint8_t has_buffer;    // 1 if buffer_info is valid, 0 otherwise
        uint8_t has_tail;      // 1 if tail_info is valid, 0 otherwise  
        uint8_t has_head;      // 1 if head_info is valid, 0 otherwise
        uint8_t padding;       // For alignment
    };
    
    // Batch processing state
    std::atomic<uint64_t> total_messages_processed{0};
    std::atomic<uint64_t> total_batches_processed{0};

public:
    virtual void operator () (const derecho::node_id_t sender,
                              const std::string& key_string,
                              const uint32_t prefix_length,
                              persistent::version_t version,
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>& outputs,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
        
        auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
        auto& client = typed_ctxt->get_service_client_ref();
        
        std::cout << "[OOB_FAST_PATH] Node " << client.get_my_id() 
                  << " received trigger: " << key_string << std::endl;
        
        auto tokens = str_tokenizer(key_string);
        
        if (tokens.size() < 2) {
            std::cout << "[ERROR] Invalid key format. Expected: oob/<command>" << std::endl;
            return;
        }
        
        if (tokens[1] == "prepare_send") {
            // Sender - prepare to send data
            const uint64_t ring_size = 64 * 1024; // 64KB ring buffer
            const uint64_t chunk_size = 16 * 1024; // NEW: 8KB programmable chunk size (was 5KB)
            uint32_t dest_node = 1; // static destination node
            
            // Extract sleep time from payload
            uint32_t sleep_time_us = 0; // default no sleep
            
            if (value_ptr) {
                const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
                if (object && object->blob.size > 0) {
                    // Parse as string: just the sleep time in microseconds
                    std::string payload_str(reinterpret_cast<const char*>(object->blob.bytes), object->blob.size);
                    sleep_time_us = std::stoul(payload_str);
                }
            }
            
            std::cout << "[PREPARE_SEND] Creating OOB send buffer for node " << dest_node 
                      << " with sleep time " << sleep_time_us << "us (creation pinned to core 10 for NUMA first-touch)" << std::endl;
            
            // Store sleep time for later use in sending and logging
            this->sleep_time_us = sleep_time_us;
            
            try {
                // Create send buffer in thread pinned to core 10 (same as run_send)
                // This ensures NUMA first-touch on the correct node
                std::thread create_thread([this, &client, dest_node, ring_size]() {
                    // Pin to core 10
                    cpu_set_t set;
                    CPU_ZERO(&set);
                    CPU_SET(10, &set);
                    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
                    
                    std::cout << "[PREPARE_SEND] Buffer creation thread pinned to core 10" << std::endl;
                    
                    // Create the buffer with programmable chunk size (this does allocation, mlock, and page warming)
                    send_buf = client.oob_send_buff_create(dest_node, MY_DESC, ring_size, chunk_size);
                    
                    std::cout << "[PREPARE_SEND] Send buffer allocated with " << chunk_size << " byte chunks on NUMA node of core 10" << std::endl;
                });
                
                create_thread.join();
                
                if (!send_buf) {
                    std::cout << "[ERROR] Failed to create OOB send buffer!" << std::endl;
                    return;
                }
                
                std::cout << "[PREPARE_SEND] OOB send buffer created successfully" << std::endl;
                
                // Notify the destination node to prepare its receive buffer with sleep time
                uint32_t my_node_id = client.get_my_id();
                std::string sleep_time_str = std::to_string(sleep_time_us);
                Blob dest_blob(reinterpret_cast<const uint8_t*>(sleep_time_str.c_str()), sleep_time_str.length());
                ObjectWithStringKey obj("oob_fp/prepare_recv", dest_blob);
                client.put_and_forget<VolatileCascadeStoreWithStringKey>(obj, 0, dest_node);
                
                std::cout << "[PREPARE_SEND] Notified node " << dest_node << " to prepare receive buffer with sleep time " << sleep_time_us << "us" << std::endl;
                
            } catch (const std::exception& e) {
                std::cout << "[ERROR] Exception in prepare_send: " << e.what() << std::endl;
            }
        }
        else if (tokens[1] == "prepare_recv") {
            // Receiver - prepare to receive data
            const uint64_t ring_size = 64 * 1024; // 64KB ring buffer
            const uint64_t chunk_size = 16 * 1024; // NEW: 8KB programmable chunk size (was 5KB)
            uint32_t send_node = 0; // static sender node
            
            // Extract sleep time from payload
            uint32_t sleep_time_us = 0; // default no sleep
            
            if (value_ptr) {
                const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
                if (object && object->blob.size > 0) {
                    // Parse as string: sleep time in microseconds
                    std::string payload_str(reinterpret_cast<const char*>(object->blob.bytes), object->blob.size);
                    sleep_time_us = std::stoul(payload_str);
                }
            }
            
            // Store sleep time for later use in logging
            this->sleep_time_us = sleep_time_us;
            
            std::cout << "[PREPARE_RECV] Creating OOB recv buffer for node " << send_node 
                      << " with sleep time " << sleep_time_us << "us (creation pinned to core 11 for NUMA first-touch)" << std::endl;
            
            try {
                // Create recv buffer in thread pinned to core 11 (same as run_recv)
                // This ensures NUMA first-touch on the correct node
                std::thread create_thread([this, &client, send_node, ring_size]() {
                    // Pin to core 11
                    cpu_set_t set;
                    CPU_ZERO(&set);
                    CPU_SET(11, &set);
                    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
                    
                    std::cout << "[PREPARE_RECV] Buffer creation thread pinned to core 11" << std::endl;
                    
                    // Create the buffer with programmable chunk size (this does allocation, mlock, and page warming)
                    recv_buf = client.oob_recv_buff_create(send_node, MY_DESC, ring_size, chunk_size);
                    
                    std::cout << "[PREPARE_RECV] Recv buffer allocated with " << chunk_size << " byte chunks on NUMA node of core 11" << std::endl;
                });
                
                create_thread.join();
                
                if (!recv_buf) {
                    std::cout << "[ERROR] Failed to create OOB recv buffer!" << std::endl;
                    return;
                }
                
                std::cout << "[PREPARE_RECV] OOB recv buffer created successfully" << std::endl;
                
                auto recv_info = client.oob_recv_get_info(recv_buf);
                
                // Extract the structured info
                Buffer buffer_info = recv_info.first;
                Tail tail_info = recv_info.second;
                
                std::cout << "[PREPARE_RECV] Buffer - addr=0x" << std::hex << buffer_info.buffer 
                          << ", rkey=0x" << buffer_info.buffer_rkey << std::dec << std::endl;
                std::cout << "[PREPARE_RECV] Tail - addr=0x" << std::hex << tail_info.tail 
                          << ", rkey=0x" << tail_info.tail_rkey << std::dec << std::endl;
                
                uint32_t my_node_id = client.get_my_id();
                ConnectionPayload payload{};
                payload.buffer_info = buffer_info;
                payload.tail_info = tail_info;
                payload.dest_node = my_node_id;
                payload.has_buffer = 1;
                payload.has_tail = 1;
                payload.has_head = 0;
                
                Blob blob(reinterpret_cast<const uint8_t*>(&payload), sizeof(payload));
                ObjectWithStringKey obj("oob_fp/send_connect", blob);
                client.put_and_forget<VolatileCascadeStoreWithStringKey>(obj, 0, send_node);
                
                std::cout << "[PREPARE_RECV] Sent connection info back to sender node " << send_node << std::endl;
                
            } catch (const std::exception& e) {
                std::cout << "[ERROR] Exception in prepare_recv: " << e.what() << std::endl;
            }
        }
        else if (tokens[1] == "send_connect") {
            // Connect the buffers and start communication
            if (!value_ptr) {
                std::cout << "[ERROR] No connection payload provided!" << std::endl;
                return;
            }
            
            const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
            if (!object || object->blob.size < sizeof(ConnectionPayload)) {
                std::cout << "[ERROR] Invalid connection payload!" << std::endl;
                return;
            }
            
            const ConnectionPayload payload = *reinterpret_cast<const ConnectionPayload*>(object->blob.bytes);
            
            std::cout << "[CONNECT] Setting up connection with node " << payload.dest_node << std::endl;
            std::cout << "[CONNECT] Buffer: addr=0x" << std::hex << payload.buffer_info.buffer 
                      << ", rkey=0x" << payload.buffer_info.buffer_rkey << std::dec << std::endl;
            std::cout << "[CONNECT] Tail: addr=0x" << std::hex << payload.tail_info.tail 
                      << ", rkey=0x" << payload.tail_info.tail_rkey << std::dec << std::endl;
            
            if (!send_buf) {
                std::cout << "[ERROR] No send buffer to connect!, Make sure to call oob_send_buff_create before calling oob_send_connect" << std::endl;
                return;
            }
            
            try {
                // // Setup connection for send buffer using the structured data
                // client.oob_send_connect(send_buf, 
                //                       payload.buffer_info.buffer, payload.tail_info.tail, 
                //                       payload.buffer_info.buffer_rkey, payload.tail_info.tail_rkey);
                // std::cout << "[CONNECT] Send buffer connected" << std::endl;
                
                // DON'T start the RDMA thread yet! Wait for receiver to be ready
                // Setup connection for send buffer using the structured data
                client.oob_send_connect(send_buf, 
                                      payload.buffer_info.buffer, payload.tail_info.tail, 
                                      payload.buffer_info.buffer_rkey, payload.tail_info.tail_rkey);
                std::cout << "[CONNECT] Send buffer connected (RDMA thread NOT started yet)" << std::endl;
                
                // Get head info to send to receiver
                auto send_info = client.oob_send_get_info(send_buf);
                Head head_info = send_info;
                
                // DEBUG: Print what we're sending
                std::cout << "[SENDER_PREP] My actual head pointer address that I'm reading from: 0x" 
                          << std::hex << reinterpret_cast<uint64_t>(send_buf->get_head_actual_ptr()) << std::dec << std::endl;
                std::cout << "[SENDER_PREP] Sending to receiver head address: 0x" 
                          << std::hex << head_info.head << ", rkey: 0x" << head_info.head_rkey << std::dec << std::endl;
                uint32_t my_node_id = client.get_my_id();
                ConnectionPayload response_payload{};
                response_payload.head_info = head_info;
                response_payload.dest_node = my_node_id;
                response_payload.has_buffer = 0;
                response_payload.has_tail = 0;
                response_payload.has_head = 1;
                Blob response_blob(reinterpret_cast<const uint8_t*>(&response_payload), sizeof(response_payload));
                ObjectWithStringKey response_obj("oob_fp/start_recv", response_blob);
                client.put_and_forget<VolatileCascadeStoreWithStringKey>(response_obj, 0, payload.dest_node);
                
                std::cout << "[CONNECT] Notified receiver to start with head info: addr=0x" 
                          << std::hex << head_info.head << ", rkey=0x" << head_info.head_rkey << std::dec << std::endl;
                std::cout << "[CONNECT] WAITING for receiver acknowledgment before starting RDMA..." << std::endl;
                
            } catch (const std::exception& e) {
                std::cout << "[ERROR] Exception in connect: " << e.what() << std::endl;
            }
        }
        else if (tokens[1] == "start_recv") {
            // Start the receive buffer and begin receiving
            if (!recv_buf) {
                std::cout << "[ERROR] No recv buffer to start!" << std::endl;
                return;
            }
            
            // Extract head info from the payload
            if (!value_ptr) {
                std::cout << "[ERROR] No head info payload provided!" << std::endl;
                return;
            }
            
            const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
            if (!object || object->blob.size < sizeof(ConnectionPayload)) {
                std::cout << "[ERROR] Invalid head info payload!" << std::endl;
                return;
            }
            
            const ConnectionPayload payload = *reinterpret_cast<const ConnectionPayload*>(object->blob.bytes);
            
            if (!payload.has_head) {
                std::cout << "[ERROR] No head info in payload!" << std::endl;
                return;
            }
            
            std::cout << "[START_RECV] Received head info from node " << payload.dest_node << std::endl;
            std::cout << "[START_RECV] Head: addr=0x" << std::hex << payload.head_info.head 
                      << ", rkey=0x" << payload.head_info.head_rkey << std::dec << std::endl;
            
            try {
                // Store client pointer for use in callbacks
                client_ptr = &client;
                
                // STEP 1: Connect recv buffer to sender's head (setup RDMA connection)
                client.oob_recv_connect(recv_buf, payload.head_info.head, payload.head_info.head_rkey);
                std::cout << "[START_RECV] Recv buffer connected to sender's head" << std::endl;
                
                // STEP 2: Add buffer to shared buffer set
                // This is where the buffer gets registered for processing
                auto& buffer_set = oob_recv_buffer_set<VolatileCascadeStoreWithStringKey, 
                                                       PersistentCascadeStoreWithStringKey, 
                                                       TriggerCascadeNoStoreWithStringKey>::get_instance();
                buffer_set.add_buffer(recv_buf.get());
                std::cout << "[START_RECV] Added recv buffer to shared buffer set" << std::endl;
                
                // STEP 3: Register zero-copy batch subscriber
                // This callback will be invoked by the shared thread when data arrives
                recv_buf->set_zero_copy_batch_subscriber(
                    [this](const std::vector<typename oob_recv_buffer<VolatileCascadeStoreWithStringKey, 
                                                                      PersistentCascadeStoreWithStringKey, 
                                                                      TriggerCascadeNoStoreWithStringKey>::MessageDescriptor>& messages) {
                        this->process_received_batch_zero_copy(messages);
                    });
                std::cout << "[START_RECV] Registered batch zero-copy subscriber" << std::endl;
                
                // STEP 4: Start shared receive thread (idempotent - safe to call multiple times)
                // Each recv buffer calls this, but only the first call actually starts the thread
                // Available strategies:
                // - FAST_AS_ABLE: Check all buffers as fast as possible (default, lowest latency)
                // - ROUND_ROBIN: Fair round-robin across buffers (predictable, equal opportunity)
                // - AGE_FAIRNESS: Prioritize oldest messages (minimizes max message age)
                using LBStrategy = oob_recv_buffer_set<VolatileCascadeStoreWithStringKey, 
                                                       PersistentCascadeStoreWithStringKey, 
                                                       TriggerCascadeNoStoreWithStringKey>::LoadBalancingStrategy;
                buffer_set.start(11, LBStrategy::FAST_AS_ABLE);  // Change strategy here
                std::cout << "[START_RECV] Called buffer_set.start() (thread-safe, idempotent)" << std::endl;
                
                // STEP 5: Send acknowledgment back to sender to start RDMA thread
                // This completes the handshake - sender will now start sending data
                uint32_t my_node_id = client.get_my_id();
                Blob ack_blob(reinterpret_cast<const uint8_t*>(&my_node_id), sizeof(uint32_t));
                ObjectWithStringKey ack_obj("oob_fp/receiver_ready", ack_blob);
                client.put_and_forget<VolatileCascadeStoreWithStringKey>(ack_obj, 0, payload.dest_node);
                
                std::cout << "[START_RECV] Sent READY acknowledgment to sender node " << payload.dest_node << std::endl;
                std::cout << "[START_RECV] Receiver setup complete and ready to receive data" << std::endl;
                
            } catch (const std::exception& e) {
                std::cout << "[ERROR] Exception in start_recv: " << e.what() << std::endl;
            }
        }
        else if (tokens[1] == "receiver_ready") {
            // Receiver is ready - now start our RDMA thread and application
            std::cout << "[RECEIVER_READY] Receiver acknowledged ready - starting RDMA and application threads" << std::endl;
            
            if (!send_buf) {
                std::cout << "[ERROR] No send buffer available to start!" << std::endl;
                return;
            }
            
            try {
                // NOW start the RDMA thread (receiver is ready)
                client.oob_send_start(send_buf, 10);
                std::cout << "[RECEIVER_READY] Send buffer RDMA thread started on core 10" << std::endl;
                
                // Start sending data in a separate thread with proper yielding
                std::thread([this, &client]() {
                    // Pin to core 12
                    cpu_set_t set;
                    CPU_ZERO(&set);
                    CPU_SET(12, &set);
                    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
                    
                    // Set thread name for debugging
                    pthread_setname_np(pthread_self(), "OOB_SEND_APP");
                    
                    std::cout << "[SEND_APP] Application sender thread pinned to core 12" << std::endl;

                    // Give RDMA thread time to start up properly
                    std::this_thread::sleep_for(std::chrono::milliseconds(200));
                    
                    this->start_sending_data(client);
                }).detach();
                
            } catch (const std::exception& e) {
                std::cout << "[ERROR] Exception in receiver_ready: " << e.what() << std::endl;
            }
        }
        else if (tokens[1] == "restart_send") {
            // Restart sending with new sleep time (reuse existing buffers)
            if (!send_buf) {
                std::cout << "[ERROR] No send buffer available! Must call prepare_send first." << std::endl;
                return;
            }
            
            // Extract new sleep time from payload
            uint32_t new_sleep_time_us = 0; // default no sleep
            
            if (value_ptr) {
                const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
                if (object && object->blob.size > 0) {
                    // Parse as string: sleep time in microseconds
                    std::string payload_str(reinterpret_cast<const char*>(object->blob.bytes), object->blob.size);
                    new_sleep_time_us = std::stoul(payload_str);
                }
            }
            
            // Update sleep time for this run
            this->sleep_time_us = new_sleep_time_us;
            
            std::cout << "[RESTART_SEND] Restarting send operation with sleep time " << new_sleep_time_us << "us" << std::endl;
            
            try {
                // Start sending data directly (buffers already connected)
                std::thread([this, &client]() {
                    // Pin to core 12
                    cpu_set_t set;
                    CPU_ZERO(&set);
                    CPU_SET(12, &set);
                    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
                    
                    // Set thread name for debugging
                    pthread_setname_np(pthread_self(), "OOB_RESTART_SEND");
                    
                    std::cout << "[RESTART_SEND] Application sender thread pinned to core 12" << std::endl;

                    // Small delay to ensure thread setup
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    
                    this->start_sending_data(client);
                }).detach();
                
            } catch (const std::exception& e) {
                std::cout << "[ERROR] Exception in restart_send: " << e.what() << std::endl;
            }
        }
        else {
            std::cout << "[ERROR] Unsupported oob operation: " << tokens[1] << std::endl;
        }
    }

private:
    void start_sending_data(ServiceClient<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>& client) {
        std::cout << "[SEND_THREAD] Starting data transmission..." << std::endl;
        
        const int num_messages = 10000;
        const auto start_time = std::chrono::high_resolution_clock::now();
        
        // For minimum latency: Remove artificial delays, rely on natural backpressure
        // The can_fit() check will naturally pace the sender when buffer fills
        
        for (int i = 0; i < num_messages; ++i) {
            // Apply configured sleep time between consecutive writes if specified
            if (sleep_time_us > 0){
                auto busy_start = std::chrono::high_resolution_clock::now();
                auto target_duration = std::chrono::microseconds(sleep_time_us);
                    while (std::chrono::high_resolution_clock::now() - busy_start < target_duration) {
                        _mm_pause(); // CPU hint for spin-wait loops
                    }
            }
            
            
            try {
                // Wait for space first
                while (!send_buf->can_fit(sizeof(TestData))) {
                    _mm_pause();
                }
                
                // Get pointer for in-place payload creation (ZERO-COPY!)
                TestData* data = reinterpret_cast<TestData*>(send_buf->get_write_pointer());
                
                data->sequence_number = i + 1;
                
                // Safely fill the entire message buffer with a simple repeating pattern
                // Create a simple, consistent pattern that's easy to verify
                const char pattern[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
                size_t pattern_len = sizeof(pattern) - 1; // Exclude null terminator
                
                // Fill buffer with repeating pattern
                for (size_t pos = 0; pos < sizeof(data->message) - 1; ++pos) {
                    data->message[pos] = pattern[pos % pattern_len];
                }
                data->message[sizeof(data->message) - 1] = '\0'; // Null terminate
                
                // Write sequence number at the beginning for identification
                snprintf(data->message, 32, "MSG_%lu:", static_cast<unsigned long>(i + 1));
                 
                // auto busy_start = std::chrono::high_resolution_clock::now();
                // auto target_duration = std::chrono::microseconds(sleep_time_us);

                // Log send timestamp
                TimestampLogger::log(LOG_OOBWRITE_SEND, client.get_my_id(), data->sequence_number);
                
                // Manually advance tail after in-place creation
                send_buf->advance_tail_manual(sizeof(TestData));

                // if (sleep_time_us > 0){
                //     while (std::chrono::high_resolution_clock::now() - busy_start < target_duration) {
                //         _mm_pause(); // CPU hint for spin-wait loops
                //     }
                // }
                
            } catch (const std::exception& e) {
                std::cout << "[ERROR] Exception while sending data at message " << i << ": " << e.what() << std::endl;
                break;
            }
        }
        std::cout << "[SEND_THREAD] Completed sending " << num_messages << std::endl;
        
        // Create filename with sleep time included
        std::string send_filename = "send_oob_fast_path_sleep" + std::to_string(sleep_time_us) + "us_timestamp.dat";
        TimestampLogger::flush(send_filename);
        const int break_ms = 1000;  
        std::this_thread::sleep_for(std::chrono::milliseconds(break_ms));
        std::cout << "[SEND_THREAD] Flushed send timestamps to " << send_filename << std::endl;
        
        // RESET mechanism: Indicate that sender is ready for next run
        std::cout << "[SEND_RESET] Send operation complete, ready for next run" << std::endl;
    }
    
    // New batch processing method
    void process_received_batch_zero_copy(const std::vector<typename oob_recv_buffer<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>::MessageDescriptor>& messages) {
        try {
            // Start timer on first batch
            if (!recv_timer_started) {
                recv_start_time = std::chrono::high_resolution_clock::now();
                recv_timer_started = true;
            }
            
            // Process entire batch efficiently
            for (const auto& msg : messages) {
                if (msg.size >= sizeof(TestData)) {
                    const TestData* test_data = reinterpret_cast<const TestData*>(msg.data_ptr);
                    // Log with sender node ID from the buffer (0 or 1)
                    TimestampLogger::log(LOG_OOBWRITE_RECV, msg.buffer_ptr->send_node, test_data->sequence_number);
                    int count = ++received_count;
                    
                    // NEW: You can now identify which buffer this came from
                    // std::cout << "[RECV-BATCH] Message " << test_data->sequence_number 
                    //           << " from sender node " << msg.buffer_ptr->send_node << std::endl;
                }
            }
            
            total_messages_processed.fetch_add(messages.size());
            total_batches_processed.fetch_add(1);
            
            // Check completion
            int count = received_count.load();
            if (count >= expected_messages) {
                auto end_time = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - recv_start_time);
                
                uint64_t total_batches = total_batches_processed.load();
                double avg_batch_size = static_cast<double>(count) / total_batches;
                
                std::cout << "[RECV-BATCH] Completed receiving " << count 
                          << " messages in " << duration.count() << " ms" << std::endl;
                std::cout << "[RECV-BATCH] Total batches: " << total_batches 
                          << ", Avg batch size: " << avg_batch_size << std::endl;
                
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                
                std::string recv_filename = "recv_oob_fast_path_sleep" + std::to_string(sleep_time_us) + "us_timestamp.dat";
                TimestampLogger::flush(recv_filename);
                std::cout << "[RECV-BATCH] Flushed receive timestamps to " << recv_filename << std::endl;
                
                // Reset for next run
                if (recv_buf) {
                    recv_buf->reset_counters();
                }
                received_count.store(0);
                total_messages_processed.store(0);
                total_batches_processed.store(0);
                recv_timer_started = false;
                std::cout << "[RECV-RESET] Counters reset, ready for next run" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cout << "[ERROR] Exception in process_received_batch_zero_copy: " << e.what() << std::endl;
        }
    }
    
    // Zero-copy lock mode: Direct access with lock/release
    void process_received_data_zero_copy(const void* data, size_t size) {
        try {
            // Start timer on first message
            if (!recv_timer_started) {
                recv_start_time = std::chrono::high_resolution_clock::now();
                recv_timer_started = true;
            }
            
            // Process the received data directly from ring buffer (zero-copy)
            if (size >= sizeof(TestData)) {
                const TestData* test_data = reinterpret_cast<const TestData*>(data);
                
                int count = ++received_count;  // Atomic increment
                
                // Payload Process Timestamp
                // if (client_ptr) {
                //     TimestampLogger::log(LOG_OOBWRITE_RECV, client_ptr->get_my_id(), test_data->sequence_number);
                // }
                
                // Uncomment for detailed logging:
                // std::cout << "[RECV-ZERO-COPY] Received message " << test_data->sequence_number 
                //           << ": " << test_data->message << " (count: " << count << ")" << std::endl;
                
                // Progress updates every 1000 messages
                // if (count % 100 == 0) {
                //     std::cout << "[RECV-ZERO-COPY] Progress: " << count 
                //               << "/" << expected_messages << " messages received" << std::endl;
                // }
                
                // Check if we've received all expected messages
                if (count >= expected_messages) {
                    auto end_time = std::chrono::high_resolution_clock::now();
                    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_time - recv_start_time);
                    
                    std::cout << "[RECV-ZERO-COPY] Completed receiving " << count 
                              << " messages in " << duration.count() << " ms" << std::endl;
                    
                    // Small delay to ensure all log entries are queued
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    
                    // Create filename with sleep time included
                    std::string recv_filename = "recv_oob_fast_path_sleep" + std::to_string(sleep_time_us) + "us_timestamp.dat";
                    TimestampLogger::flush(recv_filename);
                    std::cout << "[RECV-ZERO-COPY] Flushed receive timestamps to " << recv_filename << std::endl;
                    
                    // RESET mechanism: Clear counters and resume timestamp logging for next run
                    if (recv_buf) {
                        recv_buf->reset_counters();
                    }
                    received_count.store(0);
                    recv_timer_started = false;
                    std::cout << "[RECV-RESET] Counters reset, ready for next run" << std::endl;
                }
            } else {
                std::cout << "[RECV-ZERO-COPY] Warning: Received data too small (" << size 
                          << " bytes), expected at least " << sizeof(TestData) << " bytes" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cout << "[ERROR] Exception in process_received_data_zero_copy: " << e.what() << std::endl;
        }
    }
    
    // Memory copy mode: Data copied to our buffer
    void process_received_data_memory_copy(ServiceClient<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>& client, 
                                          const void* data, size_t size) {
        static int received_count = 0;
        static const int expected_messages = 1000;
        
        try {
            // Process the received data
            if (size >= sizeof(TestData)) {
                const TestData* test_data = reinterpret_cast<const TestData*>(data);
                
                received_count++;
                
                // Log receive timestamp
                TimestampLogger::log(LOG_OOBWRITE_RECV, client.get_my_id(), test_data->sequence_number);
                
                std::cout << "[RECV] Received message " << test_data->sequence_number 
                          << ": " << test_data->message << " (size: " << size << ")" << std::endl;
                
                // Every 100th message, print progress
                if (received_count % 100 == 0) {
                    std::cout << "[RECV] Progress: " << received_count 
                              << "/" << expected_messages << " messages received" << std::endl;
                }
                
                // Check if we've received all expected messages
                if (received_count >= expected_messages) {
                    TimestampLogger::flush("recv_oob_fast_path_timestamp.dat");
                    std::cout << "[RECV] Flushed receive timestamps" << std::endl;
                    
                    // Clear subscriber when done
                    recv_buf->clear_subscriber();
                }
            } else {
                std::cout << "[RECV] Warning: Received data too small (" << size 
                          << " bytes), expected at least " << sizeof(TestData) << " bytes" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cout << "[ERROR] Exception in process_received_data: " << e.what() << std::endl;
        }
    }
    
    // Hot-swap methods for switching subscription modes
    void switch_to_batch_zero_copy_mode(ServiceClient<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>& client) {
        std::cout << "[SWITCH] Switching to zero-copy batch mode" << std::endl;
        recv_buf->set_zero_copy_batch_subscriber(
            [this](const std::vector<typename oob_recv_buffer<VolatileCascadeStoreWithStringKey, 
                                                              PersistentCascadeStoreWithStringKey, 
                                                              TriggerCascadeNoStoreWithStringKey>::MessageDescriptor>& messages) {
                this->process_received_batch_zero_copy(messages);
            }
        );
    }
    
    void switch_to_memory_copy_mode(ServiceClient<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>& client) {
        std::cout << "[SWITCH] Switching to memory copy mode" << std::endl;
        
        // Allocate memory buffer for copying (should be at least as large as max message)
        static std::vector<uint8_t> copy_buffer(64 * 1024); // 64KB buffer
        
        recv_buf->set_memory_copy_subscriber(
            copy_buffer.data(), 
            copy_buffer.size(),
            [this, &client](const void* data, size_t size) {
                this->process_received_data_memory_copy(client, data, size);
            });
    }
    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<OOBOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> OOBOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    OOBOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext*,const nlohmann::json&) {
    return OOBOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho

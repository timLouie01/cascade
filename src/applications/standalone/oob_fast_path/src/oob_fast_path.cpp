#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <cascade/utils.hpp>
#include <memory>
#include <sys/mman.h>
#include <thread>
#include <chrono>
#include <numa.h>
#include <optional>
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
    
    // Data structure for sending meaningful data
    struct TestData {
        uint64_t sequence_number;
        char message[5112];
    };
    
    // Connection payload using the service-defined structs
    struct ConnectionPayload {
        std::optional<Buffer> buffer_info;  
        std::optional<Tail> tail_info;      
        std::optional<Head> head_info;      
        uint32_t dest_node;
    };
    

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
			// init wrong node
            uint32_t dest_node = 0;
            

            if (value_ptr) {
                const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
                if (object && object->blob.size >= sizeof(uint32_t)) {
                    std::memcpy(&dest_node, object->blob.bytes, sizeof(uint32_t));
                }
            }
            
            
            std::cout << "[PREPARE_SEND] Creating OOB send buffer for node " << dest_node << std::endl;
            
            try {
                send_buf = client.oob_send_buff_create(dest_node, MY_DESC, ring_size);
                
                if (!send_buf) {
                    std::cout << "[ERROR] Failed to create OOB send buffer!" << std::endl;
                    return;
                }
                
                std::cout << "[PREPARE_SEND] OOB send buffer created successfully" << std::endl;
                
                // Notify the destination node to prepare its receive buffer
                uint32_t my_node_id = client.get_my_id();
                Blob dest_blob(reinterpret_cast<const uint8_t*>(&my_node_id), sizeof(uint32_t));
                ObjectWithStringKey obj("oob/prepare_recv", dest_blob);
                client.put_and_forget<VolatileCascadeStoreWithStringKey>(obj, 0, dest_node);
                
                std::cout << "[PREPARE_SEND] Notified node " << dest_node << " to prepare receive buffer" << std::endl;
                
            } catch (const std::exception& e) {
                std::cout << "[ERROR] Exception in prepare_send: " << e.what() << std::endl;
            }
        }
        else if (tokens[1] == "prepare_recv") {
            // Receiver - prepare to receive data
            const uint64_t ring_size = 64 * 1024; // 64KB ring buffer
            uint32_t send_node = 1;
            
            // Extract sender node from the payload
            if (value_ptr) {
                const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
                if (object && object->blob.size >= sizeof(uint32_t)) {
                    std::memcpy(&send_node, object->blob.bytes, sizeof(uint32_t));
                }
            }
            
            std::cout << "[PREPARE_RECV] Creating OOB recv buffer for node " << send_node << std::endl;
            
            try {
                recv_buf = client.oob_recv_buff_create(send_node, MY_DESC, ring_size);
                
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
                ConnectionPayload payload{
                    buffer_info,
                    tail_info,      
                    std::nullopt,      
                    my_node_id     
                };
                
                Blob blob(reinterpret_cast<const uint8_t*>(&payload), sizeof(payload));
                ObjectWithStringKey obj("oob/send_connect", blob);
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
            std::cout << "[CONNECT] Buffer: addr=0x" << std::hex << payload.buffer_info->buffer 
                      << ", rkey=0x" << payload.buffer_info->buffer_rkey << std::dec << std::endl;
            std::cout << "[CONNECT] Tail: addr=0x" << std::hex << payload.tail_info->tail 
                      << ", rkey=0x" << payload.tail_info->tail_rkey << std::dec << std::endl;
            
            if (!send_buf) {
                std::cout << "[ERROR] No send buffer to connect!, Make sure to call oob_send_buff_create before calling oob_send_connect" << std::endl;
                return;
            }
            
            try {
                // Setup connection for send buffer using the structured data
                client.oob_send_connect(send_buf, 
                                      payload.buffer_info->buffer, payload.tail_info->tail, 
                                      payload.buffer_info->buffer_rkey, payload.tail_info->tail_rkey);
                std::cout << "[CONNECT] Send buffer connected" << std::endl;
                
                // Start the send buffer
                client.oob_send_start(send_buf);
                std::cout << "[CONNECT] Send buffer started" << std::endl;
                
                // Notify receiver to connect
                auto send_info = client.oob_send_get_info(send_buf);
                Head head_info = send_info;
                uint32_t my_node_id = client.get_my_id();
                ConnectionPayload response_payload{
                    std::nullopt,
                    std::nullopt,    
                    head_info,      
                    my_node_id      
                };
                Blob response_blob(reinterpret_cast<const uint8_t*>(&response_payload), sizeof(response_payload));
                ObjectWithStringKey response_obj("oob/start_recv", response_blob);
                client.put_and_forget<VolatileCascadeStoreWithStringKey>(response_obj, 0, payload.dest_node);
                
                std::cout << "[CONNECT] Notified receiver to start with head info: addr=0x" 
                          << std::hex << head_info.head << ", rkey=0x" << head_info.head_rkey << std::dec << std::endl;
                
                // Start sending data in a separate thread
                std::thread([this, &client]() {
                    cpu_set_t set;
                    CPU_ZERO(&set);
                    CPU_SET(9, &set);
                    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
                    this->start_sending_data(client);
                }).detach();
                
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
            
            if (!payload.head_info.has_value()) {
                std::cout << "[ERROR] No head info in payload!" << std::endl;
                return;
            }
            
            std::cout << "[START_RECV] Received head info from node " << payload.dest_node << std::endl;
            std::cout << "[START_RECV] Head: addr=0x" << std::hex << payload.head_info->head 
                      << ", rkey=0x" << payload.head_info->head_rkey << std::dec << std::endl;
            
            try {
                // Connect using the head info from sender
                client.oob_recv_connect(recv_buf, payload.head_info->head, payload.head_info->head_rkey);
                std::cout << "[START_RECV] Recv buffer connected to sender's head" << std::endl;
                
                // Start the recv buffer
                client.oob_recv_start(recv_buf);
                std::cout << "[START_RECV] Recv buffer started" << std::endl;
                
                // Register zero-copy lock subscriber for data processing
                recv_buf->set_zero_copy_subscriber(
                    [this, &client](const void* data, size_t size, std::function<void()> release_func) {
                        this->process_received_data_zero_copy(client, data, size, release_func);
                    });
                std::cout << "[START_RECV] Registered zero-copy lock subscriber" << std::endl;
                
            } catch (const std::exception& e) {
                std::cout << "[ERROR] Exception in start_recv: " << e.what() << std::endl;
            }
        }
        else {
            std::cout << "[ERROR] Unsupported oob operation: " << tokens[1] << std::endl;
        }
    }

private:
    void start_sending_data(ServiceClient<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>& client) {
        std::cout << "[SEND_THREAD] Starting data transmission..." << std::endl;
        
        const int num_messages = 50000;
        const auto start_time = std::chrono::high_resolution_clock::now();
        
        for (int i = 0; i < num_messages; ++i) {
            if (send_buf->can_fit(sizeof(TestData))){
            try {
                
                // Create test data
                TestData data;
                data.sequence_number = i + 1;
                std::string msg = "| I am data element " + std::to_string(i + 1) + " |";
                std::strncpy(data.message, msg.c_str(), sizeof(data.message) - 1);
                data.message[sizeof(data.message) - 1] = '\0'; // Ensure null termination
                
                // Log send timestamp
                TimestampLogger::log(LOG_OOBWRITE_SEND, client.get_my_id(), data.sequence_number);
                
                send_buf->write(reinterpret_cast<uint64_t>(&data), sizeof(TestData), false);
                
                std::cout << "[SEND] Sent message " << data.sequence_number 
                          << ": " << data.message << std::endl;
                
            } catch (const std::exception& e) {
                std::cout << "[ERROR] Exception while sending data: " << e.what() << std::endl;
                break;
            }
        }
    }
        std::cout << "[SEND_THREAD] Completed sending " << num_messages << std::endl;
        
        TimestampLogger::flush("send_oob_fast_path_timestamp.dat");
        std::cout << "[SEND_THREAD] Flushed send timestamps" << std::endl;
    }
    
    // Zero-copy lock mode: Direct access with lock/release
    void process_received_data_zero_copy(ServiceClient<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>& client, 
                                         const void* data, size_t size, std::function<void()> release_func) {
        static int received_count = 0;
        static auto start_time = std::chrono::high_resolution_clock::now();
        static const int expected_messages = 50000;
        
        try {
            // Process the received data directly from ring buffer (zero-copy)
            if (size >= sizeof(TestData)) {
                const TestData* test_data = reinterpret_cast<const TestData*>(data);
                
                received_count++;
                
                TimestampLogger::log(LOG_OOBWRITE_RECV, client.get_my_id(), test_data->sequence_number);
                
                std::cout << "[RECV-ZERO-COPY] Received message " << test_data->sequence_number 
                          << ": " << test_data->message << " (size: " << size << ")" << std::endl;
                
                if (received_count % 100 == 0) {
                    std::cout << "[RECV-ZERO-COPY] Progress: " << received_count 
                              << "/" << expected_messages << " messages received" << std::endl;
                }
                
                // Check if we've received all expected messages
                if (received_count >= expected_messages) {
                    auto end_time = std::chrono::high_resolution_clock::now();
                    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
                    
                    std::cout << "[RECV-ZERO-COPY] Completed receiving " << received_count 
                              << " messages in " << duration.count() << " ms" << std::endl;
                    
                    TimestampLogger::flush("recv_oob_fast_path_timestamp.dat");
                    std::cout << "[RECV-ZERO-COPY] Flushed receive timestamps" << std::endl;
                }
            } else {
                std::cout << "[RECV-ZERO-COPY] Warning: Received data too small (" << size 
                          << " bytes), expected at least " << sizeof(TestData) << " bytes" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cout << "[ERROR] Exception in process_received_data_zero_copy: " << e.what() << std::endl;
        }
        
        // CRITICAL: Must release the lock when done processing!
        release_func();
    }
    
    // Memory copy mode: Data copied to our buffer
    void process_received_data_memory_copy(ServiceClient<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>& client, 
                                          const void* data, size_t size) {
        static int received_count = 0;
        static const int expected_messages = 50000;
        
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
    void switch_to_zero_copy_mode(ServiceClient<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>& client) {
        std::cout << "[SWITCH] Switching to zero-copy lock mode" << std::endl;
        recv_buf->set_zero_copy_subscriber(
            [this, &client](const void* data, size_t size, std::function<void()> release_func) {
                this->process_received_data_zero_copy(client, data, size, release_func);
            });
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

#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <cascade/utils.hpp>
#include <memory>
#include <sys/mman.h>
#include <thread>
#include <chrono>
#include <immintrin.h>
#include <pthread.h>   // pinning
#include <sched.h>
#ifndef LOG_OOBWRITE_RECV
#define LOG_OOBWRITE_RECV 7006
#endif
#ifndef LOG_OOBWRITE_SEND
#define LOG_OOBWRITE_SEND 7005
#endif
 
namespace derecho{
namespace cascade{

#define MY_UUID     "48e60f7c-8500-11eb-8755-0242ac110002"
#define MY_DESC     "Demo DLL UDL that allocates CPU memory and performs Single Sided RDMA"

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class OOBOCDPO: public OffCriticalDataPathObserver {
	/**
		State
	 */ 
	void* buff_mr_ptr = nullptr;
	void* flag_mr_ptr = nullptr;
	uint64_t buff_size = 0;
	
	/**
		Payload
	*/
	struct Payload {
	  uint64_t data_addr;
	  uint64_t data_rkey;
	  uint64_t flag_addr;
	  uint64_t flag_rkey;
	  uint32_t dest; // node id that owns the remote MRs
  };

	// helper functions
	static constexpr size_t CACHELINE = 64;

	static inline void warm_and_lock(void* p, size_t len, char data) {
    	(void) mlock(p, len);
    	constexpr size_t PAGE = 4096;
   		volatile char* c = reinterpret_cast<volatile char*>(p);
   		for (size_t off = 0; off < len; off += PAGE) {
        c[off] = data;
    	}
			if (len % PAGE) c[len-1] = data;
		}
	static inline void warm_and_lock_send(void* p, size_t len) {
    	warm_and_lock(p,len,'a');
	}

	static inline void warm_and_lock_receive(void* p, size_t len) {
    	warm_and_lock(p,len,'b');
	}
	// Note the Size needs to be multiple of alignment for aligned_alloc
	template <typename ClientRef>
  	static inline void* alloc_warm_register(ClientRef& client,
                                            size_t num_bytes,
                                            bool is_sender_side,
                                            bool for_flag, 
																						bool gpu) {
        const size_t align = CACHELINE;
        void* p = aligned_alloc(align, num_bytes);
        if (!p) throw std::bad_alloc();
        if (is_sender_side){
					warm_and_lock_send(p, num_bytes);
				}
        else{
					warm_and_lock_receive(p, num_bytes);
				} 
				derecho::memory_attribute_t attr;
				if (gpu){
					attr.type = derecho::memory_attribute_t::CUDA;
				}else{     
        	attr.type = derecho::memory_attribute_t::SYSTEM;
				}
        client.oob_register_mem_ex(p, num_bytes, attr);

        // For a flag, ensure it's initialized to 0
        if (for_flag) {
            *static_cast<std::uint64_t*>(p) = 0ull;
        }
        return p;
    }

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

	  // std::cout << "[OOB]: I am node "
    //               << client.get_my_id()
    //               << " and I received an object from sender:" << sender
    //               << " with key=" << key_string
    //               << ", matching prefix=" << key_string.substr(0, prefix_length)
    //               << std::endl;

    auto tokens = str_tokenizer(key_string);
    
		if (tokens[1] == "send"){

			// Sender
      const size_t MiB = 1024ull * 1024ull;
      buff_size = 5120ull;

      // buffer: 1 MiB
    	buff_mr_ptr = alloc_warm_register(client, buff_size,true,false,false);
      flag_mr_ptr = alloc_warm_register(client, CACHELINE, true, true,false);

    	// Notify peer to set up
      Blob empty_blob;
      ObjectWithStringKey obj("oob/receive", empty_blob);
      client.put_and_forget<VolatileCascadeStoreWithStringKey>(obj, 0, 1);
    }
    else if(tokens[1] == "receive"){
		// Receiver
    	const size_t MiB = 1024ull * 1024ull;
      buff_size = 5120ull;

      buff_mr_ptr = alloc_warm_register(client, buff_size, false, false,false);
      flag_mr_ptr = alloc_warm_register(client, CACHELINE, false, true,false);

      const uint64_t data_addr = reinterpret_cast<uint64_t>(buff_mr_ptr);
      const uint64_t data_rkey = client.oob_rkey(buff_mr_ptr);

      const uint64_t flag_addr = reinterpret_cast<uint64_t>(flag_mr_ptr);
      const uint64_t flag_rkey = client.oob_rkey(flag_mr_ptr);

      const uint32_t dest = client.get_my_id();

            // std::cout << "DATA rkey=" << data_rkey << " @ " << data_addr << std::endl;
            // std::cout << "FLAG rkey=" << flag_rkey << " @ " << flag_addr << " (initialized to 0)" << std::endl;

      Payload payload{data_addr, data_rkey, flag_addr, flag_rkey, dest};
      Blob blob(reinterpret_cast<const uint8_t*>(&payload), sizeof(payload));
    	ObjectWithStringKey obj("oob/oob_write", blob);
      client.put_and_forget<VolatileCascadeStoreWithStringKey>(obj, 0, 0);

      // Poll the local flag until dist_size reached
      volatile std::uint64_t* flag64_ptr = static_cast<std::uint64_t*>(flag_mr_ptr);
      int my_node_id = client.get_my_id();
      const int dist_size = 2'500;

      std::thread([flag64_ptr, my_node_id, dist_size]{
				cpu_set_t set; CPU_ZERO(&set); CPU_SET(5, &set);                // <-- choose the core
 				pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
				pthread_setname_np(pthread_self(), "oob_polling_loop");   
				uint64_t consume_flag = 0;
				while (consume_flag < dist_size){
					// std::uint64_t current_flag = __atomic_load_n(flag64_ptr, __ATOMIC_ACQUIRE);
        	if (*flag64_ptr > consume_flag){
						TimestampLogger::log(LOG_OOBWRITE_RECV, my_node_id, *flag64_ptr);
						consume_flag = static_cast<uint64_t>(*flag64_ptr);
					}
        } 
          TimestampLogger::flush("recv_oobwrite_timestamp.dat");
          std::cout << "Flushed logs to recv_oobwrite_timestamp.dat" << std::endl;
			}).detach();

		}
    else if (tokens[1] == "oob_write"){
			// Send Writes after Receive has registered remote MR + remote Flag
      const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
    	const Payload payload = *reinterpret_cast<const Payload*>(object->blob.bytes);

      // Local
			auto* send_flag_ptr   = static_cast<std::uint64_t*>(this->flag_mr_ptr);
			auto* src_buf         = static_cast<std::uint8_t*>(this->buff_mr_ptr);
			const std::size_t local_buf_size = this->buff_size;

			auto* ctx_ptr = typed_ctxt;
			const int local_node_id = ctx_ptr->get_service_client_ref().get_my_id();
			const int local_dist_size = 2500;

      int my_node_id = client.get_my_id();
     	const int dist_size = 2'500;

			// std::thread([ctx_ptr, payload, send_flag_ptr, src_buf, local_buf_size, my_node_id, local_dist_size]{
				
				auto& client = ctx_ptr->get_service_client_ref();
      	for (int i = 0; i < dist_size; ++i){
      		// Update local flag value then write it to the remote flag
        	*send_flag_ptr = static_cast<std::uint64_t>(i+1);
        
					if (i != 0){
						client.template wait_for_oob_op<VolatileCascadeStoreWithStringKey>(
							payload.dest,
							1,
							90000
						);
						client.template wait_for_oob_op<VolatileCascadeStoreWithStringKey>(
							payload.dest,
							1,
							90000
						);
					}
					TimestampLogger::log(LOG_OOBWRITE_SEND, my_node_id, *send_flag_ptr);
        	// Write buffer → remote data
        	client.template oob_memwrite<VolatileCascadeStoreWithStringKey>(
        		payload.data_addr,
          	payload.dest,
          	payload.data_rkey,
          	local_buf_size,
          	false,
          	reinterpret_cast<uint64_t>(src_buf),
          	false,
          	false
        	);

        	// Write flag to remote flag
        	client.template oob_memwrite<VolatileCascadeStoreWithStringKey>(
          	payload.flag_addr,
          	payload.dest,
        		payload.flag_rkey,
        		sizeof(std::uint64_t),
        		false,
          	reinterpret_cast<uint64_t>(send_flag_ptr),
          	false,
          	false
        	);
     		}
      	TimestampLogger::flush("send_oobwrite_timestamp.dat");
      	std::cout << "Flushed logs to send_oobwrite_timestamp.dat" << std::endl;
			// }).detach();
		}
    else {
				std::cout << "Unsupported oob operation called!" << std::endl;
    }
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
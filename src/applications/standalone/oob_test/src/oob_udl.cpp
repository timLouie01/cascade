#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <cascade/utils.hpp>
#include <memory>
#include <sys/mman.h>
#ifndef LOG_OOBWRITE_RECV
#define LOG_OOBWRITE_RECV 1001
#endif
#ifndef LOG_OOBWRITE_SEND
#define LOG_OOBWRITE_SEND 1002
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
   
	void* oob_mr_ptr = nullptr;
	
	uint64_t buff_size;

	void* buff_mr_ptr = nullptr;
	
	void* flag_mr_ptr = nullptr;
	
	//   Payload now includes both data and flag descriptors
	struct Payload {
	  uint64_t data_addr;
	  uint64_t data_rkey;
	  uint64_t flag_addr;
	  uint64_t flag_rkey;
	  uint32_t dest; // node id that owns the remote MRs
  };

	static inline void warm_and_lock(void* p, size_t len, char data) {
    	(void) mlock(p, len);
    	constexpr size_t PAGE = 4096;
   		volatile char* c = reinterpret_cast<volatile char*>(p);
   		for (size_t off = 0; off < len; off += PAGE) {
        c[off] = data;
    	}
		}
	static inline void warm_and_lock_send(void* p, size_t len) {
    	warm_and_lock(p,len,'a');
	}

	static inline void warm_and_lock_receive(void* p, size_t len) {
    	warm_and_lock(p,len,'b');
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

	  std::cout << "[OOB]: I am node " << typed_ctxt->get_service_client_ref().get_my_id() << " and I received an object from sender:" << sender << " with key= " << key_string  << ", matching prefix=" << key_string.substr(0,prefix_length) << std::endl;

    auto tokens = str_tokenizer(key_string);
    
		if (tokens[1] == "send"){
			size_t PAGE = 4096;
			size_t MiB = 1024ull*1024ull;
	    size_t oob_mr_size = 512ull * MiB;
		  this->oob_mr_ptr = aligned_alloc(PAGE,oob_mr_size);
			if (!this->oob_mr_ptr) throw std::bad_alloc();

    	warm_and_lock_send(this->oob_mr_ptr, oob_mr_size);
			auto* base = static_cast<std::byte*>(this->oob_mr_ptr);
			auto* end  = base + oob_mr_size;

			this-> buff_mr_ptr = base+PAGE;
			this->buff_size = MiB;

			derecho::memory_attribute_t attr;
			attr.type = derecho::memory_attribute_t::SYSTEM;
			typed_ctxt->get_service_client_ref().oob_register_mem_ex(this->oob_mr_ptr,oob_mr_size,attr);

			uint64_t ptr = reinterpret_cast<uint64_t>(this->oob_mr_ptr);
			Blob blob;
			ObjectWithStringKey obj ("oob/receive",blob);
      typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(obj,0,1);  	
			
			// Set "Complete" Flag:
			// Buffer Replace with a Set "Updated Tail Value" Value 
			// (Value and not alloc memory after intial setup (there should be class level ptr to tail you overwrite))
      this->flag_mr_ptr = base;
			int intial_flag_value = 0;
		 	*static_cast<std::uint64_t*>(this->flag_mr_ptr) = intial_flag_value;
    }
    else if(tokens[1] == "receive"){
			size_t PAGE = 4096;
			size_t MiB = 1024ull*1024ull;
	    size_t oob_mr_size = 512ull * MiB;
		  this->oob_mr_ptr = aligned_alloc(PAGE,oob_mr_size);
			if (!this->oob_mr_ptr) throw std::bad_alloc();

    	warm_and_lock_receive(this->oob_mr_ptr, oob_mr_size);

			auto* base = static_cast<std::byte*>(this->oob_mr_ptr);
			auto* end  = base + oob_mr_size;

			this-> buff_mr_ptr = base+PAGE;
			this->buff_size = MiB;

    	derecho::memory_attribute_t attr;
			attr.type = derecho::memory_attribute_t::SYSTEM;
	    typed_ctxt->get_service_client_ref().oob_register_mem_ex(this->oob_mr_ptr,oob_mr_size,attr);
			
			const uint64_t data_addr_buff = reinterpret_cast<uint64_t>(buff_mr_ptr);
			const uint64_t data_rkey = typed_ctxt->get_service_client_ref().oob_rkey(buff_mr_ptr);

			this->flag_mr_ptr = base;
			int intial_flag_value = 0;
		 	*static_cast<std::uint64_t*>(this->flag_mr_ptr) = intial_flag_value;

			const uint64_t flag_addr = reinterpret_cast<uint64_t>(this->flag_mr_ptr);
			const uint64_t flag_rkey = data_rkey;
	    const uint32_t dest = typed_ctxt->get_service_client_ref().get_my_id();

			std::cout << "DATA rkey=" << data_rkey << " @ " << data_addr_buff << std::endl;
			std::cout << "FLAG rkey=" << flag_rkey << " @ " << flag_addr << " (initialized to 0)" << std::endl;

			Payload payload{data_addr_buff, data_rkey, flag_addr,flag_rkey, dest};
			Blob blob(reinterpret_cast<const uint8_t*>(&payload), sizeof(payload));  
			// Blob blob_1(reinterpret_cast<const uint8_t*>(&payload_1), sizeof(payload_1));  
			// Blob blob_2(reinterpret_cast<const uint8_t*>(&payload_2), sizeof(payload_2));  

			ObjectWithStringKey obj("oob/oob_write",blob);
			// ObjectWithStringKey obj_1 ("oob/oob_write",blob_1);
			// ObjectWithStringKey obj_2 ("oob/oob_write",blob_2);
			typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(obj,0,0); 
			 
      // typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(obj_1,0,0); 
			// typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(obj_2,0,0); 
			
			uint64_t* flag64_ptr = static_cast<std::uint64_t*>(this->flag_mr_ptr);
			int consume_flag = 0;
			int my_node_id = typed_ctxt->get_service_client_ref().get_my_id();
			int dist_size = 2'500;
			for (;;) {
				std::uint64_t current_flag = __atomic_load_n(flag64_ptr, __ATOMIC_ACQUIRE);
				if(current_flag > consume_flag){
					TimestampLogger::log(LOG_OOBWRITE_RECV,my_node_id,current_flag);
						uint8_t* p = static_cast<uint8_t*>(this->buff_mr_ptr);
						for (size_t i = 0; i < this->buff_size; ++i) {
							char ch = (p[i] >= 32 && p[i] <= 126) ? char(p[i]) : '.';
							std::cout << ch;
						}
						std::cout << "\"\n";
						consume_flag = current_flag;
				}
				else if (consume_flag == dist_size ){
					TimestampLogger::flush("recv_oobwrite_timestamp.dat");
					std::cout << "Flushed logs to " << "recv_oobwrite_timestamp.dat" << std::endl;
					break;
				}
			}
	/**
	  Potential Tail Poling Logic For Buffer (TODO: Fix Variable size logic list addr will instead be the actuall buffer potentially depending on Implementation
	*/
	// for (;;) {
	// 			if (*flag == -1){
	// 				for (; local_flag < *flag; ++local_flag){
	// 					uint8_t* p = static_cast<uint8_t*>(this->addrs[local_flag]);
	// 					for (size_t i = 0; i < this->sizes[local_flag]; ++i) {
	// 						char ch = (p[i] >= 32 && p[i] <= 126) ? char(p[i]) : '.';
	// 						std::cout << ch;
	// 					}
	// 					std::cout << "\"\n";
	// 					return;
	// 				}
	// 			}
	// 			else if(*flag > local_flag){
	// 				for (; local_flag < *flag; ++local_flag){
	// 					uint8_t* p = static_cast<uint8_t*>(this->addrs[local_flag]);
	// 					for (size_t i = 0; i < this->sizes[local_flag]; ++i) {
	// 						char ch = (p[i] >= 32 && p[i] <= 126) ? char(p[i]) : '.';
	// 						std::cout << ch;
	// 					}
	// 					std::cout << "\"\n";
	// 				}
	// 			}
	// 		}
    }
    else if (tokens[1] == "oob_write"){

			const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);

			const Payload* payload  = reinterpret_cast<const Payload*>(object->blob.bytes);

			uint64_t* flag64_ptr = static_cast<std::uint64_t*>(this->flag_mr_ptr);
			int sent_flag = 0;
			int dist_size = 2'500;
			int my_node_id = typed_ctxt->get_service_client_ref().get_my_id();
			for (int i = 0; i < dist_size; i++){
				TimestampLogger::log(LOG_OOBWRITE_SEND,my_node_id,*flag64_ptr);
				typed_ctxt->get_service_client_ref().oob_memwrite<VolatileCascadeStoreWithStringKey>(payload->data_addr,payload->dest,payload->data_rkey,this->buff_size,false,reinterpret_cast<uint64_t>(this->buff_mr_ptr),false, false);
				typed_ctxt->get_service_client_ref().oob_memwrite<VolatileCascadeStoreWithStringKey>(payload->flag_addr,payload->dest,payload->flag_rkey,8,false,*flag64_ptr,false, false);
				__atomic_store_n(flag64_ptr,*flag64_ptr+1, __ATOMIC_RELAXED);
			}
			TimestampLogger::flush("send_oobwrite_timestamp.dat");
			std::cout << "Flushed logs to " << "send_oobwrite_timestamp.dat" << std::endl;
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
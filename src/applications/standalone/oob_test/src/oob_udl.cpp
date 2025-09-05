#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <cascade/utils.hpp>
#include <memory>
 #include <sys/mman.h>
 
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
	
	
	// Receiver-side flag MR pointer (only meaningful on the node that runs "receive")
	void* flag_mr_ptr = nullptr;


	
	//   Payload now includes both data and flag descriptors
	struct Payload {
	  std::vector<uint64_t> data_addrs;
	  std::vector<uint64_t> data_rkeys;
	  uint64_t flag_addr;
	  uint64_t flag_rkey;
	  uint32_t dest; // node id that owns the remote MRs
  };

	std::vector<void*> addrs;
	std::vector<int> sizes;


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

			void* buf_1Kib = base;
			void* buf_1Mib = base + PAGE;

			addrs.push_back(buf_1Kib);
			addrs.push_back(buf_1Mib);
			sizes.push_back(1'024);
			sizes.push_back(1'048'576);

			derecho::memory_attribute_t attr;
			attr.type = derecho::memory_attribute_t::SYSTEM;
			typed_ctxt->get_service_client_ref().oob_register_mem_ex(this->oob_mr_ptr,oob_mr_size,attr);

			uint64_t ptr = reinterpret_cast<uint64_t>(this->oob_mr_ptr);
			Blob blob;
			ObjectWithStringKey obj ("oob/receive",blob);
      typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(obj,0,1);  				
			const size_t flag_mr_size = 4096;
      this->flag_mr_ptr = aligned_alloc(4096, flag_mr_size);
		 	std::memset(this->flag_mr_ptr, 1, 1);
      typed_ctxt->get_service_client_ref().oob_register_mem_ex(this->flag_mr_ptr, flag_mr_size, attr);
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

			void* buf_1Kib = base;
			void* buf_1Mib = base + PAGE;

    	derecho::memory_attribute_t attr;
			attr.type = derecho::memory_attribute_t::SYSTEM;
	    typed_ctxt->get_service_client_ref().oob_register_mem_ex(this->oob_mr_ptr,oob_mr_size,attr);
			
			const uint64_t data_addr_1Kib = reinterpret_cast<uint64_t>(buf_1Kib);
			const uint64_t data_rkey_1Kib = typed_ctxt->get_service_client_ref().oob_rkey(buf_1Kib);
		
			const uint64_t data_addr_1Mib = reinterpret_cast<uint64_t>(buf_1Mib);
			const uint64_t data_rkey_1Mib = typed_ctxt->get_service_client_ref().oob_rkey(buf_1Mib);

			const size_t flag_mr_size = 4096;
      this->flag_mr_ptr = aligned_alloc(4096, flag_mr_size);
		  std::memset(this->flag_mr_ptr, 0, 1);
      typed_ctxt->get_service_client_ref().oob_register_mem_ex(this->flag_mr_ptr, flag_mr_size, attr);

			const uint64_t flag_addr = reinterpret_cast<uint64_t>(this->flag_mr_ptr);
			const uint64_t flag_rkey = typed_ctxt->get_service_client_ref().oob_rkey(this->flag_mr_ptr);
	    const uint32_t dest = typed_ctxt->get_service_client_ref().get_my_id();

			// std::cout << "DATA rkey=" << data_rkey << " @ " << data_addr << std::endl;
			// std::cout << "FLAG rkey=" << flag_rkey << " @ " << flag_addr << " (initialized to 0)" << std::endl;

			Payload payload{{data_addr_1Kib, data_addr_1Mib}, {data_rkey_1Kib,data_rkey_1Mib}, flag_addr, flag_rkey, dest};
			 Blob blob(reinterpret_cast<const uint8_t*>(&payload), sizeof(payload));  
			// Blob blob_1(reinterpret_cast<const uint8_t*>(&payload_1), sizeof(payload_1));  
			// Blob blob_2(reinterpret_cast<const uint8_t*>(&payload_2), sizeof(payload_2));  

			ObjectWithStringKey obj("oob/oob_write",blob);
			// ObjectWithStringKey obj_1 ("oob/oob_write",blob_1);
			// ObjectWithStringKey obj_2 ("oob/oob_write",blob_2);
			typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(obj,0,0); 
			 
      // typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(obj_1,0,0); 
			// typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(obj_2,0,0); 
			
			uint8_t* flag = reinterpret_cast<uint8_t*>(flag_mr_ptr);
			int local_flag = 0;
			for (;;) {
				if (*flag == -1){
					for (; local_flag < *flag; ++local_flag){
						uint8_t* p = static_cast<uint8_t*>(this->addrs[local_flag]);
						for (size_t i = 0; i < this->sizes[local_flag]; ++i) {
							char ch = (p[i] >= 32 && p[i] <= 126) ? char(p[i]) : '.';
							std::cout << ch;
						}
						std::cout << "\"\n";
						return;
					}
				}
				else if(*flag > local_flag){
					for (; local_flag < *flag; ++local_flag){
						uint8_t* p = static_cast<uint8_t*>(this->addrs[local_flag]);
						for (size_t i = 0; i < this->sizes[local_flag]; ++i) {
							char ch = (p[i] >= 32 && p[i] <= 126) ? char(p[i]) : '.';
							std::cout << ch;
						}
						std::cout << "\"\n";
					}
				}
			}
    }
    else if (tokens[1] == "oob_write"){

			const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);

			const Payload* payload  = reinterpret_cast<const Payload*>(object->blob.bytes);
			uint64_t local_flag_ptr = reinterpret_cast<uint64_t>(this->flag_mr_ptr);

			for (int i = 0; i < payload->data_addrs.size(); i++){
				typed_ctxt->get_service_client_ref().oob_memwrite<VolatileCascadeStoreWithStringKey>(payload->data_addrs[i],payload->dest,payload->data_rkeys[i],this->sizes[i],false,reinterpret_cast<uint64_t>(this->addrs[i]),false, false);
			}
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
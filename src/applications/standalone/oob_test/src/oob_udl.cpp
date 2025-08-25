#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <cascade/utils.hpp>
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
	                     uint64_t data_addr;
	                     uint64_t data_rkey;
	                     uint64_t flag_addr;
	                     uint64_t flag_rkey;
	                     uint32_t dest; // node id that owns the remote MRs
                            };


	std::optional<Payload> payload_store;

	virtual void operator () (const derecho::node_id_t sender,
                              const std::string& key_string,
                              const uint32_t prefix_length,
                              persistent::version_t version,
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>& outputs,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
       auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);

	    std::cout << "[OOB]: I am node " << typed_ctxt->get_service_client_ref().get_my_id() << " and I received an object from sender:" << sender << " with key= " << key_string 
                  << ", matching prefix=" << key_string.substr(0,prefix_length) << std::endl;
       auto tokens = str_tokenizer(key_string);
       if (tokens[1] == "send"){
/**
       	       cudaSetDevice(device_id);
	
	size_in_bytes = (std::stoul("8") << 20);	
      	void* dev_ptr = nullptr;
         cudaError_t err = cudaMalloc(&dev_ptr, size_in_bytes);
   	 if (err != cudaSuccess) {
        	std::cerr << "cudaMalloc failed: " << cudaGetErrorString(err) << std::endl;
   	 }
         err = cudaMemset(dev_ptr, value, size_in_bytes);
         if (err != cudaSuccess) {
                std::cerr << "cudaMemset failed: " << cudaGetErrorString(err) << std::endl;
                cudaFree(dev_ptr); // Clean up
         }
*/
//	       void* oob_mr_ptr = nullptr; 
	       size_t      oob_mr_size     = 1ul << 20;
		size_t      oob_data_size =256;
		      this->oob_mr_ptr = aligned_alloc(4096,oob_mr_size);
     				       memset(this->oob_mr_ptr, 'a', oob_data_size);
				       derecho::memory_attribute_t attr;
				           attr.type = derecho::memory_attribute_t::SYSTEM;
					  typed_ctxt->get_service_client_ref().oob_register_mem_ex(this->oob_mr_ptr,oob_mr_size,attr);
					  uint64_t ptr = reinterpret_cast<uint64_t>(this->oob_mr_ptr);
					//  std::cout << typed_ctxt->get_service_client_ref().oob_rkey(this->oob_mr_ptr) << " RKEY FOR: " << ptr << std::endl;
					 //  std::cout << "Int mem Original: " << ptr << std::endl;
					  Blob blob; 
					  ObjectWithStringKey obj ("oob/receive",blob);
					  std::cout << "SEND RECEIVE" << std::endl;
      					typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(obj,0,1); 
       				         std::cout << "SENDING of RECEIVE worked!" << std::endl; 				

					const size_t flag_mr_size = 4096;
               				this->flag_mr_ptr = aligned_alloc(4096, flag_mr_size);
	     // Initialize flag to 1 (true to overwrite)
		 			std::memset(this->flag_mr_ptr, 0, 1);
                 			typed_ctxt->get_service_client_ref().oob_register_mem_ex(this->flag_mr_ptr, flag_mr_size, attr);
					auto* local_flag = reinterpret_cast<uint8_t*>(this->flag_mr_ptr);
					std::cout << "OVERWRITE FLAG: " << int(local_flag[0]) << std::endl;
       }
       else if(tokens[1] == "receive"){
		size_t      oob_mr_size     = 1ul << 20;
		size_t      oob_data_size =256;
		this->oob_mr_ptr = aligned_alloc(4096,oob_mr_size);
    		derecho::memory_attribute_t attr;
		attr.type = derecho::memory_attribute_t::SYSTEM;
	       	typed_ctxt->get_service_client_ref().oob_register_mem_ex(this->oob_mr_ptr,oob_mr_size,attr);
		memset(this->oob_mr_ptr, 'b', oob_data_size);

		const uint64_t data_addr = reinterpret_cast<uint64_t>(this->oob_mr_ptr);
		const uint64_t data_rkey = typed_ctxt->get_service_client_ref().oob_rkey(this->oob_mr_ptr);
		
		const size_t flag_mr_size = 4096;
                this->flag_mr_ptr = aligned_alloc(4096, flag_mr_size);
	     // Initialize flag to 0 (false)
		 std::memset(this->flag_mr_ptr, 0, 1);
                 typed_ctxt->get_service_client_ref().oob_register_mem_ex(this->flag_mr_ptr, flag_mr_size, attr);



		const uint64_t flag_addr = reinterpret_cast<uint64_t>(this->flag_mr_ptr);
		const uint64_t flag_rkey = typed_ctxt->get_service_client_ref().oob_rkey(this->flag_mr_ptr);
	       	const uint32_t dest = typed_ctxt->get_service_client_ref().get_my_id();

		std::cout << "DATA rkey=" << data_rkey << " @ " << data_addr << std::endl;
		std::cout << "FLAG rkey=" << flag_rkey << " @ " << flag_addr << " (initialized to 0)" << std::endl;

		Payload payload{data_addr, data_rkey, flag_addr, flag_rkey, dest};

		Blob blob(reinterpret_cast<const uint8_t*>(&payload), oob_data_size);  
		ObjectWithStringKey obj ("oob/oob_write",blob);
		// obj.set_timestamp(rkey);
		std::cout << "SEND" << std::endl;
      		typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(obj,0,0); 

		volatile uint8_t* flag = reinterpret_cast<volatile uint8_t*>(flag_mr_ptr);
		for (;;) {
			    if (*flag == 1)
			{
				uint8_t* p = static_cast<uint8_t*>(oob_mr_ptr);
				std::cout << "ASCII: \"";
				for (size_t i = 0; i < oob_mr_size; ++i) {
					    char ch = (p[i] >= 32 && p[i] <= 126) ? char(p[i]) : '.';
					        std::cout << ch;
				}
				std::cout << "\"\n";
			}
		}
       }
       else if (tokens[1] == "oob_write"){

	const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
// 	uint64_t rkey = object->get_timestamp();
	const Payload* payload  = reinterpret_cast<const Payload*>(object->blob.bytes);
	uint64_t local_data_ptr = reinterpret_cast<uint64_t>(this->oob_mr_ptr);
	uint64_t local_flag_ptr = reinterpret_cast<uint64_t>(this->flag_mr_ptr);

	typed_ctxt->get_service_client_ref().oob_memwrite<VolatileCascadeStoreWithStringKey>(payload->data_addr,payload->dest,payload->data_rkey,256,false,local_data_ptr,false, false);
	typed_ctxt->get_service_client_ref().oob_memwrite<VolatileCascadeStoreWithStringKey>(payload->flag_addr,payload->dest,payload->flag_rkey,256,false,local_flag_ptr,false, false);
	
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

#ifndef RDMA_H
#define RDMA_H



#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
#include <getopt.h>
#include <cassert>
#include <unordered_map>
#include <algorithm>
#include <shared_mutex>
#include <thread>
#include <chrono>
#include <memory>
#include <sstream>

//#include <options.h>

#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
//#include "util/thread_local.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <vector>
//#ifdef __cplusplus
//extern "C" { //only need to export C interface if
//// used by C++ source code
//#endif

/* poll CQ timeout in millisec (2 seconds) */
#define MAX_POLL_CQ_TIMEOUT 1000000
#define MSG "SEND operation "

#if __BYTE_ORDER == __LITTLE_ENDIAN
//	static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
//	static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
	static inline uint64_t htonll(uint64_t x) { return x; }
	static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif
struct config_t
{
  const char* dev_name; /* IB device name */
  const char* server_name;	/* server host name */
  u_int32_t tcp_port;   /* server TCP port */
  int ib_port;		  /* local IB port to work with, or physically port number */
  int gid_idx;		  /* gid index to use */
  int init_local_buffer_size;   /*initial local SST buffer size*/
};
/* structure to exchange data which is needed to connect the QPs */
struct registered_qp_config {
  uint32_t qp_num; /* QP number */
  uint16_t lid;	/* LID of the IB port */
  uint8_t gid[16]; /* gid */
} __attribute__((packed));
enum RDMA_Command_Type {create_qp_, create_mr_};
union RDMA_Command_Content{
  size_t mem_size;
  registered_qp_config qp_config;
};
struct computing_to_memory_msg
{
  RDMA_Command_Type command;
  RDMA_Command_Content content;
};

// Structure for the file handle in RDMA file system. it could be a link list
// for large files
struct SST_Metadata{
  std::shared_mutex file_lock;
  std::string fname;
  ibv_mr* mr;
  ibv_mr* map_pointer;
  SST_Metadata* last_ptr = nullptr;
  SST_Metadata* next_ptr = nullptr;
  size_t file_size = 0;

};
template <typename T>
struct atomwrapper
{
  std::atomic<T> _a;

  atomwrapper()
      :_a()
  {}

  atomwrapper(const std::atomic<T> &a)
      :_a(a.load())
  {}

  atomwrapper(const atomwrapper &other)
      :_a(other._a.load())
  {}

  atomwrapper &operator=(const atomwrapper &other)
  {
    _a.store(other._a.load());
  }
};
class In_Use_Array{
 public:
  In_Use_Array(size_t size, size_t chunk_size) :size_(size), chunk_size_(chunk_size){
    in_use = new std::atomic<bool>[size_];
    for (size_t i = 0; i < size_; ++i){
      in_use[i] = false;
    }

  }
  int allocate_memory_slot(){
    for (int i = 0; i < static_cast<int>(size_); ++i){
//      auto start = std::chrono::high_resolution_clock::now();
      bool temp = in_use[i];
      if (temp == false) {
//        auto stop = std::chrono::high_resolution_clock::now();
//        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//        std::printf("Compare and swap time duration is %ld \n", duration.count());
        if(in_use[i].compare_exchange_strong(temp, true)){
//          std::cout << "chunk" <<i << "was changed to true" << std::endl;

          return i; // find the empty slot then return the index for the slot

        }
//        else
//          std::cout << "Compare and swap fail" << "i equals" << i  << "type is" << type_ << std::endl;
      }

    }
    return -1; //Not find the empty memory chunk.
  }
  bool deallocate_memory_slot(int index) {
    bool temp = true;
    assert(in_use[index] == true);
//    std::cout << "chunk" <<index << "was changed to false" << std::endl;

    return in_use[index].compare_exchange_strong(temp, false);

  }
  size_t get_chunk_size(){
    return chunk_size_;
  }
 private:
  size_t size_;
  size_t chunk_size_;
  std::atomic<bool>* in_use;
//  int type_;
};
/* structure of system resources */
struct resources
{
  struct ibv_device_attr device_attr;
  /* Device attributes */
  struct ibv_sge* sge = nullptr;
  struct ibv_recv_wr*	rr = nullptr;
  struct ibv_port_attr port_attr;	/* IB port attributes */
//  std::vector<registered_qp_config> remote_mem_regions; /* memory buffers for RDMA */
  struct ibv_context* ib_ctx = nullptr;		   /* device handle */
  struct ibv_pd* pd = nullptr;				   /* PD handle */
  std::map<std::string,ibv_cq*> cq_map;				   /* CQ Map */
  std::map<std::string,ibv_qp*> qp_map;				   /* QP Map */
  std::shared_mutex queue_lock;
  struct ibv_mr* mr_receive = nullptr;              /* MR handle for receive_buf */
  struct ibv_mr* mr_send = nullptr;                 /* MR handle for send_buf */
//  struct ibv_mr* mr_SST = nullptr;                        /* MR handle for SST_buf */
//  struct ibv_mr* mr_remote;                     /* remote MR handle for computing node */
  char* SST_buf = nullptr;			/* SSTable buffer pools pointer, it could contain multiple SSTbuffers */
  char* send_buf = nullptr;                       /* SEND buffer pools pointer, it could contain multiple SEND buffers */
  char* receive_buf = nullptr;		        /* receive buffer pool pointer,  it could contain multiple acturall receive buffers */
  std::map<std::string, int> sock_map;						   /* TCP socket file descriptor */
  std::map<std::string, ibv_mr*> mr_receive_map;
  std::map<std::string, ibv_mr*> mr_send_map;

};

//namespace ROCKSDB_NAMESPACE {
/* structure of test parameters */
class RDMA_Manager{
 public:
  RDMA_Manager(config_t config,
               std::unordered_map<ibv_mr*, In_Use_Array>* Remote_Bitmap,
               std::unordered_map<ibv_mr*, In_Use_Array>* Write_Bitmap,
               std::unordered_map<ibv_mr*, In_Use_Array>* Read_Bitmap,
               size_t table_size, size_t write_block_size,
               size_t read_block_size);
//  RDMA_Manager(config_t config) : rdma_config(config){
//    res = new resources();
//    res->sock = -1;
//  }
//  RDMA_Manager()=delete;
  ~RDMA_Manager();
  // RDMA set up create all the resources, and create one query pair for RDMA send & Receive.
  void Client_Set_Up_Resources();
  //Set up the socket connection to remote shared memory.
  bool Client_Connect_to_Server_RDMA();


  // this function is for the server.
  void Server_to_Client_Communication();
  void server_communication_thread(std::string client_ip, int socket_fd);
  // Local memory register will register RDMA memory in local machine,
  //Both Computing node and share memory will call this function.
  // it also push the new block bit map to the Remote_Mem_Bitmap
  bool Local_Memory_Register(
      char** p2buffpointer, ibv_mr** p2mrpointer, size_t size,
      size_t chunk_size);// register the memory on the local side
  // Remote Memory registering will call RDMA send and receive to the remote memory
  // it also push the new SST bit map to the Remote_Mem_Bitmap
  bool Remote_Memory_Register(size_t size);
  int Remote_Memory_Deregister();
  // new query pair creation and connection to remote Memory by RDMA send and receive
  bool Remote_Query_Pair_Connection(
      std::string& qp_id);// Only called by client.

  int RDMA_Read(ibv_mr *remote_mr, ibv_mr *local_mr, size_t msg_size, std::string q_id, unsigned int send_flag,
                int poll_num);
  int RDMA_Write(ibv_mr* remote_mr, ibv_mr* local_mr, size_t msg_size,
                 std::string q_id, unsigned int send_flag,
                 int poll_num);
  int RDMA_Send();
  int poll_completion(ibv_wc* wc_p, int num_entries, std::string q_id);
  bool Deallocate_Local_RDMA_Slot(ibv_mr* mr, ibv_mr* map_pointer,
                                  std::string buffer_type) const;
  bool Deallocate_Remote_RDMA_Slot(SST_Metadata* sst_meta) const;

  //Allocate an empty remote SST, return the index for the memory slot
  void Allocate_Remote_RDMA_Slot(const std::string &file_name,
                                 SST_Metadata*& sst_meta);
  void Allocate_Local_RDMA_Slot(ibv_mr*& mr_input, ibv_mr*& map_pointer,
                                std::string buffer_type);

  resources* res = nullptr;
  std::vector<ibv_mr*> remote_mem_pool; /* a vector for all the remote memory regions*/
  std::vector<ibv_mr*> local_mem_pool; /* a vector for all the local memory regions.*/
  std::unordered_map<ibv_mr*, In_Use_Array>* Remote_Mem_Bitmap = nullptr;
//  std::shared_mutex remote_pool_mutex;
  std::unordered_map<ibv_mr*, In_Use_Array>* Write_Local_Mem_Bitmap = nullptr;
//  std::shared_mutex write_pool_mutex;
  std::unordered_map<ibv_mr*, In_Use_Array>* Read_Local_Mem_Bitmap = nullptr;
//  std::shared_mutex read_pool_mutex;
  size_t Read_Block_Size;
  size_t Write_Block_Size;
  uint64_t Table_Size;
  std::shared_mutex remote_mem_create_mutex;
  std::shared_mutex local_mem_create_mutex;
  std::shared_mutex rw_mutex;
  std::shared_mutex main_qp_mutex;
  std::vector<std::thread> thread_pool;
  static thread_local std::string* t_local_1;
//  static __thread std::string thread_id;
 private:

  config_t rdma_config;

  int client_sock_connect(const char* servername, int port);
  int server_sock_connect(const char* servername, int port);
  int sock_sync_data(int sock, int xfer_size, char* local_data, char* remote_data);
  template <typename T>
  int post_send(ibv_mr* mr, std::string qp_id = "main");
//  int post_receives(int len);
  template <typename T>
  int post_receive(ibv_mr* mr, std::string qp_id = "main");

  int resources_create();
  int modify_qp_to_init(struct ibv_qp* qp);
  int modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn, uint16_t dlid, uint8_t* dgid);
  int modify_qp_to_rts(struct ibv_qp* qp);
  bool create_qp(std::string& id);
  int connect_qp(registered_qp_config remote_con_data, std::string& qp_id);
  int resources_destroy();
  void print_config(void);
  void usage(const char* argv0);

};

//#ifdef __cplusplus
//}
//#endif
//}
#endif

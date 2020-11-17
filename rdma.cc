#include <rdma.h>
//namespace ROCKSDB_NAMESPACE {
void UnrefHandle_rdma(void* ptr){
  delete static_cast<std::string*>(ptr);
}
/******************************************************************************
* Function: RDMA_Manager

*
* Output
* none
*
*
* Description
* Initialize the resource for RDMA.
******************************************************************************/
RDMA_Manager::RDMA_Manager(
    config_t config, std::unordered_map<ibv_mr*, In_Use_Array>* Remote_Bitmap,
    std::unordered_map<ibv_mr*, In_Use_Array>* Write_Bitmap,
    std::unordered_map<ibv_mr*, In_Use_Array>* Read_Bitmap, size_t table_size,
    size_t write_block_size, size_t read_block_size)
    : Read_Block_Size(read_block_size), Write_Block_Size(write_block_size),Table_Size(table_size),
    rdma_config(config)
{
  assert(read_block_size <table_size);
  res = new resources();
  //  res->sock = -1;
  Remote_Mem_Bitmap = Remote_Bitmap;
  Write_Local_Mem_Bitmap = Write_Bitmap;
  Read_Local_Mem_Bitmap = Read_Bitmap;
}
/******************************************************************************
* Function: ~RDMA_Manager

*
* Output
* none
*
*
* Description
* Cleanup and deallocate all resources used for RDMA
******************************************************************************/
RDMA_Manager::~RDMA_Manager() {
  if (!res->qp_map.empty())
    for (auto it = res->qp_map.begin(); it != res->qp_map.end(); it++) {
      if (ibv_destroy_qp(it->second)) {
        fprintf(stderr, "failed to destroy QP\n");
      }
    }
  if (res->mr_receive)
    if (ibv_dereg_mr(res->mr_receive)) {
      fprintf(stderr, "failed to deregister MR\n");
    }
  if (res->mr_send)
    if (ibv_dereg_mr(res->mr_send)) {
      fprintf(stderr, "failed to deregister MR\n");
    }
  if (!local_mem_pool.empty()) {
    //    ibv_dereg_mr(local_mem_pool.at(0));
    //    std::for_each(local_mem_pool.begin(), local_mem_pool.end(), ibv_dereg_mr);
    for (ibv_mr* p : local_mem_pool) {
      ibv_dereg_mr(p);
      //       local buffer is registered on this machine need deregistering.
      delete (char*)p->addr;
    }
    //    local_mem_pool.clear();
  }
  if (!remote_mem_pool.empty()) {
    for (auto p : remote_mem_pool) {
      delete p;  // remote buffer is not registered on this machine so just delete the structure
    }
    remote_mem_pool.clear();
  }
  if (res->receive_buf) delete res->receive_buf;
  if (res->send_buf) delete res->send_buf;
  //  if (res->SST_buf)
  //    delete res->SST_buf;
  if (!res->cq_map.empty())
    for (auto it = res->cq_map.begin(); it != res->cq_map.end(); it++) {
      if (ibv_destroy_cq(it->second)) {
        fprintf(stderr, "failed to destroy CQ\n");
      }
    }

  if (res->pd)
    if (ibv_dealloc_pd(res->pd)) {
      fprintf(stderr, "failed to deallocate PD\n");
    }

  if (res->ib_ctx)
    if (ibv_close_device(res->ib_ctx)) {
      fprintf(stderr, "failed to close device context\n");
    }
  if (!res->sock_map.empty())
    for (auto it = res->sock_map.begin(); it != res->sock_map.end(); it++) {
      if (close(it->second)) {
        fprintf(stderr, "failed to close socket\n");
      }
    }

  delete res;
}
/******************************************************************************
Socket operations
For simplicity, the example program uses TCP sockets to exchange control
information. If a TCP/IP stack/connection is not available, connection manager
(CM) may be used to pass this information. Use of CM is beyond the scope of
this example
******************************************************************************/
/******************************************************************************
* Function: sock_connect
*
* Input
* servername URL of server to connect to (NULL for server mode)
* port port of service
*
* Output
* none
*
* Returns
* socket (fd) on success, negative error code on failure
*
* Description
* Connect a socket. If servername is specified a client connection will be
* initiated to the indicated server and port. Otherwise listen on the
* indicated port for an incoming connection.
*
******************************************************************************/
int RDMA_Manager::client_sock_connect(const char* servername, int port) {
  struct addrinfo* resolved_addr = NULL;
  struct addrinfo* iterator;
  char service[6];
  int sockfd = -1;
  int listenfd = 0;
  int tmp;
  struct addrinfo hints = {
      .ai_flags = AI_PASSIVE, .ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
  if (sprintf(service, "%d", port) < 0) goto sock_connect_exit;
  /* Resolve DNS address, use sockfd as temp storage */
  sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
  if (sockfd < 0) {
    fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
    goto sock_connect_exit;
  }
  /* Search through results and find the one we want */
  for (iterator = resolved_addr; iterator; iterator = iterator->ai_next) {
    sockfd = socket(iterator->ai_family, iterator->ai_socktype,
                    iterator->ai_protocol);
    if (sockfd >= 0) {
      if (servername) {
        /* Client mode. Initiate connection to remote */
        if ((tmp = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen))) {
          fprintf(stdout, "failed connect \n");
          close(sockfd);
          sockfd = -1;
        }
      } else {
        /* Server mode. Set up listening socket an accept a connection */
        listenfd = sockfd;
        sockfd = -1;
        if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
          goto sock_connect_exit;
        listen(listenfd, 1);
        sockfd = accept(listenfd, NULL, 0);
      }
    }
    fprintf(stdout, "TCP connection was established\n");
  }
sock_connect_exit:
  if (listenfd) close(listenfd);
  if (resolved_addr) freeaddrinfo(resolved_addr);
  if (sockfd < 0) {
    if (servername)
      fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
    else {
      perror("server accept");
      fprintf(stderr, "accept() failed\n");
    }
  }
  return sockfd;
}
// connection code for server side, will get prepared for multiple connection
// on the same port.
int RDMA_Manager::server_sock_connect(const char* servername, int port) {
  struct addrinfo* resolved_addr = NULL;
  struct addrinfo* iterator;
  char service[6];
  int sockfd = -1;
  int listenfd = 0;
  struct sockaddr address;
  socklen_t len = sizeof(struct sockaddr);
  struct addrinfo hints = {
      .ai_flags = AI_PASSIVE, .ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
  if (sprintf(service, "%d", port) < 0) goto sock_connect_exit;
  /* Resolve DNS address, use sockfd as temp storage */
  sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
  if (sockfd < 0) {
    fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
    goto sock_connect_exit;
  }

  /* Search through results and find the one we want */
  for (iterator = resolved_addr; iterator; iterator = iterator->ai_next) {
    sockfd = socket(iterator->ai_family, iterator->ai_socktype,
                    iterator->ai_protocol);
    if (sockfd >= 0) {
      /* Server mode. Set up listening socket an accept a connection */
      listenfd = sockfd;
      sockfd = -1;
      if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
        goto sock_connect_exit;
      listen(listenfd, 20);
      while (1) {
        sockfd = accept(listenfd, &address, &len);
        std::cout << "connection built up from" << address.sa_data << std::endl;
        std::cout << "connection family is " << address.sa_family << std::endl;
        if (sockfd < 0) {
          fprintf(stderr, "Connection accept error, erron: %d\n", errno);
          break;
        }
        thread_pool.push_back(std::thread(
            [this](std::string client_ip, int socket_fd) {
              this->server_communication_thread(client_ip, socket_fd);
            },
            std::string(address.sa_data), sockfd));
      }
    }
  }
sock_connect_exit:

  if (listenfd) close(listenfd);
  if (resolved_addr) freeaddrinfo(resolved_addr);
  if (sockfd < 0) {
    if (servername)
      fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
    else {
      perror("server accept");
      fprintf(stderr, "accept() failed\n");
    }
  }
  return sockfd;
}
void RDMA_Manager::server_communication_thread(std::string client_ip,
                                               int socket_fd) {
  char temp_receive[2];
  char temp_send[] = "Q";
  struct registered_qp_config local_con_data;
  struct registered_qp_config remote_con_data;
  struct registered_qp_config tmp_con_data;
  //  std::string qp_id = "main";
  int rc = 0;

  union ibv_gid my_gid;
  if (rdma_config.gid_idx >= 0) {
    rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port, rdma_config.gid_idx,
                       &my_gid);
    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_config.ib_port, rdma_config.gid_idx);
      return;
    }
  } else
    memset(&my_gid, 0, sizeof my_gid);
  /* exchange using TCP sockets info required to connect QPs */
  create_qp(client_ip);
  local_con_data.qp_num = htonl(res->qp_map[client_ip]->qp_num);
  local_con_data.lid = htons(res->port_attr.lid);
  memcpy(local_con_data.gid, &my_gid, 16);
  fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
  if (sock_sync_data(socket_fd, sizeof(struct registered_qp_config),
                     (char*)&local_con_data, (char*)&tmp_con_data) < 0) {
    fprintf(stderr, "failed to exchange connection data between sides\n");
    rc = 1;
  }
  remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
  remote_con_data.lid = ntohs(tmp_con_data.lid);
  memcpy(remote_con_data.gid, tmp_con_data.gid, 16);
  fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data.qp_num);
  fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data.lid);
  if (connect_qp(remote_con_data, client_ip)) {
    fprintf(stderr, "failed to connect QPs\n");
  }

  ibv_mr* send_mr;
  char* send_buff;
  if (!Local_Memory_Register(&send_buff, &send_mr, 1000, 0)) {
    fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
  }
  ibv_mr* recv_mr;
  char* recv_buff;
  if (!Local_Memory_Register(&recv_buff, &recv_mr, 1000, 0)) {
    fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
  }
  post_receive<computing_to_memory_msg>(recv_mr, client_ip);

  // sync after send & recv buffer creation and receive request posting.
  if (sock_sync_data(socket_fd, 1, temp_send,
                     temp_receive)) /* just send a dummy char back and forth */
  {
    fprintf(stderr, "sync error after QPs are were moved to RTS\n");
    rc = 1;
  }

  // Computing node and share memory connection succeed.
  // Now is the communication through rdma.
  computing_to_memory_msg* receive_pointer;
  receive_pointer = (computing_to_memory_msg*)recv_buff;
  //  receive_pointer->command = ntohl(receive_pointer->command);
  //  receive_pointer->content.qp_config.qp_num = ntohl(receive_pointer->content.qp_config.qp_num);
  //  receive_pointer->content.qp_config.lid = ntohs(receive_pointer->content.qp_config.lid);
  ibv_wc wc[2] = {};
  while (true) {
    poll_completion(wc, 1, client_ip);
    // copy the pointer of receive buf to a new place because
    // it is the same with send buff pointer.
    if (receive_pointer->command == create_mr_) {
      std::cout << "create memory region command receive for" << client_ip
                << std::endl;
      ibv_mr* send_pointer = (ibv_mr*)send_buff;
      ibv_mr* mr;
      char* buff;
      if (!Local_Memory_Register(&buff, &mr, receive_pointer->content.mem_size,
                                 0)) {
        fprintf(stderr, "memory registering failed by size of 0x%x\n",
                static_cast<unsigned>(receive_pointer->content.mem_size));
      }

      *send_pointer = *mr;
      post_receive<computing_to_memory_msg>(recv_mr, client_ip);
      post_send<ibv_mr>(
          send_mr,
          client_ip);  // note here should be the mr point to the send buffer.
      poll_completion(wc, 1, client_ip);
    } else if (receive_pointer->command == create_qp_) {
      std::string new_qp_id =
          std::to_string(receive_pointer->content.qp_config.lid) +
          std::to_string(receive_pointer->content.qp_config.qp_num);
      std::cout << "create query pair command receive for" << client_ip
                << std::endl;
      fprintf(stdout, "Remote QP number=0x%x\n",
              receive_pointer->content.qp_config.qp_num);
      fprintf(stdout, "Remote LID = 0x%x\n",
              receive_pointer->content.qp_config.lid);
      registered_qp_config* send_pointer = (registered_qp_config*)send_buff;
      create_qp(new_qp_id);
      if (rdma_config.gid_idx >= 0) {
        rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port,
                           rdma_config.gid_idx, &my_gid);
        if (rc) {
          fprintf(stderr, "could not get gid for port %d, index %d\n",
                  rdma_config.ib_port, rdma_config.gid_idx);
          return;
        }
      } else
        memset(&my_gid, 0, sizeof my_gid);
      /* exchange using TCP sockets info required to connect QPs */
      send_pointer->qp_num = res->qp_map[new_qp_id]->qp_num;
      send_pointer->lid = res->port_attr.lid;
      memcpy(local_con_data.gid, &my_gid, 16);
      connect_qp(receive_pointer->content.qp_config, new_qp_id);
      post_receive<computing_to_memory_msg>(recv_mr, client_ip);
      post_send<registered_qp_config>(send_mr, client_ip);
      poll_completion(wc, 1, client_ip);
    }
  }
  return;
  // TODO: Build up a exit method for shared memory side, don't forget to destroy all the RDMA resourses.
}
void RDMA_Manager::Server_to_Client_Communication() {
  if (resources_create()) {
    fprintf(stderr, "failed to create resources\n");
  }
  server_sock_connect(rdma_config.server_name, rdma_config.tcp_port);
}

//    Register the memory through ibv_reg_mr on the local side. this function will be called by both of the server side and client side.
bool RDMA_Manager::Local_Memory_Register(char** p2buffpointer,
                                         ibv_mr** p2mrpointer, size_t size,
                                         size_t chunk_size) {
  int mr_flags = 0;
  *p2buffpointer = new char[size];
  if (!*p2buffpointer) {
    fprintf(stderr, "failed to malloc bytes to memory buffer\n");
    return false;
  }
  memset(*p2buffpointer, 0, size);

  /* register the memory buffer */
  mr_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
//  auto start = std::chrono::high_resolution_clock::now();
  *p2mrpointer = ibv_reg_mr(res->pd, *p2buffpointer, size, mr_flags);
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//  std::printf("Memory registeration size: %zu time elapse (%ld) us\n", size, duration.count());
  local_mem_pool.push_back(*p2mrpointer);
  if (!*p2mrpointer) {
    fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x, size = %zu, region num = %zu\n",
            mr_flags, size, local_mem_pool.size());
    return false;
  } else if (rdma_config.server_name && chunk_size > 0) {  // for the send buffer and receive buffer they will not be
    // If chunk size equals 0, which means that this buffer should not be add to Local Bit
    // Map, will not be regulated by the RDMA manager.

    int placeholder_num =
        (*p2mrpointer)->length /
        (chunk_size);  // here we supposing the SSTables are 4 megabytes
    In_Use_Array in_use_array(placeholder_num, chunk_size);
    if (chunk_size == Write_Block_Size){
//        std::unique_lock<std::shared_mutex> l(write_pool_mutex);
        Write_Local_Mem_Bitmap->insert({*p2mrpointer, in_use_array});
//        l.unlock();
    }
    else if (chunk_size == Read_Block_Size){
//        std::unique_lock<std::shared_mutex> l(read_pool_mutex);
        Read_Local_Mem_Bitmap->insert({*p2mrpointer, in_use_array});
//        l.unlock();
    }

    else
      printf("RDma bitmap insert error");
    fprintf(
        stdout,
        "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
        (*p2mrpointer)->addr, (*p2mrpointer)->lkey, (*p2mrpointer)->rkey,
        mr_flags);

    return true;
  }
  fprintf(stdout,
          "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
          (*p2mrpointer)->addr, (*p2mrpointer)->lkey, (*p2mrpointer)->rkey,
          mr_flags);
  return true;
};
/******************************************************************************
* Function: set_up_RDMA
*
* Input
* argv0 command line arguments
*
* Output
* none
*
* Returns
* none
*
* Description
* set up the connection to shared memroy.
******************************************************************************/
void RDMA_Manager::Client_Set_Up_Resources() {
  //  int rc = 1;
  // int trans_times;
  char temp_char;
  std::string ip_add;
  std::cout << "Input server IP address:\n";
  std::cin >> ip_add;
  rdma_config.server_name = ip_add.c_str();
  /* if client side */

  res->sock_map["main"] =
      client_sock_connect(rdma_config.server_name, rdma_config.tcp_port);
  if (res->sock_map["main"] < 0) {
    fprintf(stderr,
            "failed to establish TCP connection to server %s, port %d\n",
            rdma_config.server_name, rdma_config.tcp_port);
  }

  if (resources_create()) {
    fprintf(stderr, "failed to create resources\n");
    return;
  }
  Client_Connect_to_Server_RDMA();
}
/******************************************************************************
* Function: resources_create
*
* Input
* res pointer to resources structure to be filled in
*
* Output
* res filled in with resources
*
* Returns
* 0 on success, 1 on failure
*
* Description
*
* This function creates and allocates all necessary system resources. These
* are stored in res.
*****************************************************************************/
int RDMA_Manager::resources_create() {
  struct ibv_device** dev_list = NULL;
  struct ibv_device* ib_dev = NULL;
//  int iter = 1;
  int i;

//  int cq_size = 0;
  int num_devices;
  int rc = 0;
  //        ibv_device_attr *device_attr;

  fprintf(stdout, "searching for IB devices in host\n");
  /* get device names in the system */
  dev_list = ibv_get_device_list(&num_devices);
  if (!dev_list) {
    fprintf(stderr, "failed to get IB devices list\n");
    rc = 1;
  }
  /* if there isn't any IB device in host */
  if (!num_devices) {
    fprintf(stderr, "found %d device(s)\n", num_devices);
    rc = 1;
  }
  fprintf(stdout, "found %d device(s)\n", num_devices);
  /* search for the specific device we want to work with */
  for (i = 0; i < num_devices; i++) {
    if (!rdma_config.dev_name) {
      rdma_config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
      fprintf(stdout, "device not specified, using first one found: %s\n",
              rdma_config.dev_name);
    }
    if (!strcmp(ibv_get_device_name(dev_list[i]), rdma_config.dev_name)) {
      ib_dev = dev_list[i];
      break;
    }
  }
  /* if the device wasn't found in host */
  if (!ib_dev) {
    fprintf(stderr, "IB device %s wasn't found\n", rdma_config.dev_name);
    rc = 1;
  }
  /* get device handle */
  res->ib_ctx = ibv_open_device(ib_dev);
  if (!res->ib_ctx) {
    fprintf(stderr, "failed to open device %s\n", rdma_config.dev_name);
    rc = 1;
  }
  /* We are now done with device list, free it */
  ibv_free_device_list(dev_list);
  dev_list = NULL;
  ib_dev = NULL;
  /* query port properties */
  if (ibv_query_port(res->ib_ctx, rdma_config.ib_port, &res->port_attr)) {
    fprintf(stderr, "ibv_query_port on port %u failed\n", rdma_config.ib_port);
    rc = 1;
  }
  /* allocate Protection Domain */
  res->pd = ibv_alloc_pd(res->ib_ctx);
  if (!res->pd) {
    fprintf(stderr, "ibv_alloc_pd failed\n");
    rc = 1;
  }

  /* computing node allocate local buffers */
//  if (rdma_config.server_name) {
//    ibv_mr* mr;
//    char* buff;
//    if (!Local_Memory_Register(&buff, &mr, rdma_config.init_local_buffer_size,
//                               0)) {
//      fprintf(stderr, "memory registering failed by size of 0x%x\n",
//              static_cast<unsigned>(rdma_config.init_local_buffer_size));
//    } else {
//      fprintf(stdout, "memory registering succeed by size of 0x%x\n",
//              static_cast<unsigned>(rdma_config.init_local_buffer_size));
//    }
//  }
  Local_Memory_Register(&(res->send_buf), &(res->mr_send), 1000, 0);
  Local_Memory_Register(&(res->receive_buf), &(res->mr_receive), 1000, 0);
  //        if(condition){
  //          fprintf(stderr, "Local memory registering failed\n");
  //
  //        }

  fprintf(stdout, "SST buffer, send&receive buffer were registered with a\n");
  rc = ibv_query_device(res->ib_ctx, &(res->device_attr));
  std::cout << "maximum query pair number is" << res->device_attr.max_qp
            << std::endl;
  std::cout << "maximum completion queue number is" << res->device_attr.max_cq
            << std::endl;
  std::cout << "maximum memory region number is" << res->device_attr.max_mr
            << std::endl;
  std::cout << "maximum memory region size is" << res->device_attr.max_mr_size
            << std::endl;

  return rc;
}

bool RDMA_Manager::Client_Connect_to_Server_RDMA() {
  //  int iter = 1;
  char temp_receive[2];
  char temp_send[] = "Q";
  struct registered_qp_config local_con_data;
  struct registered_qp_config remote_con_data;
  struct registered_qp_config tmp_con_data;
  std::string qp_id = "main";
  int rc = 0;

  union ibv_gid my_gid;
  if (rdma_config.gid_idx >= 0) {
    rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port, rdma_config.gid_idx,
                       &my_gid);
    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_config.ib_port, rdma_config.gid_idx);
      return rc;
    }
  } else
    memset(&my_gid, 0, sizeof my_gid);
  /* exchange using TCP sockets info required to connect QPs */
  create_qp(qp_id);
  local_con_data.qp_num = htonl(res->qp_map[qp_id]->qp_num);
  local_con_data.lid = htons(res->port_attr.lid);
  memcpy(local_con_data.gid, &my_gid, 16);
  fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
  if (sock_sync_data(res->sock_map["main"], sizeof(struct registered_qp_config),
                     (char*)&local_con_data, (char*)&tmp_con_data) < 0) {
    fprintf(stderr, "failed to exchange connection data between sides\n");
    rc = 1;
  }
  remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
  remote_con_data.lid = ntohs(tmp_con_data.lid);
  memcpy(remote_con_data.gid, tmp_con_data.gid, 16);

  fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data.qp_num);
  fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data.lid);
  connect_qp(remote_con_data, qp_id);
  if (sock_sync_data(res->sock_map["main"], 1, temp_send,
                     temp_receive)) /* just send a dummy char back and forth */
  {
    fprintf(stderr, "sync error after QPs are were moved to RTS\n");
    rc = 1;
  }
  return false;
}
bool RDMA_Manager::create_qp(std::string& id) {
  struct ibv_qp_init_attr qp_init_attr;

  /* each side will send only one WR, so Completion Queue with 1 entry is enough
   */
  int cq_size = 2500;
  ibv_cq* cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
  if (!cq) {
    fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
  }
  res->cq_map[id] = cq;

  /* create the Queue Pair */
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.sq_sig_all = 0;
  qp_init_attr.send_cq = cq;
  qp_init_attr.recv_cq = cq;
  qp_init_attr.cap.max_send_wr = 2500;
  qp_init_attr.cap.max_recv_wr = 2500;
  qp_init_attr.cap.max_send_sge = 30;
  qp_init_attr.cap.max_recv_sge = 30;
//  qp_init_attr.cap.max_inline_data = -1;
  ibv_qp* qp = ibv_create_qp(res->pd, &qp_init_attr);
  if (!qp) {
    fprintf(stderr, "failed to create QP\n");
  }
  res->qp_map[id] = qp;
  fprintf(stdout, "QP was created, QP number=0x%x\n", res->qp_map[id]->qp_num);

  return true;
}
/******************************************************************************
* Function: connect_qp
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, error code on failure
*
* Description
* Connect the QP. Transition the server side to RTR, sender side to RTS
******************************************************************************/
int RDMA_Manager::connect_qp(registered_qp_config remote_con_data,
                             std::string& qp_id) {
  int rc;

  if (rdma_config.gid_idx >= 0) {
    uint8_t* p = remote_con_data.gid;
    fprintf(stdout,
            "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
            p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
            p[11], p[12], p[13], p[14], p[15]);
  }
  /* modify the QP to init */
  rc = modify_qp_to_init(res->qp_map[qp_id]);
  if (rc) {
    fprintf(stderr, "change QP state to INIT failed\n");
    goto connect_qp_exit;
  }

  /* modify the QP to RTR */
  rc = modify_qp_to_rtr(res->qp_map[qp_id], remote_con_data.qp_num,
                        remote_con_data.lid, remote_con_data.gid);
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTR\n");
    goto connect_qp_exit;
  }
  rc = modify_qp_to_rts(res->qp_map[qp_id]);
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTS\n");
    goto connect_qp_exit;
  }
  fprintf(stdout, "QP %s state was change to RTS\n", qp_id.c_str());
  /* sync to make sure that both sides are in states that they can connect to prevent packet loose */
connect_qp_exit:
  return rc;
}
/******************************************************************************
* Function: modify_qp_to_init
*
* Input
* qp QP to transition
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the RESET to INIT state
******************************************************************************/
int RDMA_Manager::modify_qp_to_init(struct ibv_qp* qp) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.port_num = rdma_config.ib_port;
  attr.pkey_index = 0;
  attr.qp_access_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to INIT\n");
  return rc;
}
/******************************************************************************
* Function: modify_qp_to_rtr
*
* Input
* qp QP to transition
* remote_qpn remote QP number
* dlid destination LID
* dgid destination GID (mandatory for RoCEE)
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the INIT to RTR state, using the specified QP number
******************************************************************************/
int RDMA_Manager::modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn,
                                   uint16_t dlid, uint8_t* dgid) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = IBV_MTU_4096;
  attr.dest_qp_num = remote_qpn;
  attr.rq_psn = 0;
  attr.max_dest_rd_atomic = 1;
  attr.min_rnr_timer = 0xc;
  attr.ah_attr.is_global = 0;
  attr.ah_attr.dlid = dlid;
  attr.ah_attr.sl = 0;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = rdma_config.ib_port;
  if (rdma_config.gid_idx >= 0) {
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = 1;
    memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.hop_limit = 0xFF;
    attr.ah_attr.grh.sgid_index = rdma_config.gid_idx;
    attr.ah_attr.grh.traffic_class = 0;
  }
  flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
          IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to RTR\n");
  return rc;
}
/******************************************************************************
* Function: modify_qp_to_rts
*
* Input
* qp QP to transition
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the RTR to RTS state
******************************************************************************/
int RDMA_Manager::modify_qp_to_rts(struct ibv_qp* qp) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 0xe;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.sq_psn = 0;
  attr.max_rd_atomic = 1;
  flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
          IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to RTS\n");
  return rc;
}
/******************************************************************************
* Function: sock_sync_data
*
* Input
* sock socket to transfer data on
* xfer_size size of data to transfer
* local_data pointer to data to be sent to remote
*
* Output
* remote_data pointer to buffer to receive remote data
*
* Returns
* 0 on success, negative error code on failure
*
* Description
* Sync data across a socket. The indicated local data will be sent to the
* remote. It will then wait for the remote to send its data back. It is
* assumed that the two sides are in sync and call this function in the proper
* order. Chaos will ensue if they are not. :)
*
* Also note this is a blocking function and will wait for the full data to be
* received from the remote.
*
******************************************************************************/
int RDMA_Manager::sock_sync_data(int sock, int xfer_size, char* local_data,
                                 char* remote_data) {
  int rc;
  int read_bytes = 0;
  int total_read_bytes = 0;
  rc = write(sock, local_data, xfer_size);
  if (rc < xfer_size)
    fprintf(stderr,
            "Failed writing data during sock_sync_data, total bytes are %d\n",
            rc);
  else
    rc = 0;
  while (!rc && total_read_bytes < xfer_size) {
    read_bytes = read(sock, remote_data, xfer_size);
    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
  fprintf(stdout, "The data which has been read through is %s size is %d\n",
          remote_data, read_bytes);
  return rc;
}
/******************************************************************************
End of socket operations
******************************************************************************/

// return 0 means success
int
RDMA_Manager::RDMA_Read(ibv_mr *remote_mr, ibv_mr *local_mr, size_t msg_size, std::string q_id, unsigned int send_flag,
                        int poll_num) {
//  auto start = std::chrono::high_resolution_clock::now();
    std::shared_lock<std::shared_mutex> l(main_qp_mutex);
//    auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  std::printf("Read lock time elapse : (%ld)\n",duration.count());

  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)local_mr->addr;
  sge.length = msg_size;
  sge.lkey = local_mr->lkey;
  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_RDMA_READ;
  if (send_flag != 0 )
    sr.send_flags = send_flag;
//  printf("send flag to transform is %u", send_flag);
//  printf("send flag is %u", sr.send_flags);
  sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_mr->addr);
  sr.wr.rdma.rkey = remote_mr->rkey;

  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  std::printf("rdma read  send prepare for (%zu), time elapse : (%ld)\n", msg_size, duration.count());
//  start = std::chrono::high_resolution_clock::now();
  rc = ibv_post_send(res->qp_map.at(q_id), &sr, &bad_wr);
//    std::cout << " " << msg_size << "time elapse :" <<  << std::endl;
//  start = std::chrono::high_resolution_clock::now();

  if (rc) {
      fprintf(stderr, "failed to post SR %s \n", q_id.c_str());
      exit(1);

  }else{
//      printf("qid: %s", q_id.c_str());
  }
  //  else
  //  {
  //    fprintf(stdout, "RDMA Read Request was posted, OPCODE is %d\n", sr.opcode);
  //  }
  if (poll_num != 0){
      ibv_wc* wc = new ibv_wc[poll_num]();
      //  auto start = std::chrono::high_resolution_clock::now();
      //  while(std::chrono::high_resolution_clock::now
      //  ()-start < std::chrono::nanoseconds(msg_size+200000));
      rc = poll_completion(wc, poll_num, q_id);
      if (rc != 0) {
          std::cout << "RDMA Read Failed" << std::endl;
          std::cout << "q id is" << q_id << std::endl;
          fprintf(stdout, "QP number=0x%x\n", res->qp_map[q_id]->qp_num);
      }
      delete[] wc;
  }



//  stop = std::chrono::high_resolution_clock::now();
//  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("RDMA READ and poll: %zu elapse: %ld\n", msg_size, duration.count());
  return rc;
}
int RDMA_Manager::RDMA_Write(ibv_mr* remote_mr, ibv_mr* local_mr,
                             size_t msg_size, std::string q_id, unsigned int send_flag,
                             int poll_num) {
//  auto start = std::chrono::high_resolution_clock::now();
    std::shared_lock<std::shared_mutex> l(main_qp_mutex);

  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)local_mr->addr;
  sge.length = msg_size;
  sge.lkey = local_mr->lkey;
  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_RDMA_WRITE;
  if (send_flag != 0 )
        sr.send_flags = send_flag;
  sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_mr->addr);
  sr.wr.rdma.rkey = remote_mr->rkey;
  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("RDMA Write send preparation size: %zu elapse: %ld\n", msg_size, duration.count());
//  start = std::chrono::high_resolution_clock::now();
  rc = ibv_post_send(res->qp_map.at(q_id), &sr, &bad_wr);

  //  start = std::chrono::high_resolution_clock::now();
  if (rc) fprintf(stderr, "failed to post SR\n");
  //  else
  //  {
  //    fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
  //  }
  if (poll_num != 0){
        ibv_wc* wc = new ibv_wc[poll_num]();
      //  auto start = std::chrono::high_resolution_clock::now();
      //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
      // wait until the job complete.
      rc = poll_completion(wc, poll_num, q_id);
      if (rc != 0) {
        std::cout << "RDMA Write Failed" << std::endl;
        std::cout << "q id is" << q_id << std::endl;
        fprintf(stdout, "QP number=0x%x\n", res->qp_map[q_id]->qp_num);
      }
      delete []  wc;
  }
//  stop = std::chrono::high_resolution_clock::now();
//  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("RDMA Write post send and poll size: %zu elapse: %ld\n", msg_size, duration.count());
  return rc;
}
// int RDMA_Manager::post_atomic(int opcode)
//{
//  struct ibv_send_wr sr;
//  struct ibv_sge sge;
//  struct ibv_send_wr* bad_wr = NULL;
//  int rc;
//  extern int msg_size;
//  /* prepare the scatter/gather entry */
//  memset(&sge, 0, sizeof(sge));
//  sge.addr = (uintptr_t)res->send_buf;
//  sge.length = msg_size;
//  sge.lkey = res->mr_receive->lkey;
//  /* prepare the send work request */
//  memset(&sr, 0, sizeof(sr));
//  sr.next = NULL;
//  sr.wr_id = 0;
//  sr.sg_list = &sge;
//  sr.num_sge = 1;
//  sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
//  sr.send_flags = IBV_SEND_SIGNALED;
//  if (opcode != IBV_WR_SEND)
//  {
//    sr.wr.rdma.remote_addr = res->mem_regions.addr;
//    sr.wr.rdma.rkey = res->mem_regions.rkey;
//  }
//  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
//  //*(start) = std::chrono::steady_clock::now();
//  //start = std::chrono::steady_clock::now();
//  rc = ibv_post_send(res->qp, &sr, &bad_wr);
//  if (rc)
//    fprintf(stderr, "failed to post SR\n");
//  else
//  {
//    /*switch (opcode)
//    {
//    case IBV_WR_SEND:
//            fprintf(stdout, "Send Request was posted\n");
//            break;
//    case IBV_WR_RDMA_READ:
//            fprintf(stdout, "RDMA Read Request was posted\n");
//            break;
//    case IBV_WR_RDMA_WRITE:
//            fprintf(stdout, "RDMA Write Request was posted\n");
//            break;
//    default:
//            fprintf(stdout, "Unknown Request was posted\n");
//            break;
//    }*/
//  }
//  return rc;
//}
/******************************************************************************
* Function: post_send
*
* Input
* res pointer to resources structure
* opcode IBV_WR_SEND, IBV_WR_RDMA_READ or IBV_WR_RDMA_WRITE
*
* Output
* none
*
* Returns
* 0 on success, error code on failure
*
* Description
* This function will create and post a send work request
******************************************************************************/
template <typename T>
int RDMA_Manager::post_send(ibv_mr* mr, std::string qp_id) {
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  if (!rdma_config.server_name) {
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)mr->addr;
    sge.length = sizeof(T);
    sge.lkey = mr->lkey;
  } else {
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)res->send_buf;
    sge.length = sizeof(T);
    sge.lkey = res->mr_send->lkey;
  }

  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
  sr.send_flags = IBV_SEND_SIGNALED;

  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();

  if (rdma_config.server_name)
    rc = ibv_post_send(res->qp_map["main"], &sr, &bad_wr);
  else
    rc = ibv_post_send(res->qp_map[qp_id], &sr, &bad_wr);
  if (rc)
    fprintf(stderr, "failed to post SR\n");
  else {
    fprintf(stdout, "Send Request was posted\n");
  }
  return rc;
}

/******************************************************************************
* Function: post_receive
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, error code on failure
*
* Description
*
******************************************************************************/
// TODO: Add templete for post send and post receive, making the type of transfer data configurable.
template <typename T>
int RDMA_Manager::post_receive(ibv_mr* mr, std::string qp_id) {
  struct ibv_recv_wr rr;
  struct ibv_sge sge;
  struct ibv_recv_wr* bad_wr;
  int rc;
  if (!rdma_config.server_name) {
    /* prepare the scatter/gather entry */

    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)mr->addr;
    sge.length = sizeof(T);
    sge.lkey = mr->lkey;

  } else {
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)res->receive_buf;
    sge.length = sizeof(T);
    sge.lkey = res->mr_receive->lkey;
  }

  /* prepare the receive work request */
  memset(&rr, 0, sizeof(rr));
  rr.next = NULL;
  rr.wr_id = 0;
  rr.sg_list = &sge;
  rr.num_sge = 1;
  /* post the Receive Request to the RQ */
  if (rdma_config.server_name)
    rc = ibv_post_recv(res->qp_map["main"], &rr, &bad_wr);
  else
    rc = ibv_post_recv(res->qp_map[qp_id], &rr, &bad_wr);
  if (rc)
    fprintf(stderr, "failed to post RR\n");
  else
    fprintf(stdout, "Receive Request was posted\n");
  return rc;
}
/* poll_completion */
/******************************************************************************
* Function: poll_completion
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, 1 on failure
*
* Description
* Poll the completion queue for a single event. This function will continue to
* poll the queue until MAX_POLL_CQ_TIMEOUT milliseconds have passed.
*
******************************************************************************/
int RDMA_Manager::poll_completion(ibv_wc* wc_p, int num_entries,
                                  std::string q_id) {
  // unsigned long start_time_msec;
  // unsigned long cur_time_msec;
  // struct timeval cur_time;
  int poll_result;
  int poll_num = 0;
  int rc = 0;
  /* poll the completion for a while before giving up of doing it .. */
  // gettimeofday(&cur_time, NULL);
  // start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
  do {
    poll_result = ibv_poll_cq(res->cq_map.at(q_id), num_entries, wc_p);
    if (poll_result < 0)
      break;
    else
      poll_num = poll_num + poll_result;
    /*gettimeofday(&cur_time, NULL);
    cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);*/
  } while (poll_num < num_entries);  // && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
  //*(end) = std::chrono::steady_clock::now();
  // end = std::chrono::steady_clock::now();
  if (poll_result < 0) {
    /* poll CQ failed */
    fprintf(stderr, "poll CQ failed\n");
    rc = 1;
  } else if (poll_result == 0) { /* the CQ is empty */
    fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
    rc = 1;
  } else {
    /* CQE found */
    // fprintf(stdout, "completion was found in CQ with status 0x%x\n", wc.status);
    /* check the completion status (here we don't care about the completion opcode */
    if (wc_p->status != IBV_WC_SUCCESS)  // TODO:: could be modified into check all the entries in the array
    {
      fprintf(stderr,
              "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
              wc_p->status, wc_p->vendor_err);
      rc = 1;
    }
  }
  return rc;
}

/******************************************************************************
* Function: print_config
*
* Input
* none
*
* Output
* none
*
* Returns
* none
*
* Description
* Print out config information
******************************************************************************/
void RDMA_Manager::print_config(void) {
  fprintf(stdout, " ------------------------------------------------\n");
  fprintf(stdout, " Device name : \"%s\"\n", rdma_config.dev_name);
  fprintf(stdout, " IB port : %u\n", rdma_config.ib_port);
  if (rdma_config.server_name)
    fprintf(stdout, " IP : %s\n", rdma_config.server_name);
  fprintf(stdout, " TCP port : %u\n", rdma_config.tcp_port);
  if (rdma_config.gid_idx >= 0)
    fprintf(stdout, " GID index : %u\n", rdma_config.gid_idx);
  fprintf(stdout, " ------------------------------------------------\n\n");
}

/******************************************************************************
* Function: usage
*
* Input
* argv0 command line arguments
*
* Output
* none
*
* Returns
* none
*
* Description
* print a description of command line syntax
******************************************************************************/
void RDMA_Manager::usage(const char* argv0) {
  fprintf(stdout, "Usage:\n");
  fprintf(stdout, " %s start a server and wait for connection\n", argv0);
  fprintf(stdout, " %s <host> connect to server at <host>\n", argv0);
  fprintf(stdout, "\n");
  fprintf(stdout, "Options:\n");
  fprintf(
      stdout,
      " -p, --port <port> listen on/connect to port <port> (default 18515)\n");
  fprintf(
      stdout,
      " -d, --ib-dev <dev> use IB device <dev> (default first device found)\n");
  fprintf(stdout,
          " -i, --ib-port <port> use port <port> of IB device (default 1)\n");
  fprintf(stdout,
          " -g, --gid_idx <git index> gid index to be used in GRH (default not used)\n");
}

bool RDMA_Manager::Remote_Memory_Register(size_t size) {
  std::unique_lock<std::shared_mutex> l(main_qp_mutex);
  // register the memory block from the remote memory
  computing_to_memory_msg* send_pointer;
  send_pointer = (computing_to_memory_msg*)res->send_buf;
  send_pointer->command = create_mr_;
  send_pointer->content.mem_size = size;
  ibv_mr* receive_pointer;
  receive_pointer = (ibv_mr*)res->receive_buf;
  post_receive<ibv_mr>(res->mr_receive, std::string("main"));
  post_send<computing_to_memory_msg>(res->mr_send, std::string("main"));
  ibv_wc wc[2] = {};
  //  while(wc.opcode != IBV_WC_RECV){
  //    poll_completion(&wc);
  //    if (wc.status != 0){
  //      fprintf(stderr, "Work completion status is %d \n", wc.status);
  //    }
  //
  //  }
  //  assert(wc.opcode == IBV_WC_RECV);

  if (!poll_completion(wc, 2, std::string("main"))) {  // poll the receive for 2 entires
    auto* temp_pointer = new ibv_mr();
    // Memory leak?, No, the ibv_mr pointer will be push to the remote mem pool,
    // Please remember to delete it when diregistering mem region from the remote memory
    *temp_pointer = *receive_pointer;  // create a new ibv_mr for storing the new remote memory region handler
    remote_mem_pool.push_back(temp_pointer);  // push the new pointer for the new ibv_mr (different from the receive buffer) to remote_mem_pool

    // push the bitmap of the new registed buffer to the bitmap vector in resource.
    int placeholder_num =static_cast<int>(temp_pointer->length) /(Table_Size);  // here we supposing the SSTables are 4 megabytes
    In_Use_Array in_use_array(placeholder_num, Table_Size);
//    std::unique_lock l(remote_pool_mutex);
    Remote_Mem_Bitmap->insert({temp_pointer, in_use_array});
//    l.unlock();

    // NOTICE: Couold be problematic because the pushback may not an absolute
    //   value copy. it could raise a segment fault(Double check it)
  } else {
    fprintf(stderr, "failed to poll receive for remote memory register\n");
    return false;
  }

  return true;
}
bool RDMA_Manager::Remote_Query_Pair_Connection(std::string& qp_id) {
  std::unique_lock<std::shared_mutex> l(main_qp_mutex);
  create_qp(qp_id);
  union ibv_gid my_gid;
  int rc;
  if (rdma_config.gid_idx >= 0) {
    rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port, rdma_config.gid_idx,
                       &my_gid);

    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_config.ib_port, rdma_config.gid_idx);
      return false;
    }
  } else
    memset(&my_gid, 0, sizeof my_gid);

  computing_to_memory_msg* send_pointer;
  send_pointer = (computing_to_memory_msg*)res->send_buf;
  send_pointer->command = create_qp_;
  send_pointer->content.qp_config.qp_num = res->qp_map[qp_id]->qp_num;
  send_pointer->content.qp_config.lid = res->port_attr.lid;
  memcpy(send_pointer->content.qp_config.gid, &my_gid, 16);
  fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
  registered_qp_config* receive_pointer;
  receive_pointer = (registered_qp_config*)res->receive_buf;
  post_receive<registered_qp_config>(res->mr_receive, std::string("main"));
  post_send<computing_to_memory_msg>(res->mr_send, std::string("main"));
  ibv_wc wc[2] = {};
  //  while(wc.opcode != IBV_WC_RECV){
  //    poll_completion(&wc);
  //    if (wc.status != 0){
  //      fprintf(stderr, "Work completion status is %d \n", wc.status);
  //    }
  //
  //  }
  //  assert(wc.opcode == IBV_WC_RECV);

  if (!poll_completion(wc, 2, std::string("main"))) {
    // poll the receive for 2 entires
    registered_qp_config temp_buff = *receive_pointer;
    fprintf(stdout, "Remote QP number=0x%x\n", receive_pointer->qp_num);
    fprintf(stdout, "Remote LID = 0x%x\n", receive_pointer->lid);
    // te,p_buff will have the informatin for the remote query pair,
    // use this information for qp connection.
    connect_qp(temp_buff, qp_id);
    return true;
  } else
    return false;
  //  // sync the communication by rdma.
  //  post_receive<registered_qp_config>(receive_pointer, std::string("main"));
  //  post_send<computing_to_memory_msg>(send_pointer, std::string("main"));
  //  if(!poll_completion(wc, 2, std::string("main"))){
  //    return true;
  //  }else
  //    return false;
}

void RDMA_Manager::Allocate_Remote_RDMA_Slot(const std::string& file_name,
                                             SST_Metadata*& sst_meta) {
  // If the Remote buffer is empty, register one from the remote memory.
  sst_meta = new SST_Metadata;
  sst_meta->mr = new ibv_mr;
  if (Remote_Mem_Bitmap->empty()) {
    // this lock is to prevent the system register too much remote memory at the
    // begginning.
    std::unique_lock<std::shared_mutex> mem_write_lock(remote_mem_create_mutex);
    if (Remote_Mem_Bitmap->empty()) {
      Remote_Memory_Register(1 * 1024 * 1024 * 1024);
    }
    mem_write_lock.unlock();
  }
  std::shared_lock<std::shared_mutex> mem_read_lock(remote_mem_create_mutex);
  auto ptr = Remote_Mem_Bitmap->begin();

  while (ptr != Remote_Mem_Bitmap->end()) {
    // iterate among all the remote memory region
    // find the first empty SSTable Placeholder's iterator, iterator->first is ibv_mr*
    // second is the bool vector for this ibv_mr*. Each ibv_mr is the origin block get
    // from the remote memory. The memory was divided into chunks with size == SSTable size.
    int sst_index = ptr->second.allocate_memory_slot();
    if (sst_index >= 0) {
      *(sst_meta->mr) = *(ptr->first);
      sst_meta->mr->addr = static_cast<void*>(
          static_cast<char*>(sst_meta->mr->addr) + sst_index * Table_Size);
      sst_meta->mr->length = Table_Size;
      sst_meta->fname = file_name;
      sst_meta->map_pointer =
          ptr->first;  // it could be confused that the map_pointer is for the memtadata deletion
      // so that we can easily find where to deallocate our RDMA buffer. The key is a pointer to ibv_mr.
      sst_meta->file_size = 0;
#ifndef NDEBUG
//      std::cout <<"Chunk allocate at" << sst_meta->mr->addr <<"index :" << sst_index << "name: " << sst_meta->fname << std::endl;
#endif
      return;
    } else
      ptr++;
  }
  mem_read_lock.unlock();
  // If not find remote buffers are all used, allocate another remote memory region.
  std::unique_lock<std::shared_mutex> mem_write_lock(remote_mem_create_mutex);
  Remote_Memory_Register(1 * 1024 * 1024 * 1024);
  ibv_mr* mr_last;
  mr_last = remote_mem_pool.back();
  int sst_index = Remote_Mem_Bitmap->at(mr_last).allocate_memory_slot();
  mem_write_lock.unlock();

  //  sst_meta->mr = new ibv_mr();
  *(sst_meta->mr) = *(mr_last);
  sst_meta->mr->addr = static_cast<void*>(
      static_cast<char*>(sst_meta->mr->addr) + sst_index * Table_Size);
  sst_meta->mr->length = Table_Size;
  sst_meta->fname = file_name;
  sst_meta->map_pointer = mr_last;
  return;
}
// A function try to allocat
void RDMA_Manager::Allocate_Local_RDMA_Slot(ibv_mr*& mr_input,
                                            ibv_mr*& map_pointer,
                                            std::string buffer_type) {
  // allocate the RDMA slot is seperate into two situation, read and write.
  size_t chunk_size;
  if (buffer_type == "write") {
    chunk_size = Write_Block_Size;
    if (Write_Local_Mem_Bitmap->empty()) {
      std::unique_lock<std::shared_mutex> mem_write_lock(local_mem_create_mutex);
      if (Write_Local_Mem_Bitmap->empty()) {
        ibv_mr* mr;
        char* buff;
        Local_Memory_Register(&buff, &mr, 256 * chunk_size, chunk_size);
      }
      mem_write_lock.unlock();
    }
    std::shared_lock<std::shared_mutex> mem_read_lock(local_mem_create_mutex);
    auto ptr = Write_Local_Mem_Bitmap->begin();

    while (ptr != Write_Local_Mem_Bitmap->end()) {
      size_t region_chunk_size = ptr->second.get_chunk_size();
      if (region_chunk_size != chunk_size) {
        ptr++;
        continue;
      }
      int block_index = ptr->second.allocate_memory_slot();
      if (block_index >= 0) {
        mr_input = new ibv_mr();
        map_pointer = ptr->first;
        *(mr_input) = *(ptr->first);
        mr_input->addr = static_cast<void*>(static_cast<char*>(mr_input->addr) +
                                            block_index * chunk_size);
        mr_input->length = chunk_size;

        return;
      } else
        ptr++;
    }
    mem_read_lock.unlock();
    // if not find available Local block buffer then allocate a new buffer. then
    // pick up one buffer from the new Local memory region.
    // TODO:: It could happen that the local buffer size is not enough, need to reallocate a new buff again,
    // TODO:: Because there are two many thread going on at the same time.
    ibv_mr* mr_to_allocate = new ibv_mr();
    char* buff = new char[chunk_size];

    std::unique_lock<std::shared_mutex> mem_write_lock(local_mem_create_mutex);
    Local_Memory_Register(&buff, &mr_to_allocate, chunk_size * 32, chunk_size);


    int block_index = Write_Local_Mem_Bitmap->at(mr_to_allocate).allocate_memory_slot();
    mem_write_lock.unlock();
    if (block_index >= 0) {
      mr_input = new ibv_mr();
      map_pointer = mr_to_allocate;
      *(mr_input) = *(mr_to_allocate);
      mr_input->addr = static_cast<void*>(static_cast<char*>(mr_input->addr) +
                                          block_index * chunk_size);
      mr_input->length = chunk_size;
      //  mr_input.fname = file_name;
      return;
    }
  }
  else if (buffer_type == "read"){
    chunk_size = Read_Block_Size;
    if (Read_Local_Mem_Bitmap->empty()) {
        std::unique_lock<std::shared_mutex> mem_write_lock(local_mem_create_mutex);
      if (Read_Local_Mem_Bitmap->empty()) {
        ibv_mr* mr;
        char* buff;
        Local_Memory_Register(&buff, &mr, 256*chunk_size, chunk_size);
      }
      mem_write_lock.unlock();
    }
    std::shared_lock<std::shared_mutex> mem_read_lock(local_mem_create_mutex);
    auto ptr = Read_Local_Mem_Bitmap->begin();
    while (ptr != Read_Local_Mem_Bitmap->end()) {
      size_t region_chunk_size = ptr->second.get_chunk_size();
      if (region_chunk_size != chunk_size){
        ptr++;
        continue;
      }
      int block_index = ptr->second.allocate_memory_slot();
      if (block_index >= 0) {
        mr_input = new ibv_mr();
        map_pointer = ptr->first;
        *(mr_input) = *(ptr->first);
        mr_input->addr = static_cast<void*>(static_cast<char*>(mr_input->addr) +
                                            block_index * chunk_size);

        return;

      } else
        ptr++;
    }
    mem_read_lock.unlock();
    // if not find available Local block buffer then allocate a new buffer. then
    // pick up one buffer from the new Local memory region.
    // TODO:: It could happen that the local buffer size is not enough, need to reallocate a new buff again,
    // TODO:: Because there are two many thread going on at the same time.
    ibv_mr* mr_to_allocate;
    char* buff;

    std::unique_lock<std::shared_mutex> mem_write_lock(local_mem_create_mutex);
    Local_Memory_Register(&buff, &mr_to_allocate, chunk_size*50, chunk_size);
    int block_index = Read_Local_Mem_Bitmap->at(mr_to_allocate).allocate_memory_slot();
    mem_write_lock.unlock();

    if (block_index >= 0) {
      mr_input = new ibv_mr();
      map_pointer = mr_to_allocate;
      *(mr_input) = *(mr_to_allocate);
      mr_input->addr = static_cast<void*>(static_cast<char*>(mr_input->addr) +
                                          block_index * chunk_size);
      //  mr_input.fname = file_name;
      return;
    }
  else
    printf("invalid buffer type");
  // Need to develop a mechanism to preallocate the memory in advance.

  }
#ifndef NDEBUG
  else {
    std::cout << "block registerration failed" << std::endl;
  }
#endif
}
// Remeber to delete the mr because it was created be new, otherwise memory leak.
bool RDMA_Manager::Deallocate_Local_RDMA_Slot(ibv_mr* mr, ibv_mr* map_pointer,
                                              std::string buffer_type) const {
  int buff_offset =
      static_cast<char*>(mr->addr) - static_cast<char*>(map_pointer->addr);
  if (buffer_type == "read"){
    assert(buff_offset % Read_Block_Size == 0);
    return Read_Local_Mem_Bitmap->at(map_pointer)
        .deallocate_memory_slot(buff_offset / Read_Block_Size);
  }
  else if (buffer_type == "write"){
    assert(buff_offset % Write_Block_Size == 0);
    return Write_Local_Mem_Bitmap->at(map_pointer)
        .deallocate_memory_slot(buff_offset / Write_Block_Size);
  }
  return false;
}
bool RDMA_Manager::Deallocate_Remote_RDMA_Slot(SST_Metadata* sst_meta) const {
  int buff_offset = static_cast<char*>(sst_meta->mr->addr) -
                    static_cast<char*>(sst_meta->map_pointer->addr);
  assert(buff_offset % Table_Size == 0);
#ifndef NDEBUG
//  std::cout <<"Chunk deallocate at" << sst_meta->mr->addr << "index: " << buff_offset/Table_Size << std::endl;
#endif
  return Remote_Mem_Bitmap->at(sst_meta->map_pointer)
      .deallocate_memory_slot(buff_offset / Table_Size);
}

//}
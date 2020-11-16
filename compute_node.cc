#include <iostream>
#include <pthread.h>
#include <thread>
#include "rdma.h"

void client_thread(RDMA_Manager *rdma_manager, long int &start, long int &end, int iteration, ibv_mr** local_mr_pointer,
                   SST_Metadata** sst_meta, std::string *thread_id, size_t msg_size) {

//    usleep(100);
//printf("thread_id: %s \n", thread_id->c_str());
  start = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  for (int i = 0; i<iteration; i++){
      if ((i+1)%1000 == 0){
          int j = (i+1)%1000;
          rdma_manager->RDMA_Write(sst_meta[j]->mr, local_mr_pointer[j],
                                   msg_size, *thread_id, IBV_SEND_SIGNALED, 1000);
      }else{
          rdma_manager->RDMA_Write(sst_meta[0]->mr, local_mr_pointer[0],
                                   msg_size, *thread_id, IBV_SEND_SIGNALED, 0);
      }

  }
//    rdma_manager->RDMA_Write(sst_meta->mr, local_mr_pointer,
//                            msg_size, *thread_id, IBV_SEND_SIGNALED, iteration);
//  rdma_manager->RDMA_Write(rdma_manager->remote_mem_pool[0], &mem_pool_table[0],
//                           msg_size, *thread_id);

//  rdma_manager->RDMA_Read(rdma_manager->remote_mem_pool[0], &mem_pool_table[1],
//                          msg_size, *thread_id);
//  ibv_wc* wc = new ibv_wc();
//
//    rdma_manager->poll_completion(wc, 2, thread_id);
//    if (wc->status != 0){
//      fprintf(stderr, "Work completion status is %d \n", wc->status);
//      fprintf(stderr, "Work completion opcode is %d \n", wc->opcode);
//    }

    end = std::chrono::high_resolution_clock::now().time_since_epoch().count();
//  time_slot = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();


}
double average(long int *start, long int *end, int n)
{
    long int sum = 0; // initialize average

    // Iterate through all elements
    // and add them to average
    for (int i = 0; i < n; i++)
        sum += end[i] - start[i];

    return sum/(double)n;
}
int main()
{
  struct config_t config = {
      NULL,  /* dev_name */
      NULL,  /* server_name */
      19875, /* tcp_port */
      1,	 /* ib_port */ //physical
      -1, /* gid_idx */
      4*10*1024*1024 /*initial local buffer Chunk_size*/
  };
  auto Remote_Bitmap = new std::unordered_map<ibv_mr*, In_Use_Array>;
  auto Read_Bitmap = new std::unordered_map<ibv_mr*, In_Use_Array>;
  auto Write_Bitmap = new std::unordered_map<ibv_mr*, In_Use_Array>;
  size_t read_block_size = 4*1024;
  size_t write_block_size = 1*1024*1024;
  size_t table_size = 8*1024*1024;
  RDMA_Manager* rdma_manager = new RDMA_Manager(config, Remote_Bitmap, Write_Bitmap, Read_Bitmap,
                             table_size, write_block_size, read_block_size);//  RDMA_Manager rdma_manager(config, Remote_Bitmap, Local_Bitmap);
  rdma_manager->Client_Set_Up_Resources();
  size_t Chunk_size = rdma_manager->Read_Block_Size;
  size_t thread_num = 1;
  ibv_mr* RDMA_mem_chunks[thread_num][1000];
  ibv_mr* RDMA_map[thread_num][1000];
  SST_Metadata* meta_data[thread_num][1000];
    int iteration = 1000000;
//    long int dummy1;
//    long int dummy2;
//    auto start = std::chrono::high_resolution_clock::now();
//    std::thread thread_dummy = std::thread(client_thread, rdma_manager, std::ref(dummy1), std::ref(dummy2), nullptr,
//                                           nullptr, nullptr, 0);
//    thread_dummy.join();
//    auto end = std::chrono::high_resolution_clock::now();
//    std::cout << "thread create time elapse:" << std::chrono::duration_cast<std::chrono::nanoseconds>(start - end).count() << std::endl;
  long int starts[thread_num];
  long int ends[thread_num];
//  long temp;
  std::thread thread_object[thread_num];
//  ibv_mr* mr_pointer_array[thread_num];
//  ibv_mr* map_mr_pointer_array[thread_num];
//  SST_Metadata* meta_data_array[thread_num];
  std::basic_string<char> name[thread_num];
    for (size_t i = 0; i < thread_num; i++){
//        SST_Metadata* sst_meta;
        name[i] = std::to_string(i);
        rdma_manager->Remote_Query_Pair_Connection(name[i]);
        for(size_t j= 0; j< thread_num; j++){
            rdma_manager->Allocate_Remote_RDMA_Slot(name[i], meta_data[i][j]);

            rdma_manager->Allocate_Local_RDMA_Slot(RDMA_mem_chunks[i][j], RDMA_map[i][j], std::string("read"));
            size_t msg_size = Chunk_size;
            memset(RDMA_mem_chunks[i][j]->addr,1,msg_size);
        }

    }
  for (size_t i = 0; i < thread_num; i++){
//      std::string name = std::to_string(i);
      thread_object[i] = std::thread(client_thread, rdma_manager, std::ref(starts[i]), std::ref(ends[i]), iteration, RDMA_mem_chunks[i],
                                     meta_data[i], &(name[i]), Chunk_size);
//      for (auto s = 0; s<1000; s++);// spin for some time
//      thread_object[i].detach();
  }
//    client_thread(rdma_manager, std::ref(starts[1]), std::ref(ends[1]), iteration, mr_pointer_array[1],
//                  meta_data_array[1], std::to_string(1), Chunk_size);
  for (size_t i = 0; i < thread_num; i++){
      thread_object[i].join();
  }
//    usleep(100000000);
    auto max = ends[0];
    for (size_t i = 0; i < thread_num; i++)
    {
        if (max < ends[i])
            max = ends[i];
    }
    auto min = starts[0];
    for (size_t i = 0; i < thread_num; i++)
    {
        if (min > starts[i])
            min = starts[i];
    }
//    average(starts, ends, thread_num)
  double bandwidth = ((double)Chunk_size*thread_num*iteration) / (max-min) * 1000;
  double latency = ((double) (max-min)) / (thread_num * iteration);
  std::cout << (max-min) << std::endl;
  std::cout << "Size: " << Chunk_size << "Bandwidth is " << bandwidth << "MB/s" << std::endl;
  std::cout << "Size: " << Chunk_size << "Dummy latency is " << latency << "ns" << std::endl;
  return 0;
}

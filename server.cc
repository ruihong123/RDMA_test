
#include <iostream>
#include "rdma.h"

int main()
{
    auto Remote_Bitmap = new std::unordered_map<ibv_mr*, In_Use_Array>;
    auto Read_Bitmap = new std::unordered_map<ibv_mr*, In_Use_Array>;
    auto Write_Bitmap = new std::unordered_map<ibv_mr*, In_Use_Array>;
    struct config_t config = {
            NULL,  /* dev_name */
            NULL,  /* server_name */
            19875, /* tcp_port */
            1,	 /* ib_port */
            -1, /* gid_idx */
            0};
    size_t write_block_size = 4*1024*1024;
    size_t read_block_size = 4*1024;
    size_t table_size = 10*1024*1024;
    RDMA_Manager RDMA_manager(config, Remote_Bitmap, Write_Bitmap, Read_Bitmap, table_size,
                                       write_block_size, read_block_size);

    RDMA_manager.Server_to_Client_Communication();


    return 0;
}

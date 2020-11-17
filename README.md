# RDMA_test
## This script can test the multithread performance for RDMA Read and write.
To run the test, you need two have two machines with RDMA Requirement.<br>
Follow the guide of the print to configure your test.<br>
For read or write, 0 represent read, others represent write.
## Use CMAKE to compile the file.
mkdir build<br>
cd build<br>
cmake ..<br>
make RDMA_test_server RDMA_test_client

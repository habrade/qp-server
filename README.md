# RDMA QP Server

This project contains a simple RDMA application written in C++ that configures a Queue Pair, posts receive buffers and polls a completion queue in a dedicated thread. It is primarily intended as an example for experimenting with RDMA transfers from an FPGA or other remote peer.

## Prerequisites

The program depends on the RDMA userspace libraries provided by `libibverbs` and the `rdma-core` package. A C++17 compiler, CMake and Make are also required.

On Ubuntu these packages can be installed with:

```bash
sudo apt-get install build-essential cmake libibverbs-dev rdma-core
```

## Building

Build the executable using CMake:

```bash
mkdir build && cd build
cmake ..
make
```

The resulting binary `rdma_app` will be placed in the `build` directory.

## Command line parameters

The application accepts several parameters to configure the RDMA connection:

```
--device <name>       RDMA device name (default: rocep94s0f1)
--port <num>         RDMA port number (default: 1)
--sgid_idx <idx>     Local SGID index for RoCE v2 (default: 3 – verify for your device)
--remote_ip <ip>     Remote/FPGA IP address (default: 192.168.160.32)
--remote_qpn <qpn>   Remote QPN in hex or decimal (default: 0x100)
--remote_psn <psn>   Remote initial PSN (default: 0)
--local_psn <psn>    Local initial SQ PSN (default: 0)
--buffer_size <B>    Size of the main receive buffer (default: 3221225472)
--num_wrs <num>      Number of posted receive WRs (default: 32)
--msg_size <B>       Size of each receive slice (default: 104857600)
--mtu <256|512|1024|2048|4096>  Path MTU (default: 4096)
--recv_op <send|write> Operation used by the peer to transmit data (default: write)
--write_file         Stream received data directly to a file
--debug              Enable verbose debug logging
-h, --help           Show usage information
```

The defaults correspond to the constants defined in `src/rdma_manager.h`.

## Example

To run a test where the peer writes data to this host using RDMA Write with Immediate:

```bash
./rdma_app --recv_op write --remote_ip 192.168.160.32 --remote_qpn 0x100
```

During operation the completion queue polling thread prints throughput statistics when no new data has arrived for a few seconds. Typical output looks like:

```
Data received: <MB> MB in <seconds> s (<MB/s> MB/s).
Total messages processed: <count>, total bytes processed: <bytes>
```

On startup the application prints port details including the negotiated link rate based on the active speed and width. This helps verify that the connection is running at the expected line rate.

Use `Ctrl+C` to stop the application. Connection parameters are also written to `rdma_params.json` for use by the remote peer.

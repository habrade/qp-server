#ifndef RDMA_MANAGER_H
#define RDMA_MANAGER_H

#include <infiniband/verbs.h>
#include <string>
#include <vector>
#include <deque>
#include <atomic>
#include <thread>
#include <stdexcept> // For std::runtime_error
#include <cstdint>   // For uintX_t types
#include <cstdio>    // For FILE, fopen, etc.
#include <chrono>    // For timing measurements

// Default configurations (can be overridden by main.cpp arguments)
// Size of each receive slice. The application will run long tests with up to
// 50MB messages, so allocate enough space per WR by default to hold the
// largest possible transfer from the FPGA.  The value can still be reduced via
// command line if smaller buffers are desired.
constexpr size_t DEFAULT_RECV_BUFFER_SLICE_SIZE_H = (100ull * 1024 * 1024); // 100MB per WR

// Number of receive work requests to keep posted. The total buffer size must be
// large enough to contain all these slices simultaneously.
constexpr int DEFAULT_NUM_RECV_WRS_H = 32;          // Match the sender's queue

// Main receive buffer size. By default allocate space for all slices to fit at
// once. This avoids immediate RNR conditions if the FPGA issues many sends
// quickly and also prevents the initialization check from failing.
constexpr size_t DEFAULT_BUFFER_SIZE_H =
    DEFAULT_RECV_BUFFER_SLICE_SIZE_H * DEFAULT_NUM_RECV_WRS_H; // 3.2GB
constexpr const char* DEFAULT_OUTPUT_FILENAME_H = "fpga_received_data_cpp.bin";
constexpr const char* DEFAULT_PARAMS_FILENAME_H = "rdma_params.json";
constexpr int DEFAULT_CQ_SIZE_H = DEFAULT_NUM_RECV_WRS_H * 2; // Recommended CQ size relative to WRs

// Structure to manage individual receive buffer slots within a larger registered MR
struct RecvBufferSlot {
    char* ptr;                  // Pointer to the start of this slice in the main buffer
    struct ibv_mr *mr_parent;   // Pointer to the parent MR (the large registered buffer)
    uint64_t wr_id;             // Work Request ID, typically the index of this slot
    size_t slice_size;          // Size of this buffer slice
};

// Structure to hold parameters for the remote QP
struct RemoteQPParams {
    std::string ip_str;         // Remote IP address string
    uint32_t qpn;               // Remote QP Number
    uint32_t initial_psn;       // Initial PSN expected from remote (for local QP's RTR state)
                                // or remote's initial PSN (if local QP is sending to it)
};

// Type of RDMA operation used by the peer to deliver data
enum class RecvOpType {
    WRITE, // RDMA Write with Immediate
    SEND   // RDMA Send
};

class RdmaManager {
public:
    // Constructor
    RdmaManager(const std::string& dev_name,
                int ib_port,
                uint8_t sgid_idx,                   // Local SGID index, CRITICAL for RoCE path resolution
                const RemoteQPParams& remote_params,  // Parameters of the remote peer (FPGA)
                uint32_t local_qpn_hint = 0,        // Hint for local QPN (0 means system assigned)
                uint32_t initial_local_sq_psn = 0,  // Initial PSN for local Send Queue
                size_t buffer_sz = DEFAULT_BUFFER_SIZE_H,
                int num_recv_wrs = DEFAULT_NUM_RECV_WRS_H,
                size_t recv_slice_sz = DEFAULT_RECV_BUFFER_SLICE_SIZE_H,
                enum ibv_mtu path_mtu = IBV_MTU_4096,
                bool write_immediately = false,
                RecvOpType recv_op = RecvOpType::WRITE);
    
    // Destructor (handles resource cleanup via RAII)
    ~RdmaManager();

    // Disable copy and assignment
    RdmaManager(const RdmaManager&) = delete;
    RdmaManager& operator=(const RdmaManager&) = delete;

    // Public interface
    bool initialize_resources();        // Initializes all RDMA resources (device, PD, MR, CQ, QP)
    bool setup_qp_to_rts();             // Transitions the local QP to RTS state
    
    void start_cq_polling_thread();     // Starts the CQ polling thread
    void stop_cq_polling_thread();      // Signals and joins the CQ polling thread
    void request_shutdown_flag();       // Method to be called by signal handler to request shutdown
    bool is_shutdown_requested() const; // Getter for the shutdown flag

    bool post_all_initial_recv_wrs();        // Posts receive WRs for all available slots

    // Prints throughput statistics based on recorded timestamps and bytes
    void print_performance_stats() const;

    // Write connection parameters (rkey, qpn, addresses, etc.) to a JSON file
    bool write_params_to_json(const char* filename, size_t msg_size) const;


    // Convert an ibv_mtu enumeration to the corresponding byte value.  This
    // helper is public so callers outside the class can easily print or use the
    // configured MTU size.
    static int mtu_enum_to_value(enum ibv_mtu mtu);

    // Only the most recent messages are kept in memory when write_immediately
    // is disabled. Exposed here for help text and diagnostics.
    static constexpr size_t MAX_STORED_MSGS = 100;


private:
    // RDMA resources
    struct ibv_context* m_context;
    struct ibv_pd* m_pd;              // Protection Domain
    struct ibv_cq* m_cq;              // Completion Queue
    struct ibv_qp* m_qp;              // Queue Pair
    
    char* m_main_buffer_ptr;          // Pointer to the large allocated memory block for receives
    struct ibv_mr* m_main_mr;         // Memory Region for the main_buffer_ptr
    std::vector<RecvBufferSlot> m_recv_slots; // Manages slices of the main_buffer for receive WRs

    // Configuration parameters
    std::string m_device_name;
    int m_ib_port;
    uint8_t m_local_sgid_index; 
    enum ibv_mtu m_path_mtu;

    RemoteQPParams m_remote_qp_params;
    uint32_t m_local_qpn;             // Actual local QPN (assigned by system or from hint)
    uint32_t m_initial_local_sq_psn;  // Initial PSN for this QP's Send Queue

    // Buffer configuration
    size_t m_buffer_size_actual;
    int m_num_recv_wrs_actual;
    size_t m_recv_slice_size_actual;
    int m_cq_size_actual;

    // State and Threading
    std::atomic<bool> m_shutdown_requested; // Flag to signal shutdown to threads/loops
    std::atomic<bool> m_qp_in_error_state;  // Flag indicating QP is in error
    std::thread m_cq_thread;                // Thread object for CQ polling

    // Statistics and limited storage for received data (from RECV_RDMA_WITH_IMM)
    size_t m_total_recv_msgs{0};
    size_t m_total_recv_bytes{0};
    // Only the most recent messages are kept in memory when write_immediately is false
    std::deque<std::vector<char>> m_recent_received_data;

    // Timing information for throughput calculation
    std::chrono::steady_clock::time_point m_first_recv_ts;
    std::chrono::steady_clock::time_point m_last_recv_ts;
    bool m_first_ts_recorded{false};

    // Idle timeout before computing final throughput in the polling thread
    std::chrono::steady_clock::duration m_idle_timeout{std::chrono::seconds(5)};
    bool m_stats_printed{false};

    bool m_write_immediately{false};
    RecvOpType m_recv_op_type{RecvOpType::WRITE};

    bool dump_all_received_data_to_file(const char* filename) const; // Dumps only messages still held in memory

    // Internal helper methods for resource management and QP state transitions
    bool query_port_attributes();
    bool register_memory_region();
    bool create_completion_queue();
    bool create_queue_pair(uint32_t qpn_hint); // qpn_hint is usually ignored by standard verbs
    bool transition_to_init();
    bool transition_to_rtr();
    bool transition_to_rts();
    bool try_reset_and_reinit_qp();          // Tries to reset QP from ERR state back to RTS
    bool post_single_recv(uint64_t wr_id_idx); // Posts a single receive WR for a given slot
    void process_work_completion(struct ibv_wc* wc, FILE* outfile); // Handles a single work completion
    
    void cq_poll_loop_func();                // The function executed by the CQ polling thread

    // Static GID conversion helper (could also be a free function)
    static int str_to_gid(const char *ip_str, union ibv_gid *gid);
};

#endif // RDMA_MANAGER_H

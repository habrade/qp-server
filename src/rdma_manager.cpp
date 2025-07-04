#include "rdma_manager.h"
#include <atomic>

// Extern pointer to allow destructor to clear global instance
extern std::atomic<RdmaManager*> g_app_rdma_manager_instance_ptr;
#include <iostream>   // For std::cout, std::cerr, std::endl
#include <cstring>    // For memset, memcpy, strerror
#include <cerrno>     // For errno
#include <sys/mman.h> // For mmap, munmap, MAP_HUGETLB
#include <arpa/inet.h> // For inet_pton
#include <poll.h>      // For poll()
#include <unistd.h>   // For usleep, sysconf, write (in signal handler)
#include <cstdlib>    // For aligned_alloc, free, exit (if needed)
#include <vector>     // Already included via header indirectly
#include <cstdarg>
#include <pthread.h>  // For pthread_setaffinity_np
#include <iomanip>    // For std::setprecision

int RdmaManager::mtu_enum_to_value(enum ibv_mtu mtu) {
    switch (mtu) {
        case IBV_MTU_256: return 256;
        case IBV_MTU_512: return 512;
        case IBV_MTU_1024: return 1024;
        case IBV_MTU_2048: return 2048;
        case IBV_MTU_4096: return 4096;
        default: return -1;
    }
}

// GID conversion helper
int RdmaManager::str_to_gid(const char *ip_str, union ibv_gid *gid) {
    struct in_addr ipv4_addr;
    if (inet_pton(AF_INET, ip_str, &ipv4_addr) != 1) {
        fprintf(stderr, "ERROR: inet_pton failed for IP '%s'\n", ip_str);
        return 1; 
    }
    memset(gid, 0, sizeof(union ibv_gid));
    gid->raw[10] = 0xff; // Marks it as an IPv4-mapped IPv6 GID
    gid->raw[11] = 0xff;
    memcpy(&gid->raw[12], &ipv4_addr.s_addr, sizeof(ipv4_addr.s_addr));
    return 0;
}

// Convert active_speed enum to Gbps per lane
static double port_speed_to_gbps(uint8_t speed) {
    switch (speed) {
        case 1:   return 2.5;   // SDR
        case 2:   return 5.0;   // DDR
        case 4:   return 10.0;  // QDR
        case 8:   return 10.0;  // FDR10
        case 16:  return 14.0;  // FDR
        case 32:  return 25.0;  // EDR
        case 64:  return 50.0;  // HDR
        case 128: return 100.0; // NDR
        case 256: return 200.0; // XDR
        default:  return 0.0;
    }
}

// Convert active_width enum to number of lanes
static int port_width_to_lanes(uint8_t width) {
    switch (width) {
        case 1:  return 1;  // 1X
        case 2:  return 4;  // 4X
        case 4:  return 8;  // 8X
        case 8:  return 12; // 12X
        default: return 1;
    }
}

void RdmaManager::log_printf(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
    if (m_log_file) {
        va_start(args, fmt);
        vfprintf(m_log_file, fmt, args);
        fflush(m_log_file);
        va_end(args);
    }
}

// Constructor
RdmaManager::RdmaManager(const std::string& dev_name, int port, uint8_t sgid_idx,
                         const RemoteQPParams& remote_params, uint32_t local_qpn_hint,
                         uint32_t initial_local_sq_psn,
                         size_t buffer_sz, int num_recv_wrs, size_t recv_slice_sz,
                         enum ibv_mtu path_mtu,
                         bool write_immediately,
                         RecvOpType recv_op,
                         bool debug_enabled)
    : m_context(nullptr), m_pd(nullptr), m_cq(nullptr), m_qp(nullptr),
      m_main_buffer_ptr(nullptr), m_main_mr(nullptr),
      m_device_name(dev_name), m_ib_port(port), m_local_sgid_index(sgid_idx),
      m_path_mtu(path_mtu),
      m_remote_qp_params(remote_params),
      m_local_qpn(local_qpn_hint), 
      m_initial_local_sq_psn(initial_local_sq_psn),
      m_buffer_size_actual(buffer_sz),
      m_num_recv_wrs_actual(num_recv_wrs),
      m_recv_slice_size_actual(recv_slice_sz),
      m_cq_size_actual(num_recv_wrs * 2),
      m_comp_channel(nullptr),
      m_shutdown_requested(false), m_qp_in_error_state(false),
      m_total_recv_msgs(0), m_total_recv_bytes(0),
      m_write_immediately(write_immediately),
      m_recv_op_type(recv_op),
      m_debug_enabled(debug_enabled),
      m_log_file(nullptr) {

    if (m_debug_enabled) {
        std::cout << "Debug mode enabled." << std::endl;
    }

    m_log_file = fopen("rdma_app.log", "a");
    if (!m_log_file) {
        perror("Failed to open log file rdma_app.log");
    }

    m_recent_received_data.resize(MAX_STORED_MSGS);
    m_recent_data_index = 0;
    m_recent_data_count = 0;

    std::cout << "RdmaManager instance created." << std::endl;
    std::cout << "  Device: " << m_device_name 
              << ", Port: " << m_ib_port 
              << ", Target SGID Index: " << (int)m_local_sgid_index << std::endl;
    std::cout << "  Remote Target: " << m_remote_qp_params.ip_str
              << ", Remote QPN: 0x" << std::hex << m_remote_qp_params.qpn << std::dec
              << ", Remote Initial PSN: " << m_remote_qp_params.initial_psn << std::endl;
    std::cout << "  Local Initial SQ PSN: " << m_initial_local_sq_psn << std::endl;
    std::cout << "  Path MTU set to: " << mtu_enum_to_value(m_path_mtu) << " bytes" << std::endl;
    std::cout << "  Receiving operation type: "
              << (m_recv_op_type == RecvOpType::WRITE ? "write" : "send")
              << std::endl;
    // Signal handling will be set up in main.cpp using a global pointer to this instance
}

// Destructor
RdmaManager::~RdmaManager() {
    // Clear global instance pointer so signal handler no longer references this object
    g_app_rdma_manager_instance_ptr.store(nullptr);

    std::cout << std::endl;
    std::cout << "RdmaManager destructor: Requesting CQ thread shutdown..." << std::endl;
    stop_cq_polling_thread(); // Ensure thread is stopped and joined before destroying resources

    std::cout << "Total messages processed: " << m_total_recv_msgs
              << ", total bytes processed: " << m_total_recv_bytes << std::endl;

    if (!m_stats_printed) {
        print_performance_stats();
    }


    // When write_immediately is enabled, data has already been streamed to disk.
    // If disabled, only a limited number of messages were kept in memory and we
    // avoid dumping them automatically to a file.

    std::cout << "Cleaning up RDMA resources..." << std::endl;
    if (m_qp && ibv_destroy_qp(m_qp)) {
        perror("~RdmaManager: ibv_destroy_qp failed");
    }
    m_qp = nullptr; 
    if (m_cq && ibv_destroy_cq(m_cq)) {
        perror("~RdmaManager: ibv_destroy_cq failed");
    }
    m_cq = nullptr;
    if (m_comp_channel && ibv_destroy_comp_channel(m_comp_channel)) {
        perror("~RdmaManager: ibv_destroy_comp_channel failed");
    }
    m_comp_channel = nullptr;
    if (m_main_mr && ibv_dereg_mr(m_main_mr)) {
        perror("~RdmaManager: ibv_dereg_mr failed");
    }
    m_main_mr = nullptr;
    if (m_main_buffer_ptr) {
        if (m_buffer_allocated_via_mmap) {
            munmap(m_main_buffer_ptr, m_buffer_size_actual);
        } else {
            free(m_main_buffer_ptr);
        }
        m_main_buffer_ptr = nullptr;
    }
    if (m_pd && ibv_dealloc_pd(m_pd)) {
        perror("~RdmaManager: ibv_dealloc_pd failed");
    }
    m_pd = nullptr;
    if (m_context && ibv_close_device(m_context)) {
        perror("~RdmaManager: ibv_close_device failed");
    }
    m_context = nullptr;

    if (m_log_file) {
        fclose(m_log_file);
        m_log_file = nullptr;
    }
    std::cout << "Cleanup in destructor complete." << std::endl;
}

// Method to set shutdown flag (called by external signal handler via global pointer)
void RdmaManager::request_shutdown_flag() {
    m_shutdown_requested.store(true);
}

bool RdmaManager::dump_all_received_data_to_file(const char* filename) const {
    if (!filename) return false;
    FILE* f = fopen(filename, "wb");
    if (!f) {
        perror("dump_all_received_data_to_file: fopen failed");
        return false;
    }
    for (size_t i = 0; i < m_recent_data_count; ++i) {
        size_t idx = (m_recent_data_index + MAX_STORED_MSGS - m_recent_data_count + i) % MAX_STORED_MSGS;
        const auto& msg = m_recent_received_data[idx];
        if (!msg.empty()) {
            size_t written = fwrite(msg.data(), 1, msg.size(), f);
            if (written != msg.size()) {
                fprintf(stderr,
                        "Warning: wrote %zu of %zu bytes to %s\n",
                        written, msg.size(), filename);
            }
        }
    }
    fclose(f);
    return true;
}

// Getter for shutdown flag
bool RdmaManager::is_shutdown_requested() const {
    return m_shutdown_requested.load();
}

// Query port attributes
bool RdmaManager::query_port_attributes() {
    struct ibv_port_attr port_attr;
    if (ibv_query_port(m_context, m_ib_port, &port_attr)) {
        perror("ibv_query_port failed");
        return false;
    }
    m_path_mtu = (port_attr.active_mtu < m_path_mtu) ? port_attr.active_mtu : m_path_mtu;
    double lane_speed = port_speed_to_gbps(port_attr.active_speed);
    int lanes = port_width_to_lanes(port_attr.active_width);
    double line_rate = lane_speed * lanes;

    std::cout << "Port " << m_ib_port << ":"
              << " State: " << ibv_port_state_str(port_attr.state)
              << ", LinkLayer: " << (port_attr.link_layer == IBV_LINK_LAYER_ETHERNET ? "Ethernet" :
                                     (port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND ? "InfiniBand" : "Unknown"))
              << ", Active MTU: " << mtu_enum_to_value(port_attr.active_mtu) << " bytes"
              << ", Using path MTU: " << mtu_enum_to_value(m_path_mtu) << " bytes"
              << ", LID: 0x" << std::hex << port_attr.lid << std::dec
              << ", GID table length: " << port_attr.gid_tbl_len;
    if (line_rate > 0.0) {
        std::cout << ", Link rate: " << static_cast<int>(line_rate + 0.5) << " Gbps";
    }
    std::cout << std::endl;

    if (m_local_sgid_index >= port_attr.gid_tbl_len) {
        std::cerr << "ERROR: local_sgid_index " << (int)m_local_sgid_index 
                  << " is out of bounds for GID table length " << port_attr.gid_tbl_len << std::endl;
        return false;
    }
    union ibv_gid sgid_queried;
    if (ibv_query_gid(m_context, m_ib_port, m_local_sgid_index, &sgid_queried)) {
        perror("ibv_query_gid for local SGID failed");
        return false;
    }
    printf("  Using SGID at index %d: %02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x\n",
           m_local_sgid_index,
           sgid_queried.raw[0], sgid_queried.raw[1], sgid_queried.raw[2], sgid_queried.raw[3],
           sgid_queried.raw[4], sgid_queried.raw[5], sgid_queried.raw[6], sgid_queried.raw[7],
           sgid_queried.raw[8], sgid_queried.raw[9], sgid_queried.raw[10], sgid_queried.raw[11],
           sgid_queried.raw[12], sgid_queried.raw[13], sgid_queried.raw[14], sgid_queried.raw[15]);
    return true;
}

// Register memory region
bool RdmaManager::register_memory_region() {
    const size_t page_size = 2 * 1024 * 1024; // Use 2MB huge pages when available

    if (m_buffer_size_actual % page_size != 0) {
        size_t adjusted = ((m_buffer_size_actual + page_size - 1) / page_size) * page_size;
        std::cerr << "WARNING: buffer size " << m_buffer_size_actual
                  << " is not a multiple of huge page size " << page_size
                  << ". Adjusting to " << adjusted << " bytes." << std::endl;
        m_buffer_size_actual = adjusted;
    }

    int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS;
#ifdef MAP_HUGETLB
    mmap_flags |= MAP_HUGETLB;
#ifdef MAP_HUGE_2MB
    mmap_flags |= MAP_HUGE_2MB;
#endif
#endif
    m_main_buffer_ptr = static_cast<char*>(mmap(nullptr, m_buffer_size_actual,
                                               PROT_READ | PROT_WRITE,
                                               mmap_flags, -1, 0));
    if (m_main_buffer_ptr == MAP_FAILED) {
        perror("mmap hugepage failed, falling back to aligned_alloc");
        m_main_buffer_ptr = static_cast<char*>(aligned_alloc(page_size, m_buffer_size_actual));
        if (!m_main_buffer_ptr) {
            std::cerr << "ERROR: aligned_alloc failed for " << m_buffer_size_actual
                      << " bytes: " << strerror(errno) << std::endl;
            return false;
        }
    } else {
        m_buffer_allocated_via_mmap = true;
    }
    memset(m_main_buffer_ptr, 0x77, m_buffer_size_actual);
    std::cout << "Main buffer (" << m_buffer_size_actual << " bytes) allocated and initialized with 0x77." << std::endl;

    m_main_mr = ibv_reg_mr(m_pd, m_main_buffer_ptr, m_buffer_size_actual,
                           IBV_ACCESS_LOCAL_WRITE |
                           IBV_ACCESS_REMOTE_WRITE |
                           IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_ATOMIC);
    if (!m_main_mr) {
        std::cerr << "ERROR: ibv_reg_mr failed: " << strerror(errno) << std::endl;
        return false; // Destructor will free m_main_buffer_ptr if it was allocated
    }
    std::cout << "Memory Region (MR) registered:" << std::endl;
    printf("  Buffer Address (m_main_buffer_ptr): %p (Decimal: %lu)\n", (void *)m_main_buffer_ptr, (unsigned long)(uintptr_t)m_main_buffer_ptr);
    printf("  MR Verbs Addr (m_main_mr->addr):  %p (Decimal: %lu)\n", (void *)m_main_mr->addr, (unsigned long)(uintptr_t)m_main_mr->addr);
    printf("  MR Length:                          %zu bytes\n", (size_t)m_main_mr->length);
    printf("  MR LKey:                            0x%x\n", m_main_mr->lkey);
    printf("  MR RKey:                            0x%x (%u)  <-- Remote will use this RKey\n",
           m_main_mr->rkey, m_main_mr->rkey);

    // Initialize receive buffer slots
    m_recv_slots.resize(m_num_recv_wrs_actual);
    m_wr_posted.assign(m_num_recv_wrs_actual, false);
    if (m_num_recv_wrs_actual * m_recv_slice_size_actual > m_buffer_size_actual) {
        std::cerr << "ERROR: Total size of receive slices (" 
                  << m_num_recv_wrs_actual * m_recv_slice_size_actual 
                  << ") exceeds main buffer size (" << m_buffer_size_actual << ")." << std::endl;
        return false;
    }
    for (int i = 0; i < m_num_recv_wrs_actual; ++i) {
        m_recv_slots[i].ptr = m_main_buffer_ptr + (i * m_recv_slice_size_actual);
        m_recv_slots[i].mr_parent = m_main_mr;
        m_recv_slots[i].wr_id = i; 
        m_recv_slots[i].slice_size = m_recv_slice_size_actual;
    }
    return true;
}

// Create Completion Queue
bool RdmaManager::create_completion_queue() {
    m_comp_channel = ibv_create_comp_channel(m_context);
    if (!m_comp_channel) {
        perror("ibv_create_comp_channel failed");
        return false;
    }
    m_cq = ibv_create_cq(m_context, m_cq_size_actual, NULL, m_comp_channel, 0);
    if (!m_cq) {
        perror("ibv_create_cq failed");
        return false;
    }
    if (ibv_req_notify_cq(m_cq, 0)) {
        perror("ibv_req_notify_cq failed");
        return false;
    }
    std::cout << "CQ created with " << m_cq_size_actual << " entries." << std::endl;
    return true;
}

// Create Queue Pair
bool RdmaManager::create_queue_pair(uint32_t qpn_hint) {
    struct ibv_qp_init_attr qp_init_attr_val;
    memset(&qp_init_attr_val, 0, sizeof(qp_init_attr_val));
    qp_init_attr_val.send_cq = m_cq;
    qp_init_attr_val.recv_cq = m_cq;
    qp_init_attr_val.qp_type = IBV_QPT_RC;
    qp_init_attr_val.sq_sig_all = 1; 
    qp_init_attr_val.cap.max_send_wr = 10; 
    qp_init_attr_val.cap.max_recv_wr = m_num_recv_wrs_actual + 2; // A bit more than posted WRs
    qp_init_attr_val.cap.max_send_sge = 1;
    qp_init_attr_val.cap.max_recv_sge = 1;
    // If qpn_hint is used, it would be via a custom field if supported by driver/HW
    // Standard ibv_create_qp does not take qpn_hint as input parameter for user QPs.
    // The qp_num is an output.
    m_qp = ibv_create_qp(m_pd, &qp_init_attr_val);
    if (!m_qp) {
        perror("ibv_create_qp failed");
        return false;
    }
    m_local_qpn = m_qp->qp_num; // Store the actual QPN assigned by the system
    printf("QP created with system-assigned QPN: 0x%x (%u)\n", m_local_qpn, m_local_qpn);
    return true;
}

// Initialize all RDMA resources
bool RdmaManager::initialize_resources() {
    struct ibv_device **dev_list_ptr;
    struct ibv_device *ib_dev_ptr = nullptr;

    dev_list_ptr = ibv_get_device_list(NULL);
    if (!dev_list_ptr) {
        perror("ibv_get_device_list failed");
        return false;
    }
    for (int k = 0; dev_list_ptr[k]; ++k) {
        if (m_device_name.empty() || strcmp(ibv_get_device_name(dev_list_ptr[k]), m_device_name.c_str()) == 0) {
            ib_dev_ptr = dev_list_ptr[k];
            break; 
        }
    }
    if (!ib_dev_ptr) {
        std::cerr << "RDMA device '" << m_device_name << "' not found." << std::endl;
        ibv_free_device_list(dev_list_ptr);
        return false;
    }
    std::cout << "Using RDMA device: " << ibv_get_device_name(ib_dev_ptr) << std::endl;
    m_context = ibv_open_device(ib_dev_ptr);
    ibv_free_device_list(dev_list_ptr); 
    if (!m_context) { perror("ibv_open_device failed"); return false; }
    if (!query_port_attributes()) return false;
    m_pd = ibv_alloc_pd(m_context);
    if (!m_pd) { perror("ibv_alloc_pd failed"); return false; }
    std::cout << "PD allocated." << std::endl;
    if (!register_memory_region()) return false;
    if (!create_completion_queue()) return false;
    if (!create_queue_pair(m_local_qpn)) return false; // m_local_qpn (hint) -> m_local_qpn (actual)
    return true;
}

// QP state transition to INIT
bool RdmaManager::transition_to_init() {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = m_ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | 
                           IBV_ACCESS_REMOTE_WRITE | 
                           IBV_ACCESS_REMOTE_READ | 
                           IBV_ACCESS_REMOTE_ATOMIC;
    if (ibv_modify_qp(m_qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
        fprintf(stderr, "Failed to modify QP to INIT (errno %d: %s)\n", errno, strerror(errno));
        return false;
    }
    std::cout << "QP state changed to INIT." << std::endl;
    return true;
}

// QP state transition to RTR
bool RdmaManager::transition_to_rtr() {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = m_path_mtu;
    attr.dest_qp_num = m_remote_qp_params.qpn;
    attr.rq_psn = m_remote_qp_params.initial_psn; 
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12; 

    attr.ah_attr.is_global = 1;
    attr.ah_attr.grh.hop_limit = 64; 
    attr.ah_attr.grh.sgid_index = m_local_sgid_index;
    if (str_to_gid(m_remote_qp_params.ip_str.c_str(), &attr.ah_attr.grh.dgid)) {
        std::cerr << "Failed to parse remote IP '" << m_remote_qp_params.ip_str << "' to GID for RTR." << std::endl;
        return false;
    }
    attr.ah_attr.dlid = 0; 
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = m_ib_port;

    int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | 
                IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    if (ibv_modify_qp(m_qp, &attr, flags)) {
        fprintf(stderr, "Failed to modify QP to RTR (errno %d: %s)\n", errno, strerror(errno));
        return false;
    }
    std::cout << "QP state changed to RTR." << std::endl;
    return true;
}

// QP state transition to RTS
bool RdmaManager::transition_to_rts() {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = m_initial_local_sq_psn; 
    attr.timeout = 14; // Typical value
    attr.retry_cnt = 7; // Max retries
    attr.rnr_retry = 7; // Max RNR retries
    attr.max_rd_atomic = 1;

    int flags = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | 
                IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
    if (ibv_modify_qp(m_qp, &attr, flags)) {
        fprintf(stderr, "Failed to modify QP to RTS (errno %d: %s)\n", errno, strerror(errno));
        return false;
    }
    std::cout << "QP state changed to RTS. Ready for RDMA operations!" << std::endl;
    return true;
}

// Setup QP through all states to RTS
bool RdmaManager::setup_qp_to_rts() {
    if (!transition_to_init()) return false;
    if (!transition_to_rtr()) return false;
    if (!transition_to_rts()) return false;
    m_qp_in_error_state.store(false); // QP is now good
    return true;
}

// Try to reset QP from error state back to RTS
bool RdmaManager::try_reset_and_reinit_qp() {
    std::cout << "Attempting to reset QP 0x" << std::hex << m_local_qpn << std::dec 
              << " and re-initialize to RTS..." << std::endl;
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;
    if (ibv_modify_qp(m_qp, &attr, IBV_QP_STATE)) {
        fprintf(stderr, "Failed to modify QP to RESET during recovery (errno %d: %s)\n", errno, strerror(errno));
        return false;
    }
    std::cout << "QP state changed to RESET." << std::endl;
    
    // PSNs for re-connection typically start from initial values agreed upon.
    // m_initial_local_sq_psn (for RTS) and m_remote_qp_params.initial_psn (for RTR)
    // are already set from constructor/arguments.

    if (!setup_qp_to_rts()) { // This will call init, rtr, rts again
        std::cerr << "Failed to re-initialize QP to RTS after reset." << std::endl;
        return false;
    }
    std::cout << "QP successfully reset and re-initialized to RTS." << std::endl;
    m_qp_in_error_state.store(false);
    return true;
}

// Post a single receive work request
bool RdmaManager::post_single_recv(uint64_t wr_id_idx) {
    if (!m_qp || m_qp_in_error_state.load()) { // Don't post if QP is bad
        std::cerr << "Skipping post_single_recv: QP is null or in error state." << std::endl;
        return false;
    }
    if (wr_id_idx >= m_recv_slots.size()) {
        std::cerr << "Error: WR ID index " << wr_id_idx << " out of bounds for m_recv_slots." << std::endl;
        return false;
    }
    RecvBufferSlot& slot = m_recv_slots[wr_id_idx];

    struct ibv_recv_wr recv_wr;
    struct ibv_sge sge;
    struct ibv_recv_wr *bad_wr = nullptr;

    memset(&recv_wr, 0, sizeof(recv_wr));
    recv_wr.wr_id = slot.wr_id;

    if (m_recv_op_type == RecvOpType::SEND) {
        sge.addr = (uintptr_t)slot.ptr;
        sge.length = slot.slice_size;
        sge.lkey = slot.mr_parent->lkey;
        recv_wr.sg_list = &sge;
        recv_wr.num_sge = 1;
    } else {
        // For RDMA Write with immediate we only need the WR to receive the
        // immediate, no buffer is consumed.
        recv_wr.sg_list = nullptr;
        recv_wr.num_sge = 0;
    }

    if (ibv_post_recv(m_qp, &recv_wr, &bad_wr)) {
        fprintf(stderr, "ibv_post_recv failed for wr_id %lu (errno %d: %s)\n", slot.wr_id, errno, strerror(errno));
        m_qp_in_error_state.store(true); // Posting failed, QP might be in error
        return false;
    }
    if (wr_id_idx < m_wr_posted.size()) {
        m_wr_posted[wr_id_idx] = true;
    }
    return true;
}

// Post receive WRs for all available slots
bool RdmaManager::post_all_initial_recv_wrs() {
    if (m_recv_slots.empty()) {
        std::cerr << "No receive slots configured to post." << std::endl;
        return false; // Or true if this is not an error condition
    }
    for (size_t i = 0; i < m_recv_slots.size(); ++i) {
        if (!post_single_recv(i)) { 
            std::cerr << "Failed to post initial recv WR for slot " << i << std::endl;
            return false; // Stop if any post fails
        }
    }
    std::cout << m_recv_slots.size() << " initial receive WRs posted." << std::endl;
    return true;
}

// Process a single Work Completion
void RdmaManager::process_work_completion(struct ibv_wc* wc, FILE* outfile) {

    if (wc->status == IBV_WC_SUCCESS) {
        if (wc->opcode == IBV_WC_RECV || wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
            if (!m_first_ts_recorded) {
                m_first_recv_ts = std::chrono::steady_clock::now();
                m_first_ts_recorded = true;
            }
            m_last_recv_ts = std::chrono::steady_clock::now();
            m_stats_printed = false; // new data arrived
            if (wc->wr_id < m_recv_slots.size()) {
                RecvBufferSlot& slot = m_recv_slots[wc->wr_id];
                size_t xfer_len = wc->byte_len;
                if (m_recv_op_type == RecvOpType::WRITE &&
                    wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM && xfer_len == 0) {
                    xfer_len = m_recv_slice_size_actual; // assume full slice written by remote
                }
                if (wc->wr_id < m_wr_posted.size()) {
                    m_wr_posted[wc->wr_id] = false;
                }
                uint32_t imm = 0;
                if (wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                    imm = ntohl(wc->imm_data);
                }

                if (outfile && xfer_len > 0) {
                    size_t written = fwrite(slot.ptr, 1, xfer_len, outfile);
                    if (written != xfer_len) {
                        fprintf(stderr, "  ERROR: writing %zu received bytes to file (wrote %zu).\n", xfer_len, written);
                    }
                    fflush(outfile); // Ensure data is written immediately
                }

                if (xfer_len > 0 && !m_write_immediately) {
                    // std::vector<char>& buf = m_recent_received_data[m_recent_data_index];
                    // buf.assign(slot.ptr, slot.ptr + xfer_len);
                    if (m_recent_data_count < MAX_STORED_MSGS) {
                        m_recent_data_count++;
                    }
                    size_t idx_print = m_recent_data_index;
                    // m_recent_data_index = (m_recent_data_index + 1) % MAX_STORED_MSGS;
                }

                if (xfer_len > 0) {
                    m_total_recv_msgs++;
                    m_total_recv_bytes += xfer_len;
                    // printf("\rMessages received: %zu", m_total_recv_msgs);
                    // fflush(stdout);
                }

                // Re-post this receive buffer for future receives
                if (!post_single_recv(wc->wr_id)) {
                    fprintf(stderr, "  CRITICAL: Failed to re-post recv WR for WR_ID %lu. Shutting down.\n", wc->wr_id);
                    request_shutdown_flag();
                }

            } else { // Should not happen if wr_ids are managed correctly
                 fprintf(stderr, "  ERROR: Received WC with out-of-bounds WR_ID %lu (max is %zu)\n", wc->wr_id, m_recv_slots.size() -1);
            }
        } // Add handling for other successful opcodes if this app also sends (e.g. IBV_WC_SEND)
    } else { // Work completion status is not SUCCESS
        fprintf(stderr, "  Work completion failed with status: %s (%d) for WR_ID %lu\n", 
                ibv_wc_status_str(wc->status), wc->status, wc->wr_id);
        if (wc->status == IBV_WC_WR_FLUSH_ERR) {
            log_printf("  QP 0x%x likely entered error state (WR_FLUSH_ERR). Flagging for reset.\n", m_local_qpn);
        } else {
             log_printf("  Unhandled WC error for QP 0x%x. Flagging for reset.\n", m_local_qpn);
        }
        m_qp_in_error_state.store(true); // Signal main loop to attempt reset
    }
    if (m_debug_enabled) {
        for (size_t i = 0; i < m_recv_slots.size(); ++i) {
            const char* st = m_wr_posted[i] ? "POSTED" : "COMPLETED";
            log_printf("  [DEBUG] WR %zu buffer %p status %s\n", i, (void*)m_recv_slots[i].ptr, st);
        }
    }
}

// The actual CQ polling loop function, run in a separate thread
void RdmaManager::cq_poll_loop_func() {
    std::cout << "[CQ Thread] Started in busy polling mode on CPU 0. Local QP 0x"
              << std::hex << m_local_qpn << std::dec << std::endl;

#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset); // Pin thread to CPU 0 for dedicated polling
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
#endif

    FILE *output_file = nullptr;
    if (m_write_immediately) {
        output_file = fopen(DEFAULT_OUTPUT_FILENAME_H, "wb");
        if (!output_file) {
            perror("[CQ Thread] Failed to open output file");
            std::cerr << "[CQ Thread] Warning: Received data will not be saved to file." << std::endl;
        }
    }

    std::vector<struct ibv_wc> wc_array(m_cq_size_actual);
    while (!m_shutdown_requested.load()) {
        if (m_qp_in_error_state.load()) {
            std::cout << "[CQ Thread] QP 0x" << std::hex << m_local_qpn << std::dec
                      << " is in error state. Attempting reset..." << std::endl;
            if (try_reset_and_reinit_qp()) {
                if (!post_all_initial_recv_wrs()) {
                    std::cerr << "[CQ Thread] CRITICAL: Failed to re-post all receive WRs after QP reset. Requesting shutdown." << std::endl;
                    request_shutdown_flag();
                } else {
                    std::cout << "[CQ Thread] QP reset successful and receive WRs re-posted." << std::endl;
                }
            } else {
                std::cerr << "[CQ Thread] CRITICAL: Failed to reset QP. Requesting shutdown." << std::endl;
                request_shutdown_flag();
            }
        }

        if (m_shutdown_requested.load())
            break;

        int num_wcs = ibv_poll_cq(m_cq, m_cq_size_actual, wc_array.data());
        if (num_wcs > 0) {
            for (int k = 0; k < num_wcs; ++k) {
                process_work_completion(&wc_array[k], output_file);
                if (m_shutdown_requested.load())
                    break;
            }
        } else if (num_wcs < 0) {
            perror("[CQ Thread] ibv_poll_cq failed");
            request_shutdown_flag();
            break;
        }

        auto now = std::chrono::steady_clock::now();
        if (m_first_ts_recorded && !m_stats_printed &&
            now - m_last_recv_ts >= m_idle_timeout) {
            print_performance_stats();
            m_stats_printed = true;
        }
    }

    std::cout << "[CQ Thread] Finishing." << std::endl;
    if (output_file) {
        fclose(output_file);
        std::cout << "[CQ Thread] Output file '" << DEFAULT_OUTPUT_FILENAME_H << "' closed." << std::endl;
    }
}

// Method to start the CQ polling thread
void RdmaManager::start_cq_polling_thread() {
    if (m_cq_thread.joinable()) {
        // This check might not be perfectly thread-safe if start is called multiple times
        // but for this example, assume it's called once.
        std::cout << "CQ polling thread may already be running or not properly joined from a previous run." << std::endl;
        return;
    }
    m_shutdown_requested.store(false); 
    m_qp_in_error_state.store(false);
    m_cq_thread = std::thread(&RdmaManager::cq_poll_loop_func, this);
    std::cout << "CQ polling thread launched." << std::endl;
}

// Method to stop and join the CQ polling thread
void RdmaManager::stop_cq_polling_thread() {
    std::cout << "Attempting to stop CQ polling thread..." << std::endl;
    m_shutdown_requested.store(true); // Signal the thread to stop its loop
    if (m_cq_thread.joinable()) {
        std::cout << "Waiting for CQ polling thread to join..." << std::endl;
        try {
            m_cq_thread.join(); // Wait for the thread to finish
            std::cout << "CQ polling thread joined successfully." << std::endl;
        } catch (const std::system_error& e) {
            std::cerr << "System error while joining CQ thread: " << e.what() 
                      << " (Code: " << e.code() << ")" << std::endl;
            // This can happen if thread was already joined or not joinable for some reason
        }
    } else {
        std::cout << "CQ polling thread was not joinable (e.g., not started or already finished)." << std::endl;
    }
}

// Print basic throughput statistics based on recorded timestamps
void RdmaManager::print_performance_stats() const {
    if (!m_first_ts_recorded || m_total_recv_bytes == 0) {
        std::cout << "No receive timing information recorded." << std::endl;
        return;
    }

    auto duration = std::chrono::duration_cast<std::chrono::duration<double>>(m_last_recv_ts - m_first_recv_ts);
    double seconds = duration.count();
    if (seconds <= 0.0) {
        std::cout << "Duration too small to compute throughput." << std::endl;
        return;
    }
    double mb = static_cast<double>(m_total_recv_bytes) / (1024.0 * 1024.0);
    double mbps = mb / seconds;
    std::cout << "Data received: " << mb << " MB in " << seconds
              << " s (" << mbps << " MB/s)." << std::endl;
}

bool RdmaManager::write_params_to_json(const char* filename, size_t msg_size) const {
    if (!filename || !m_main_mr) {
        return false;
    }
    FILE* f = fopen(filename, "w");
    if (!f) {
        perror("write_params_to_json: fopen failed");
        return false;
    }
    fprintf(f,
            "{\n"
            "  \"local_qpn\": %u,\n"
            "  \"mr_rkey\": %u,\n"
            "  \"mr_addr\": %zu,\n"
            "  \"buffer_size\": %zu,\n"
            "  \"msg_size\": %zu\n"
            "}\n",
            m_local_qpn,
            m_main_mr->rkey,
            (unsigned long)m_main_mr->addr,
            m_buffer_size_actual,
            msg_size);
    fclose(f);
    std::cout << "Connection parameters written to '" << filename << "'." << std::endl;
    return true;
}

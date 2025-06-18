#include "rdma_manager.h" // Should include <atomic>, <csignal> indirectly if RdmaManager uses them
#include <iostream>
#include <string>
#include <vector>   // Not strictly needed in main but often useful
#include <getopt.h> // For getopt_long
#include <cstdlib>  // For std::stoi, std::stoul, exit, EXIT_FAILURE, EXIT_SUCCESS
#include <csignal>  // For sigaction, SIGINT, SIGTERM struct
#include <unistd.h> // For write (in signal handler for safety)
#include <cstring>  // For strlen (in signal handler)

// Global pointer to RdmaManager instance for signal handler to access
std::atomic<RdmaManager*> g_app_rdma_manager_instance_ptr{nullptr};

// Signal handler function
static void app_signal_handler(int sig_num) {
    // This function must be reentrant and async-signal-safe.
    // The main action is to set a flag that the application's main/worker threads can check.
    RdmaManager* mgr = g_app_rdma_manager_instance_ptr.load();
    if (mgr) {
        mgr->request_shutdown_flag();
        
        // For debugging, write a simple message to stderr (write is async-signal-safe)
        char msg_buffer[128];
        snprintf(msg_buffer, sizeof(msg_buffer), "Signal %d received, shutdown requested by handler for RdmaManager.\n", sig_num);
        write(STDERR_FILENO, msg_buffer, strlen(msg_buffer));
    } else {
        const char* msg_err = "Signal received, but RdmaManager instance is null in handler.\n";
        write(STDERR_FILENO, msg_err, strlen(msg_err));
        // If RdmaManager is not set, perhaps we should exit directly, though this is abrupt.
        // For this example, we just note it. The application might hang if the main loop isn't also checking.
    }
}

void print_usage(const char* prog_name) {
    std::cerr << "Usage: " << prog_name << " [options]\n\n"
              << "Manages an RDMA connection, sets up a QP to RTS, posts receive buffers,\n"
              << "and polls for completions in a separate thread. Data received via RDMA Send\n"
              << "from the remote peer is appended to '" << DEFAULT_OUTPUT_FILENAME_H << "'.\n"
              << "The QP will attempt to reset and re-initialize if it enters an error state.\n\n"
              << "Options:\n"
              << "  --device     <name>    RDMA device name (default: rocep94s0f1)\n"
              << "  --port       <num>     RDMA port number (default: 1)\n"
              << "  --sgid_idx   <idx>     Local SGID index for the RoCE v2 GID. "
              <<                          "CRITICAL: Must be verified for your local IP on the device/port.\n"
              << "                           (default: 3 - User MUST verify this!)\n"
              << "  --remote_ip  <ip>      Remote/FPGA IP address (default: 192.168.160.32)\n"
              << "  --remote_qpn <qpn>     Remote/FPGA QPN (hex [0x] or decimal, default: 0x100 / 256)\n"
              << "  --remote_psn <psn>     Remote/FPGA initial PSN (for PC's RTR setup, default: 0)\n"
              << "  --local_psn  <psn>     PC's initial SQ PSN (default: 0)\n"
              << "  --buffer_size <bytes>  Size of receive buffer (default: " << DEFAULT_BUFFER_SIZE_H << ")\n"
              << "  --num_wrs     <num>    Number of receive WRs (default: " << DEFAULT_NUM_RECV_WRS_H << ")\n"
              << "  --msg_size    <bytes>  Size of each message/slice (default: " << DEFAULT_RECV_BUFFER_SLICE_SIZE_H << ")\n"
              << "  --mtu         <256|512|1024|2048|4096> Path MTU (default: 4096)\n"
              << "  -h, --help             Show this help message and exit\n"
              << "\nExample: " << prog_name << " --sgid_idx 4 --remote_qpn 0x100\n"
              << std::endl;
}

int main(int argc, char* argv[]) {
    std::ios_base::sync_with_stdio(false); // Potentially speeds up C++ iostreams for non-mixed use
    std::cout << "RDMA Application (C++ Threaded & Parameterized) starting..." << std::endl;

    // Default parameters
    std::string param_device_name = "rocep94s0f1";
    int param_ib_port = 1;
    uint8_t param_sgid_index = 3; // CRITICAL DEFAULT - USER MUST VERIFY!
    
    RemoteQPParams param_remote_qp_info;
    param_remote_qp_info.ip_str = "192.168.160.32";
    param_remote_qp_info.qpn = 0x100; 
    param_remote_qp_info.initial_psn = 0;

    uint32_t param_pc_initial_sq_psn = 0;
    size_t param_buffer_size = DEFAULT_BUFFER_SIZE_H;
    int param_num_recv_wrs = DEFAULT_NUM_RECV_WRS_H;
    size_t param_recv_slice_size = DEFAULT_RECV_BUFFER_SLICE_SIZE_H;
    enum ibv_mtu param_mtu = IBV_MTU_4096;

    // Command line argument parsing
    int opt_char;
    int option_index = 0;
    static struct option long_options[] = {
        {"device",     required_argument, 0, 'd'},
        {"port",       required_argument, 0, 'p'},
        {"sgid_idx",   required_argument, 0, 'g'},
        {"remote_ip",  required_argument, 0, 'r'},
        {"remote_qpn", required_argument, 0, 'q'},
        {"remote_psn", required_argument, 0, 'n'},
        {"local_psn",  required_argument, 0, 's'},
        {"buffer_size", required_argument, 0, 'b'},
        {"num_wrs",    required_argument, 0, 'w'},
        {"msg_size",   required_argument, 0, 'm'},
        {"mtu",        required_argument, 0, 'u'},
        {"help",       no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    while ((opt_char = getopt_long(argc, argv, "h", long_options, &option_index)) != -1) {
        switch (opt_char) {
            case 'd': param_device_name = optarg; break;
            case 'p': 
                try { param_ib_port = std::stoi(optarg); } 
                catch (const std::exception& e) { std::cerr << "Invalid port number '" << optarg << "': " << e.what() << std::endl; return EXIT_FAILURE;} 
                break;
            case 'g': 
                try { 
                    long temp_sgid = std::stol(optarg); // Use stol for potentially larger base range
                    if(temp_sgid < 0 || temp_sgid > 255) throw std::out_of_range("SGID index out of range (0-255)");
                    param_sgid_index = static_cast<uint8_t>(temp_sgid); 
                } catch (const std::exception& e) { std::cerr << "Invalid sgid_idx '" << optarg << "': " << e.what() << std::endl; return EXIT_FAILURE;} 
                break;
            case 'r': param_remote_qp_info.ip_str = optarg; break;
            case 'q': 
                try { param_remote_qp_info.qpn = std::stoul(optarg, nullptr, 0); } 
                catch (const std::exception& e) { std::cerr << "Invalid remote_qpn '" << optarg << "': " << e.what() << std::endl; return EXIT_FAILURE;} 
                break;
            case 'n': 
                try { param_remote_qp_info.initial_psn = std::stoul(optarg, nullptr, 0); } 
                catch (const std::exception& e) { std::cerr << "Invalid remote_psn '" << optarg << "': " << e.what() << std::endl; return EXIT_FAILURE;} 
                break;
            case 's':
                try { param_pc_initial_sq_psn = std::stoul(optarg, nullptr, 0); }
                catch (const std::exception& e) { std::cerr << "Invalid local_psn '" << optarg << "': " << e.what() << std::endl; return EXIT_FAILURE; }
                break;
            case 'b':
                try { param_buffer_size = std::stoul(optarg, nullptr, 0); }
                catch (const std::exception& e) { std::cerr << "Invalid buffer_size '" << optarg << "': " << e.what() << std::endl; return EXIT_FAILURE; }
                break;
            case 'w':
                try { param_num_recv_wrs = std::stoi(optarg); }
                catch (const std::exception& e) { std::cerr << "Invalid num_wrs '" << optarg << "': " << e.what() << std::endl; return EXIT_FAILURE; }
                break;
            case 'm':
                try { param_recv_slice_size = std::stoul(optarg, nullptr, 0); }
                catch (const std::exception& e) { std::cerr << "Invalid msg_size '" << optarg << "': " << e.what() << std::endl; return EXIT_FAILURE; }
                break;
            case 'u':
                try {
                    int mtu_val = std::stoi(optarg);
                    switch (mtu_val) {
                        case 256: param_mtu = IBV_MTU_256; break;
                        case 512: param_mtu = IBV_MTU_512; break;
                        case 1024: param_mtu = IBV_MTU_1024; break;
                        case 2048: param_mtu = IBV_MTU_2048; break;
                        case 4096: param_mtu = IBV_MTU_4096; break;
                        default: throw std::out_of_range("invalid mtu");
                    }
                } catch (const std::exception& e) { std::cerr << "Invalid mtu '" << optarg << "': " << e.what() << std::endl; return EXIT_FAILURE; }
                break;
            case 'h': print_usage(argv[0]); return EXIT_SUCCESS;
            case '?': // getopt_long already printed an error message
            default:  print_usage(argv[0]); return EXIT_FAILURE;
        }
    }

    if (param_buffer_size < param_recv_slice_size * static_cast<size_t>(param_num_recv_wrs)) {
        param_buffer_size = param_recv_slice_size * static_cast<size_t>(param_num_recv_wrs);
        std::cout << "Adjusted buffer_size to " << param_buffer_size
                  << " bytes to hold " << param_num_recv_wrs
                  << " messages of " << param_recv_slice_size << " bytes." << std::endl;
    }

    std::cout << "--- Effective Configuration ---" << std::endl;
    std::cout << "Device: " << param_device_name << ", Port: " << param_ib_port << std::endl;
    std::cout << "Local SGID Index: " << (int)param_sgid_index << " (CRITICAL: User must verify this!)" << std::endl;
    std::cout << "Remote IP: " << param_remote_qp_info.ip_str
              << ", Remote QPN: 0x" << std::hex << param_remote_qp_info.qpn << std::dec << " (" << param_remote_qp_info.qpn << ")"
              << ", Remote Initial PSN: " << param_remote_qp_info.initial_psn << std::endl;
    std::cout << "Local Initial SQ PSN: " << param_pc_initial_sq_psn << std::endl;
    std::cout << "Buffer Size: " << param_buffer_size << " bytes, Num WRs: " << param_num_recv_wrs
              << ", Message Size: " << param_recv_slice_size << " bytes" << std::endl;
    std::cout << "Path MTU: " << RdmaManager::mtu_enum_to_value(param_mtu) << " bytes" << std::endl;
    std::cout << "-----------------------------" << std::endl;
    
    if (param_ib_port <= 0) { std::cerr << "Error: Port number must be positive." << std::endl; return EXIT_FAILURE; }

    try {
        RdmaManager rdma_manager(param_device_name, param_ib_port, param_sgid_index,
                                 param_remote_qp_info, 0 /* local_qpn_hint */,
                                 param_pc_initial_sq_psn, param_buffer_size,
                                 param_num_recv_wrs, param_recv_slice_size,
                                 param_mtu);
        
        g_app_rdma_manager_instance_ptr.store(&rdma_manager);

        struct sigaction sa_main_custom_handler;
        memset(&sa_main_custom_handler, 0, sizeof(sa_main_custom_handler));
        sa_main_custom_handler.sa_handler = app_signal_handler;
        sigemptyset(&sa_main_custom_handler.sa_mask);
        sa_main_custom_handler.sa_flags = SA_RESTART; // Important for syscalls like getchar()

        if (sigaction(SIGINT, &sa_main_custom_handler, NULL) == -1) {
            perror("main: sigaction(SIGINT) failed, proceeding without custom handler for SIGINT.");
        }
        if (sigaction(SIGTERM, &sa_main_custom_handler, NULL) == -1) {
            perror("main: sigaction(SIGTERM) failed, proceeding without custom handler for SIGTERM.");
        }

        if (!rdma_manager.initialize_resources()) {
            std::cerr << "Main: Failed to initialize RDMA resources." << std::endl;
            return EXIT_FAILURE;
        }

        if (!rdma_manager.setup_qp_to_rts()) {
            std::cerr << "Main: Failed to setup QP to RTS." << std::endl;
            return EXIT_FAILURE; 
        }

        if (!rdma_manager.post_all_initial_recv_wrs()) {
            std::cerr << "Main: Failed to post initial receive WRs." << std::endl;
            return EXIT_FAILURE;
        }

        rdma_manager.start_cq_polling_thread(); 

        std::cout << "Main thread: CQ polling thread started. Main thread will wait." << std::endl;
        std::cout << "Press Enter or send Ctrl+C/kill signal to request shutdown and exit..." << std::endl;
        
        // Wait for Enter key or for shutdown to be requested by signal
        while(true) {
            // Check if shutdown was requested by a signal before blocking on getchar
            if (rdma_manager.is_shutdown_requested()) { 
                 std::cout << "Main: Shutdown already requested by signal, exiting wait loop." << std::endl;
                 break;
            }

            // Try to read a character. getchar can be interrupted by signals.
            // A more robust way for main thread to wait might involve condition variables
            // or select/poll on stdin with a timeout if other main thread work is needed.
            // For this example, simple getchar loop is used.
            int ch = getchar(); 
            
            if (rdma_manager.is_shutdown_requested()) { 
                 std::cout << "Main: Shutdown requested by signal (checked after getchar), exiting wait loop." << std::endl;
                 break;
            }
            if (ch == '\n' || ch == EOF) { 
                std::cout << "Main: Enter pressed or EOF, requesting shutdown..." << std::endl;
                rdma_manager.request_shutdown_flag(); 
                break; 
            }
            // If getchar returned something else (e.g., due to EINTR and no actual char), loop again.
        }
        
        std::cout << "Main thread: Initiating stop of CQ polling thread..." << std::endl;
        // stop_cq_polling_thread() will be called by RdmaManager's destructor when rdma_manager goes out of scope.
        // Calling it explicitly here ensures the thread is joined before main attempts to exit further.
        rdma_manager.stop_cq_polling_thread(); 

    } catch (const std::exception& e) {
        std::cerr << "Main: An unhandled exception occurred: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "RDMA Application finished." << std::endl;
    return EXIT_SUCCESS;
}

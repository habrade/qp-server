cmake_minimum_required(VERSION 3.10)
project(RdmaCppApp CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -g") # Enable warnings and debug symbols

find_package(PkgConfig REQUIRED)
pkg_check_modules(IBVERBS REQUIRED libibverbs)

# For std::thread, ensure pthread is linked
find_package(Threads REQUIRED)

include_directories(${IBVERBS_INCLUDE_DIRS})

add_executable(rdma_app
    src/main.cpp
    src/rdma_manager.cpp
    # rdma_manager.h # Header files are typically not listed here for add_executable
)

# Add include directory for headers if they are in a subdirectory or for clarity
target_include_directories(rdma_app PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(rdma_app
    PRIVATE
    ${IBVERBS_LIBRARIES}
    Threads::Threads # Link pthread
)
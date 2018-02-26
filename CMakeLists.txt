project(goplusplus C CXX ASM)

add_definitions(-std=c++17 -fno-omit-frame-pointer)

set(CMAKE_C_FLAGS_DEBUG "-g -fsanitize=address")
set(CMAKE_CXX_FLAGS_DEBUG "-g -fsanitize=address")

add_library(gopp channels.cc gopp.cc asm.S)

# test binary
add_executable(buffers-sample buffers-sample.cc)
target_link_libraries(buffers-sample gopp pthread)
# 
# add_executable(server-sample server-sample.cc)
# target_link_libraries(server-sample gopp pthread)
 
add_executable(http-client-sample http-client-sample.cc)
target_link_libraries(http-client-sample gopp pthread)

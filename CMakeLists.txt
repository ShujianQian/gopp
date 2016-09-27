project(goplusplus C CXX ASM)

add_definitions(-std=c++11)

add_library(gopp gopp.cc epoll-channel.cc asm.S)

# test binary
add_executable(server-sample server-sample.cc)
target_link_libraries(server-sample gopp pthread)

add_executable(http-client-sample http-client-sample.cc)
target_link_libraries(http-client-sample gopp pthread)

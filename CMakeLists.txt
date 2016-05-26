project(goplusplus)

add_definitions(-std=c++11)

add_library(gopp gopp.cc epoll-channel.cc)

# test binary
add_executable(sample sample.cc)
target_link_libraries(sample gopp pthread)

project(goplusplus)

add_definitions(-std=c++11)

# test binary
add_executable(sample sample.cc gopp.cc)
target_link_libraries(sample pthread)

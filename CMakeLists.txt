cmake_minimum_required(VERSION 3.16)
project(cpp_multithreaded_web_server LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_executable(web_server
    src/main.cpp
)

target_include_directories(web_server PRIVATE src)
target_link_libraries(web_server PRIVATE Threads::Threads)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(web_server PRIVATE -Wall -Wextra -Wpedantic)
endif()

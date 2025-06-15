cmake_minimum_required(VERSION 3.12.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(cascade_oob_test CXX)

set(CMAKE_CXX_STANDARD 17)
find_package(cascade CONFIG REQUIRED)

include(CheckLanguage)
check_language(CUDA)

if (CMAKE_CUDA_COMPILER)
    enable_language(CUDA)
    find_package(CUDAToolkit)
    set (CUDA_FOUND 1)

    if (NOT DEFINED CMAKE_CUDA_STANDARD)
        set(CMAKE_CUDA_STANDARD 17)
        set(CMAKE_CUDA_STANDARD_REQUIRED ON)
    endif()

    if (NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
        execute_process(COMMAND bash -c "nvidia-smi --query-gpu=compute_cap --format=csv,noheader,nounits | sed 's/\\.//' | head -1"
                        OUTPUT_VARIABLE NATIVE_CUDA_ARCH)
        string(STRIP ${NATIVE_CUDA_ARCH} NATIVE_CUDA_ARCH)
        set(CMAKE_CUDA_ARCHITECTURES ${NATIVE_CUDA_ARCH})
        message("NATIVE_CUDA_ARCH=${NATIVE_CUDA_ARCH}")
    endif()
endif()


add_library(oob_udl SHARED src/oob_udl.cpp)
target_include_directories(oob_udl PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  )

if (CUDA_FOUND)
target_link_libraries(oob_udl CUDA::cuda_driver)
endif()

add_custom_command(TARGET oob_udl POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n3/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n3/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n3/udl_dlls.cfg
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/cfg/run.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cfg/layout.json
            ${CMAKE_CURRENT_SOURCE_DIR}/cfg/dfgs.json
            ${CMAKE_CURRENT_SOURCE_DIR}/cfg/udl_dlls.cfg
    COMMENT "prepare cascade oob demo configuration"
   )

target_link_libraries(oob_udl derecho::cascade)

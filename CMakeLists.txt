cmake_minimum_required(VERSION 3.6)
project(143_2)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

#[[
set(SOURCE_FILES
    Bruinbase.h
    BTreeIndex.cc
    BTreeIndex.h
    BTreeNode.cc
    BTreeNode.h
    lex.sql.c
    main.cc
    PageFile.cc
    PageFile.h
    RecordFile.cc
    RecordFile.h
    SqlEngine.cc
    SqlEngine.h
    SqlParser.tab.c
    SqlParser.tab.h)
    ]]

add_custom_target(143_2 COMMAND make -C ${143_2_SOURCE_DIR}
        CLION_EXE_DIR=${PROJECT_BINARY_DIR})


# add_executable(143_2 ${SOURCE_FILES})

# project(SMQP - secure multi-party query processing system)

include_directories(${CMAKE_SOURCE_DIR}/SMQP/)

#############################################
##                demo file                ##
#############################################

add_executable(SMQP
    test.cpp
    join.cpp
    Permutation.cpp
    PrefixSumCircuit.cpp
    SingleRelationOperator.cpp
    sort.cpp
    baselinejoin.cpp
)

target_link_libraries(SMQP com-psi)
target_link_libraries(SMQP aby3-ML)
target_link_libraries(SMQP com-psi_Tests)
target_link_libraries(SMQP aby3_Tests)
target_link_libraries(SMQP oc::tests_cryptoTools)

#############################################
##               tpchq3 file               ##
#############################################

add_executable(TPCHQ3
    tpchq3.cpp
    join.cpp
    Permutation.cpp
    PrefixSumCircuit.cpp
    SingleRelationOperator.cpp
    sort.cpp
    baselinejoin.cpp
)

target_link_libraries(TPCHQ3 com-psi)
target_link_libraries(TPCHQ3 aby3-ML)
target_link_libraries(TPCHQ3 com-psi_Tests)
target_link_libraries(TPCHQ3 aby3_Tests)
target_link_libraries(TPCHQ3 oc::tests_cryptoTools)

#############################################
##              bitcoinalpha               ##
#############################################

add_executable(bitcoinalpha
    bitcoinalpha.cpp
    join.cpp
    Permutation.cpp
    PrefixSumCircuit.cpp
    SingleRelationOperator.cpp
    sort.cpp
    baselinejoin.cpp
)

target_link_libraries(bitcoinalpha com-psi)
target_link_libraries(bitcoinalpha aby3-ML)
target_link_libraries(bitcoinalpha com-psi_Tests)
target_link_libraries(bitcoinalpha aby3_Tests)
target_link_libraries(bitcoinalpha oc::tests_cryptoTools)

#############################################
##               tpchq1 file               ##
#############################################

add_executable(TPCHQ1
    tpchq1.cpp
    join.cpp
    Permutation.cpp
    PrefixSumCircuit.cpp
    SingleRelationOperator.cpp
    sort.cpp
    baselinejoin.cpp
)

target_link_libraries(TPCHQ1 com-psi)
target_link_libraries(TPCHQ1 aby3-ML)
target_link_libraries(TPCHQ1 com-psi_Tests)
target_link_libraries(TPCHQ1 aby3_Tests)
target_link_libraries(TPCHQ1 oc::tests_cryptoTools)

#############################################
##               tpchq4 file               ##
#############################################

add_executable(TPCHQ4
    tpchq4.cpp
    join.cpp
    Permutation.cpp
    PrefixSumCircuit.cpp
    SingleRelationOperator.cpp
    sort.cpp
    baselinejoin.cpp
)

target_link_libraries(TPCHQ4 com-psi)
target_link_libraries(TPCHQ4 aby3-ML)
target_link_libraries(TPCHQ4 com-psi_Tests)
target_link_libraries(TPCHQ4 aby3_Tests)
target_link_libraries(TPCHQ4 oc::tests_cryptoTools)

#############################################
##               tpchq11 file               ##
#############################################

add_executable(TPCHQ11
    tpchq11.cpp
    join.cpp
    Permutation.cpp
    PrefixSumCircuit.cpp
    SingleRelationOperator.cpp
    sort.cpp
    baselinejoin.cpp
)

target_link_libraries(TPCHQ11 com-psi)
target_link_libraries(TPCHQ11 aby3-ML)
target_link_libraries(TPCHQ11 com-psi_Tests)
target_link_libraries(TPCHQ11 aby3_Tests)
target_link_libraries(TPCHQ11 oc::tests_cryptoTools)


#############################################
##                tpch full                ##
#############################################

add_executable(TPCH
    tpch.cpp
    join.cpp
    Permutation.cpp
    PrefixSumCircuit.cpp
    SingleRelationOperator.cpp
    sort.cpp
    baselinejoin.cpp
)

target_link_libraries(TPCH com-psi)
target_link_libraries(TPCH aby3-ML)
target_link_libraries(TPCH com-psi_Tests)
target_link_libraries(TPCH aby3_Tests)
target_link_libraries(TPCH oc::tests_cryptoTools)

#############################################
##              tpchq10 file               ##
#############################################

add_executable(TPCHQ10
    tpchq10.cpp
    join.cpp
    Permutation.cpp
    PrefixSumCircuit.cpp
    SingleRelationOperator.cpp
    sort.cpp
    baselinejoin.cpp
)

target_link_libraries(TPCHQ10 com-psi)
target_link_libraries(TPCHQ10 aby3-ML)
target_link_libraries(TPCHQ10 com-psi_Tests)
target_link_libraries(TPCHQ10 aby3_Tests)
target_link_libraries(TPCHQ10 oc::tests_cryptoTools)

#############################################
##              tpchq18 file               ##
#############################################

add_executable(TPCHQ18
    tpchq18.cpp
    join.cpp
    Permutation.cpp
    PrefixSumCircuit.cpp
    SingleRelationOperator.cpp
    sort.cpp
    baselinejoin.cpp
)

target_link_libraries(TPCHQ18 com-psi)
target_link_libraries(TPCHQ18 aby3-ML)
target_link_libraries(TPCHQ18 com-psi_Tests)
target_link_libraries(TPCHQ18 aby3_Tests)
target_link_libraries(TPCHQ18 oc::tests_cryptoTools)


#############################################
##              bitcoinalpha               ##
#############################################

add_executable(graph
    graph.cpp
    join.cpp
    Permutation.cpp
    PrefixSumCircuit.cpp
    SingleRelationOperator.cpp
    sort.cpp
    baselinejoin.cpp
)

target_link_libraries(graph com-psi)
target_link_libraries(graph aby3-ML)
target_link_libraries(graph com-psi_Tests)
target_link_libraries(graph aby3_Tests)
target_link_libraries(graph oc::tests_cryptoTools)


#############################################
##                genranks                 ##
#############################################

add_executable(genranks
    genranks.cpp
    join.cpp
    Permutation.cpp
    PrefixSumCircuit.cpp
    SingleRelationOperator.cpp
    sort.cpp
    baselinejoin.cpp
)

target_link_libraries(genranks com-psi)
target_link_libraries(genranks aby3-ML)
target_link_libraries(genranks com-psi_Tests)
target_link_libraries(genranks aby3_Tests)
target_link_libraries(genranks oc::tests_cryptoTools)
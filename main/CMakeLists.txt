add_executable(1_filter_project 1_filter_project.cpp)
add_executable(2_aggregate 2_aggregate.cpp)
add_executable(3_join_continuous_print 3_join_continuous_print.cpp)
add_executable(4_web_client 4_web_client.cpp)
add_executable(5_continuous_arrival 5_continuous_arrival.cpp)

target_link_libraries(1_filter_project PRIVATE AliceDB)
target_link_libraries(2_aggregate PRIVATE AliceDB)
target_link_libraries(3_join_continuous_print PRIVATE AliceDB)
target_link_libraries(4_web_client PRIVATE AliceDB)
target_link_libraries(5_continuous_arrival PRIVATE AliceDB)

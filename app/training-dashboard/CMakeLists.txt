cmake_minimum_required(VERSION 3.20)

find_package(Qt5 REQUIRED COMPONENTS Core Widgets Network)

set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)

# Header files with Q_OBJECT macros must be listed so AUTOMOC processes them.
add_executable(training-dashboard
    main.cc
    models/metrics_client.h
    viewmodels/dashboard_viewmodel.h
    views/main_window.h
    views/topology_widget.h
    views/training_chart_widget.h
    views/actor_table_widget.h
    views/network_widget.h
    views/alert_panel_widget.h
)

target_include_directories(training-dashboard PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/models
    ${CMAKE_CURRENT_SOURCE_DIR}/viewmodels
    ${CMAKE_CURRENT_SOURCE_DIR}/views
    ${CMAKE_CURRENT_SOURCE_DIR}/resources
)

# Qt5Gui requires OpenGL. The conda gcc linker strips system library paths,
# so we explicitly add the system GL lib directory and rpath-link.
target_link_options(training-dashboard PRIVATE
    -L/usr/lib/x86_64-linux-gnu
    -Wl,-rpath-link,/usr/lib/x86_64-linux-gnu
)
target_link_libraries(training-dashboard PRIVATE Qt5::Core Qt5::Widgets Qt5::Network GL)

qt5_add_resources(training-dashboard RESOURCES resources/resources.qrc)

// Training Dashboard — Qt5 MVVM application for monitoring distributed training.
//
// Build:
//   cd build && cmake .. && make training-dashboard
// Run:
//   ./bin/training-dashboard
//
// Connect to a running PS node (default http://127.0.0.1:18080).

#include <QApplication>
#include "viewmodels/dashboard_viewmodel.h"
#include "views/main_window.h"

using dashboard::viewmodel::DashboardViewModel;
using dashboard::view::MainWindow;

int main(int argc, char* argv[]) {
    QApplication app(argc, argv);
    app.setApplicationName("Ultra-Net Training Dashboard");
    app.setOrganizationName("ultra-net");

    // Create ViewModel (owns the HTTP client).
    DashboardViewModel view_model;

    // Create View (connects to ViewModel via signals/slots).
    MainWindow window(&view_model);
    window.show();

    // Auto-connect if argument provided.
    if (argc > 1) {
        view_model.connect_to(QString::fromUtf8(argv[1]));
    }

    return app.exec();
}

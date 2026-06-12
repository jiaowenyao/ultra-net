// View: Node topology graph — shows PS + Workers as connected nodes.
#pragma once

#include <QGraphicsView>
#include <QGraphicsScene>
#include <QGraphicsEllipseItem>
#include <QGraphicsLineItem>
#include <QGraphicsTextItem>
#include <QVector>
#include <QMap>
#include <cmath>

#include "../models/node_info.h"

namespace dashboard::view {

using model::NodeInfo;
using model::NodeStatus;

class TopologyWidget : public QGraphicsView {
    Q_OBJECT
public:
    explicit TopologyWidget(QWidget* parent = nullptr)
        : QGraphicsView(parent), m_scene(new QGraphicsScene(this)) {
        setScene(m_scene);
        setRenderHint(QPainter::Antialiasing);
        setDragMode(QGraphicsView::ScrollHandDrag);
        setBackgroundBrush(QColor(0x0a, 0x0e, 0x14));
        setMinimumHeight(250);
    }

public slots:
    void update_nodes(const QVector<NodeInfo>& nodes) {
        m_scene->clear();
        if (nodes.isEmpty()) return;

        // Layout: PS in center, workers in a circle around it.
        const double cx = 200.0;
        const double cy = 130.0;
        const double radius = 100.0;
        const double node_r = 22.0;

        // Draw PS (first node).
        auto* ps = draw_node(cx, cy, node_r, "PS", nodes[0].status);
        m_scene->addItem(ps);

        // Draw workers in a circle.
        for (int i = 1; i < nodes.size(); ++i) {
            double angle = 2.0 * M_PI * (i - 1) / (nodes.size() - 1);
            double wx = cx + radius * std::cos(angle);
            double wy = cy + radius * std::sin(angle);

            auto* w = draw_node(wx, wy, node_r,
                                QString("W%1").arg(i - 1), nodes[i].status);
            m_scene->addItem(w);

            // Draw connection line.
            auto* line = m_scene->addLine(cx, cy, wx, wy,
                QPen(QColor(0x1a, 0x23, 0x32), 1.5));
            line->setZValue(-1);
        }

        m_scene->setSceneRect(m_scene->itemsBoundingRect().adjusted(-20, -20, 20, 20));
        fitInView(m_scene->sceneRect(), Qt::KeepAspectRatio);
    }

private:
    QGraphicsScene* m_scene;

    QGraphicsItem* draw_node(double x, double y, double r,
                              const QString& label, NodeStatus status) {
        auto* group = new QGraphicsItemGroup();

        // Circle.
        QColor color = (status == NodeStatus::Online) ? QColor(0x7f, 0xd9, 0x62)
                     : (status == NodeStatus::Degraded) ? QColor(0xff, 0xcc, 0x66)
                     : QColor(0xf2, 0x6d, 0x78);
        auto* circle = new QGraphicsEllipseItem(x - r, y - r, r * 2, r * 2);
        circle->setBrush(color);
        circle->setPen(QPen(color.darker(150), 1.5));
        group->addToGroup(circle);

        // Glow effect (outer ring).
        auto* glow = new QGraphicsEllipseItem(x - r - 3, y - r - 3, (r + 3) * 2, (r + 3) * 2);
        glow->setPen(QPen(QColor(color.red(), color.green(), color.blue(), 60), 2));
        glow->setBrush(Qt::NoBrush);
        group->addToGroup(glow);

        // Label.
        auto* text = new QGraphicsTextItem(label);
        text->setDefaultTextColor(QColor(0xbf, 0xc7, 0xd5));
        QFont f("Consolas", 9, QFont::Bold);
        text->setFont(f);
        text->setPos(x - text->boundingRect().width() / 2, y + r + 4);
        group->addToGroup(text);

        group->setZValue(1);
        return group;
    }
};

} // namespace dashboard::view

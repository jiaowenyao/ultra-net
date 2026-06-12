// Model: Alert rules and alert state for the alert panel.
#pragma once

#include <QString>
#include <QDateTime>
#include <QVector>
#include <functional>

namespace dashboard::model {

enum class AlertSeverity { Info, Warning, Critical };

struct Alert {
    QString       id;
    QString       title;
    QString       message;
    AlertSeverity severity = AlertSeverity::Info;
    QDateTime     raised_at;
    QDateTime     cleared_at;
    bool          active = true;

    QString severity_string() const {
        switch (severity) {
            case AlertSeverity::Info:     return "INFO";
            case AlertSeverity::Warning:  return "WARNING";
            case AlertSeverity::Critical: return "CRITICAL";
        }
        return "INFO";
    }
};

// An alert rule evaluates a condition and produces an Alert.
struct AlertRule {
    QString       rule_id;
    QString       title_template;
    AlertSeverity severity = AlertSeverity::Warning;
    int           cooldown_seconds = 60;  // Minimum interval between re-triggers.
};

// Engine that evaluates rules and manages active alerts.
class AlertEngine {
public:
    void add_rule(const AlertRule& rule) {
        m_rules.append(rule);
    }

    // Evaluate all rules. Each rule's `evaluate` callback returns
    // an alert message if triggered, or empty string if not.
    void evaluate(std::function<QString(const AlertRule&)> evaluator) {
        QDateTime now = QDateTime::currentDateTime();

        for (const auto& rule : m_rules) {
            QString msg = evaluator(rule);
            if (msg.isEmpty()) {
                // Rule not triggered — clear any active alert for this rule.
                clear_alert(rule.rule_id, now);
            } else {
                // Rule triggered — raise or update alert.
                raise_alert(rule, msg, now);
            }
        }
    }

    const QVector<Alert>& active_alerts() const { return m_active_alerts; }

    QVector<Alert> alerts_by_severity(AlertSeverity sev) const {
        QVector<Alert> result;
        for (const auto& a : m_active_alerts) {
            if (a.severity == sev && a.active) {
                result.append(a);
            }
        }
        return result;
    }

    int active_count() const {
        int count = 0;
        for (const auto& a : m_active_alerts) {
            if (a.active) {
                ++count;
            }
        }
        return count;
    }

    int critical_count() const {
        return alerts_by_severity(AlertSeverity::Critical).size();
    }

    void clear_all() {
        m_active_alerts.clear();
        m_alert_history.clear();
    }

private:
    QVector<AlertRule> m_rules;
    QVector<Alert> m_active_alerts;
    QVector<Alert> m_alert_history;  // Cleared alerts for history view.

    void raise_alert(const AlertRule& rule, const QString& msg, const QDateTime& now) {
        // Check if this rule already has an active alert.
        for (auto& a : m_active_alerts) {
            if (a.id == rule.rule_id && a.active) {
                // Update message if it changed.
                a.message = msg;
                return;
            }
        }

        // Check cooldown: don't re-raise if recently cleared.
        for (const auto& a : m_alert_history) {
            if (a.id == rule.rule_id) {
                qint64 elapsed = a.cleared_at.secsTo(now);
                if (elapsed < rule.cooldown_seconds) {
                    return;
                }
            }
        }

        // Raise new alert.
        Alert alert;
        alert.id       = rule.rule_id;
        alert.title    = rule.title_template;
        alert.message  = msg;
        alert.severity = rule.severity;
        alert.raised_at = now;
        alert.active   = true;
        m_active_alerts.append(alert);
    }

    void clear_alert(const QString& rule_id, const QDateTime& now) {
        for (auto& a : m_active_alerts) {
            if (a.id == rule_id && a.active) {
                a.active = false;
                a.cleared_at = now;
                m_alert_history.append(a);
                return;
            }
        }
    }
};

// Pre-defined alert rules for distributed training.
inline QVector<AlertRule> default_alert_rules() {
    QVector<AlertRule> rules;

    {
        AlertRule r;
        r.rule_id         = "node_offline";
        r.title_template  = "Node Offline";
        r.severity        = AlertSeverity::Critical;
        r.cooldown_seconds = 30;
        rules.append(r);
    }
    {
        AlertRule r;
        r.rule_id         = "high_latency";
        r.title_template  = "High Latency Detected";
        r.severity        = AlertSeverity::Warning;
        r.cooldown_seconds = 60;
        rules.append(r);
    }
    {
        AlertRule r;
        r.rule_id         = "throughput_drop";
        r.title_template  = "Throughput Drop";
        r.severity        = AlertSeverity::Warning;
        r.cooldown_seconds = 120;
        rules.append(r);
    }
    {
        AlertRule r;
        r.rule_id         = "actor_queue_full";
        r.title_template  = "Actor Queue Approaching Capacity";
        r.severity        = AlertSeverity::Warning;
        r.cooldown_seconds = 30;
        rules.append(r);
    }
    {
        AlertRule r;
        r.rule_id         = "checksum_mismatch";
        r.title_template  = "Data Integrity Check Failed";
        r.severity        = AlertSeverity::Critical;
        r.cooldown_seconds = 15;
        rules.append(r);
    }

    return rules;
}

} // namespace dashboard::model

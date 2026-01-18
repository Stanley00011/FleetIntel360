-- warehouse/sql/seed/alert_thresholds.sql

INSERT OR REPLACE INTO mart.alert_thresholds VALUES
('avg_fatigue_index', 0.6, 0.8, '>', 'Driver fatigue risk', TRUE),
('speeding_rate', 0.08, 0.12, '>', 'High speeding frequency', TRUE),
('engine_temp_c', 90, 120, '>', 'Engine overheating risk', TRUE),
('battery_voltage', 11.8, 11.2, '<', 'Low vehicle battery', TRUE),
('fraud_alerts_count', 1, 3, '>', 'Potential fraud detected', TRUE);

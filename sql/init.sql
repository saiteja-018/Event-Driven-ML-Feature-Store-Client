CREATE TABLE IF NOT EXISTS features (
    entity_id VARCHAR(255) NOT NULL,
    feature_name VARCHAR(255) NOT NULL,
    feature_value TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_id, feature_name)
);

CREATE INDEX IF NOT EXISTS idx_features_entity_id ON features(entity_id);

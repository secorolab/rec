CREATE TABLE IF NOT EXISTS {prefix}_runs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL UNIQUE,
    bundle VARCHAR(45),
    state VARCHAR(20) NOT NULL,
    verdict VARCHAR(20) NOT NULL,
    queued_time DATETIME(6),
    start_time DATETIME(6),
    end_time DATETIME(6),
    heartbeat_time DATETIME(6),
    fail_trace TEXT,
    host_hostname TEXT,
    host_os TEXT,
    host_python TEXT,
    host_cpu TEXT
);

CREATE TABLE IF NOT EXISTS {prefix}_packages (
    run INT NOT NULL,
    purl VARCHAR(512) NOT NULL,
    name TEXT NOT NULL,
    version TEXT,
    revision TEXT,
    repository TEXT,
    dirty BOOL NOT NULL,
    recorder BOOL NOT NULL,
    UNIQUE (run, purl, recorder),
    FOREIGN KEY (run) REFERENCES {prefix}_runs (id)
);

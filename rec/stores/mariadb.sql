CREATE TABLE IF NOT EXISTS {prefix}_runs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL UNIQUE,
    state VARCHAR(20) NOT NULL,
    verdict VARCHAR(20) NOT NULL,
    queued_time DATETIME(6),
    start_time DATETIME(6),
    end_time DATETIME(6),
    heartbeat_time DATETIME(6),
    fail_trace TEXT,
    result TEXT,
    trigger_entity TEXT,
    trigger_generator TEXT,
    program_path TEXT,
    program_root TEXT,
    program_title TEXT,
    program_sha256 CHAR(64),
    program_size_bytes BIGINT,
    recorder_name TEXT,
    recorder_version TEXT,
    recorder_commit TEXT,
    recorder_repository TEXT,
    host_hostname TEXT,
    host_os TEXT,
    host_python TEXT,
    host_cpu TEXT
);

CREATE TABLE IF NOT EXISTS {prefix}_agents (
    run INT NOT NULL,
    id TEXT NOT NULL,
    type VARCHAR(20) NOT NULL,
    name TEXT,
    UNIQUE (run, id),
    FOREIGN KEY (run) REFERENCES {prefix}_runs (id)
);

CREATE TABLE IF NOT EXISTS {prefix}_activities (
    run INT NOT NULL,
    id TEXT NOT NULL,
    kind VARCHAR(20) NOT NULL,
    agent TEXT NOT NULL,
    start_time DATETIME(6) NOT NULL,
    end_time DATETIME(6),
    UNIQUE (run, id),
    FOREIGN KEY (run) REFERENCES {prefix}_runs (id)
);

CREATE TABLE IF NOT EXISTS {prefix}_repositories (
    run INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    url TEXT,
    `commit` TEXT,
    UNIQUE (run, name),
    FOREIGN KEY (run) REFERENCES {prefix}_runs (id)
);

CREATE TABLE IF NOT EXISTS {prefix}_dependencies (
    run INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    version TEXT,
    UNIQUE (run, name),
    FOREIGN KEY (run) REFERENCES {prefix}_runs (id)
);

CREATE TABLE IF NOT EXISTS {prefix}_resources (
    run INT NOT NULL,
    path TEXT NOT NULL,
    root TEXT NOT NULL,
    title TEXT,
    sha256 CHAR(64),
    size_bytes BIGINT,
    activity TEXT NOT NULL,
    time DATETIME(6),
    UNIQUE (run, activity, path, root),
    FOREIGN KEY (run) REFERENCES {prefix}_runs (id)
);

CREATE TABLE IF NOT EXISTS {prefix}_artefacts (
    run INT NOT NULL,
    path TEXT NOT NULL,
    root TEXT NOT NULL,
    title TEXT,
    sha256 CHAR(64),
    size_bytes BIGINT,
    activity TEXT NOT NULL,
    time DATETIME(6),
    UNIQUE (run, path, root),
    FOREIGN KEY (run) REFERENCES {prefix}_runs (id)
);

CREATE TABLE IF NOT EXISTS {prefix}_metrics (
    run INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    step INT NOT NULL,
    value DOUBLE NOT NULL,
    is_integer BOOL NOT NULL,
    time DATETIME(6),
    UNIQUE (run, name, step),
    FOREIGN KEY (run) REFERENCES {prefix}_runs (id)
);

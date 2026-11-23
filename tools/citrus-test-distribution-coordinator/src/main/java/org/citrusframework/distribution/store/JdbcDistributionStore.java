/*
 * Copyright the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.citrusframework.distribution.store;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.dbcp2.BasicDataSource;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.model.BuildSessionRecord;
import org.citrusframework.distribution.model.PartitionRecord;
import org.citrusframework.distribution.model.PartitionState;
import org.citrusframework.distribution.model.SessionStatus;
import org.citrusframework.distribution.model.SessionSummary;
import org.citrusframework.distribution.model.SubmitWorkResponse;
import org.citrusframework.distribution.model.TestExecutionRecord;
import org.citrusframework.distribution.model.TestHistorySummary;
import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkerCacheStats;
import org.citrusframework.distribution.model.WorkerRecord;
import org.citrusframework.distribution.model.WorkerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;

/**
 * JDBC implementation of {@link DistributionStore} supporting both embedded/file HSQLDB
 * and PostgreSQL with standard SQL DDL.
 */
public class JdbcDistributionStore implements DistributionStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDistributionStore.class);

    private static final TypeReference<Map<String, String>> STRING_MAP_TYPE = new TypeReference<>() {};
    private static final TypeReference<List<WorkUnitSpec>> WORK_UNITS_TYPE = new TypeReference<>() {};
    private static final TypeReference<List<String>> STRING_LIST_TYPE = new TypeReference<>() {};

    private final String jdbcUrl;
    private final BasicDataSource dataSource;

    public JdbcDistributionStore(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.dataSource = new BasicDataSource();
        this.dataSource.setDriverClassLoader(JdbcDistributionStore.class.getClassLoader());
        String driverClass = resolveDriverClassName(jdbcUrl);
        if (driverClass != null) {
            this.dataSource.setDriverClassName(driverClass);
        }
        this.dataSource.setUrl(jdbcUrl);
        this.dataSource.setUsername(username != null ? username : "SA");
        this.dataSource.setPassword(password != null ? password : "");
        this.dataSource.setMinIdle(1);
        this.dataSource.setMaxTotal(8);
    }

    public static String resolveDriverClassName(String jdbcUrl) {
        if (jdbcUrl == null) {
            return null;
        }
        String lower = jdbcUrl.toLowerCase(Locale.ROOT);
        if (lower.startsWith("jdbc:hsqldb:")) {
            return "org.hsqldb.jdbc.JDBCDriver";
        }
        if (lower.startsWith("jdbc:postgresql:")) {
            return "org.postgresql.Driver";
        }
        return null;
    }

    public static String resolveLongTextType(String jdbcUrl) {
        if (jdbcUrl != null && jdbcUrl.toLowerCase(Locale.ROOT).startsWith("jdbc:postgresql:")) {
            return "TEXT";
        }
        return "CLOB";
    }

    @Override
    public synchronized void initialize() {
        String longText = resolveLongTextType(jdbcUrl);
        List<String> ddl = List.of(
                "CREATE TABLE IF NOT EXISTS dist_sessions (" +
                        "session_id VARCHAR(64) PRIMARY KEY, " +
                        "project_name VARCHAR(255), " +
                        "git_branch VARCHAR(255), " +
                        "git_commit VARCHAR(64), " +
                        "status VARCHAR(32), " +
                        "started_at BIGINT, " +
                        "finished_at BIGINT, " +
                        "total_tests INT, " +
                        "passed_tests INT, " +
                        "failed_tests INT, " +
                        "skipped_tests INT, " +
                        "total_blobs INT, " +
                        "cached_blobs INT, " +
                        "serial_duration_ms BIGINT, " +
                        "wall_duration_ms BIGINT, " +
                        "failure_reason VARCHAR(4096))",
                "CREATE TABLE IF NOT EXISTS dist_session_changes (" +
                        "session_id VARCHAR(64), " +
                        "file_path VARCHAR(1024), " +
                        "PRIMARY KEY (session_id, file_path))",
                "CREATE TABLE IF NOT EXISTS dist_workers (" +
                        "worker_id VARCHAR(128) PRIMARY KEY, " +
                        "host VARCHAR(255), " +
                        "capabilities_json VARCHAR(4096), " +
                        "state VARCHAR(32), " +
                        "active_partition_id VARCHAR(64), " +
                        "last_heartbeat_at BIGINT, " +
                        "registered_at BIGINT, " +
                        "cache_stats_json VARCHAR(2048))",
                "CREATE TABLE IF NOT EXISTS dist_partitions (" +
                        "partition_id VARCHAR(64) NOT NULL, " +
                        "session_id VARCHAR(64), " +
                        "worker_id VARCHAR(128), " +
                        "state VARCHAR(32), " +
                        "attempt INT NOT NULL, " +
                        "estimated_duration_ms BIGINT, " +
                        "started_at BIGINT, " +
                        "finished_at BIGINT, " +
                        "required_caps_json VARCHAR(4096), " +
                        "work_units_json " + longText + ", " +
                        "module_name VARCHAR(255), " +
                        "fork_workspace BOOLEAN, " +
                        "working_directory VARCHAR(1024), " +
                        "inputs_json " + longText + ", " +
                        "outputs_json " + longText + ", " +
                        "produced_outputs_json " + longText + ", " +
                        "error_message VARCHAR(4096), " +
                        "PRIMARY KEY (partition_id, attempt))",
                "CREATE TABLE IF NOT EXISTS dist_work_submissions (" +
                        "session_id VARCHAR(64) NOT NULL, " +
                        "submission_id VARCHAR(128) NOT NULL, " +
                        "response_json " + longText + " NOT NULL, " +
                        "recorded_at BIGINT NOT NULL, " +
                        "PRIMARY KEY (session_id, submission_id))",
                "CREATE TABLE IF NOT EXISTS dist_test_executions (" +
                        "id VARCHAR(64) PRIMARY KEY, " +
                        "session_id VARCHAR(64), " +
                        "partition_id VARCHAR(64), " +
                        "worker_id VARCHAR(128), " +
                        "work_unit_key VARCHAR(512), " +
                        "class_name VARCHAR(512), " +
                        "test_name VARCHAR(512), " +
                        "status VARCHAR(32), " +
                        "duration_ms BIGINT, " +
                        "attempt INT, " +
                        "flaky BOOLEAN, " +
                        "error_message VARCHAR(4096), " +
                        "stack_trace " + longText + ", " +
                        "endpoints_json VARCHAR(4096), " +
                        "recorded_at BIGINT, " +
                        "unique_id VARCHAR(1024))"
        );

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            for (String sql : ddl) {
                stmt.execute(sql);
            }
            ensureColumns(conn, stmt, longText);
            // Lookups by session (session detail) and by work unit (historical durations for scheduling).
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_dist_tests_session ON dist_test_executions (session_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_dist_tests_unit ON dist_test_executions (work_unit_key)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_dist_parts_session ON dist_partitions (session_id)");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize distribution schema on " + jdbcUrl, e);
        }
    }

    /** Adds columns introduced after a table's first release, scanning the schema once. */
    private static void ensureColumns(Connection conn, Statement stmt, String longText) throws SQLException {
        Map<String, java.util.Set<String>> existing = new java.util.HashMap<>();
        String partitionsTable = null;
        try (ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(), conn.getSchema(), null, null)) {
            while (rs.next()) {
                String table = rs.getString("TABLE_NAME");
                String col = rs.getString("COLUMN_NAME");
                if (table != null && col != null) {
                    existing.computeIfAbsent(table.toLowerCase(Locale.ROOT), k -> new java.util.HashSet<>())
                            .add(col.toLowerCase(Locale.ROOT));
                    if ("dist_partitions".equalsIgnoreCase(table)) {
                        partitionsTable = table;
                    }
                }
            }
        }
        Map<String, Map<String, String>> additions = new LinkedHashMap<>();
        Map<String, String> partitionColumns = new LinkedHashMap<>();
        partitionColumns.put("module_name", "VARCHAR(255)");
        partitionColumns.put("fork_workspace", "BOOLEAN");
        partitionColumns.put("working_directory", "VARCHAR(1024)");
        partitionColumns.put("inputs_json", longText);
        partitionColumns.put("outputs_json", longText);
        partitionColumns.put("produced_outputs_json", longText);
        partitionColumns.put("error_message", "VARCHAR(4096)");
        additions.put("dist_partitions", partitionColumns);
        additions.put("dist_workers", Map.of("cache_stats_json", "VARCHAR(2048)"));
        additions.put("dist_sessions", Map.of("failure_reason", "VARCHAR(4096)"));
        additions.put("dist_test_executions", Map.of("unique_id", "VARCHAR(1024)"));
        for (Map.Entry<String, Map<String, String>> table : additions.entrySet()) {
            java.util.Set<String> columns = existing.getOrDefault(table.getKey(), java.util.Set.of());
            for (Map.Entry<String, String> column : table.getValue().entrySet()) {
                if (!columns.contains(column.getKey())) {
                    addColumn(conn, stmt, table.getKey(), column.getKey(), column.getValue());
                }
            }
        }
        ensurePartitionPrimaryKey(conn, stmt, partitionsTable);
    }

    /**
     * Adds {@code column} to {@code table}. A failure is ignored when the column exists by now: another coordinator
     * sharing the database added it between this one's schema scan and its {@code ALTER TABLE}.
     */
    static void addColumn(Connection conn, Statement stmt, String table, String column, String type) throws SQLException {
        try {
            stmt.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " " + type);
        } catch (SQLException e) {
            if (!columnExists(conn, table, column)) {
                throw e;
            }
        }
    }

    private static boolean columnExists(Connection conn, String table, String column) throws SQLException {
        try (ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(), conn.getSchema(), null, null)) {
            while (rs.next()) {
                if (table.equalsIgnoreCase(rs.getString("TABLE_NAME")) && column.equalsIgnoreCase(rs.getString("COLUMN_NAME"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void ensurePartitionPrimaryKey(Connection conn, Statement stmt, String actualTableName) throws SQLException {
        if (actualTableName != null) {
            java.util.Set<String> pkCols = new java.util.HashSet<>();
            String pkName = null;
            try (ResultSet pkRs = conn.getMetaData().getPrimaryKeys(conn.getCatalog(), conn.getSchema(), actualTableName)) {
                while (pkRs.next()) {
                    String col = pkRs.getString("COLUMN_NAME");
                    if (col != null) {
                        pkCols.add(col.toLowerCase(Locale.ROOT));
                    }
                    if (pkName == null) {
                        pkName = pkRs.getString("PK_NAME");
                    }
                }
            }
            if (!pkCols.isEmpty() && !pkCols.contains("attempt")) {
                stmt.execute("UPDATE dist_partitions SET attempt = 1 WHERE attempt IS NULL");
                stmt.execute("ALTER TABLE dist_partitions ALTER COLUMN attempt SET NOT NULL");
                if (pkName != null && pkName.matches("[A-Za-z0-9_]+")) {
                    stmt.execute("ALTER TABLE dist_partitions DROP CONSTRAINT " + pkName);
                } else {
                    stmt.execute("ALTER TABLE dist_partitions DROP PRIMARY KEY");
                }
                stmt.execute("ALTER TABLE dist_partitions ADD PRIMARY KEY (partition_id, attempt)");
            }
        }
    }

    @Override
    public synchronized void saveSession(BuildSessionRecord s) {
        String updateSql = "UPDATE dist_sessions SET project_name=?, git_branch=?, git_commit=?, status=?, " +
                "started_at=?, finished_at=?, total_tests=?, passed_tests=?, failed_tests=?, skipped_tests=?, " +
                "total_blobs=?, cached_blobs=?, serial_duration_ms=?, wall_duration_ms=?, failure_reason=? WHERE session_id=?";
        String insertSql = "INSERT INTO dist_sessions (session_id, project_name, git_branch, git_commit, status, " +
                "started_at, finished_at, total_tests, passed_tests, failed_tests, skipped_tests, " +
                "total_blobs, cached_blobs, serial_duration_ms, wall_duration_ms, failure_reason) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                ps.setString(1, s.projectName());
                ps.setString(2, s.gitBranch());
                ps.setString(3, s.gitCommit());
                ps.setString(4, s.status().name());
                ps.setLong(5, s.startedAt());
                ps.setLong(6, s.finishedAt());
                ps.setInt(7, s.totalTests());
                ps.setInt(8, s.passedTests());
                ps.setInt(9, s.failedTests());
                ps.setInt(10, s.skippedTests());
                ps.setInt(11, s.totalBlobs());
                ps.setInt(12, s.cachedBlobs());
                ps.setLong(13, s.serialDurationMs());
                ps.setLong(14, s.wallDurationMs());
                ps.setString(15, truncate(s.failureReason(), 4096));
                ps.setString(16, s.sessionId());
                if (ps.executeUpdate() > 0) {
                    return;
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                ps.setString(1, s.sessionId());
                ps.setString(2, s.projectName());
                ps.setString(3, s.gitBranch());
                ps.setString(4, s.gitCommit());
                ps.setString(5, s.status().name());
                ps.setLong(6, s.startedAt());
                ps.setLong(7, s.finishedAt());
                ps.setInt(8, s.totalTests());
                ps.setInt(9, s.passedTests());
                ps.setInt(10, s.failedTests());
                ps.setInt(11, s.skippedTests());
                ps.setInt(12, s.totalBlobs());
                ps.setInt(13, s.cachedBlobs());
                ps.setLong(14, s.serialDurationMs());
                ps.setLong(15, s.wallDurationMs());
                ps.setString(16, truncate(s.failureReason(), 4096));
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to save build session " + s.sessionId(), e);
        }
    }

    @Override
    public synchronized void updateSessionStatus(String sessionId, SessionStatus status, long finishedAt,
                                                 SessionSummary summary, String failureReason) {
        String sql = "UPDATE dist_sessions SET status=?, finished_at=?, total_tests=?, passed_tests=?, " +
                "failed_tests=?, skipped_tests=?, total_blobs=?, cached_blobs=?, serial_duration_ms=?, wall_duration_ms=?, " +
                "failure_reason=? WHERE session_id=?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status.name());
            ps.setLong(2, finishedAt);
            ps.setInt(3, summary.totalTests());
            ps.setInt(4, summary.passedTests());
            ps.setInt(5, summary.failedTests());
            ps.setInt(6, summary.skippedTests());
            ps.setInt(7, summary.totalBlobs());
            ps.setInt(8, summary.cachedBlobs());
            ps.setLong(9, summary.serialDurationMs());
            ps.setLong(10, summary.wallDurationMs());
            ps.setString(11, truncate(failureReason, 4096));
            ps.setString(12, sessionId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to update session status for " + sessionId, e);
        }
    }

    @Override
    public Optional<BuildSessionRecord> findSession(String sessionId) {
        String sql = "SELECT session_id, project_name, git_branch, git_commit, status, started_at, finished_at, " +
                "total_tests, passed_tests, failed_tests, skipped_tests, total_blobs, cached_blobs, " +
                "serial_duration_ms, wall_duration_ms, failure_reason FROM dist_sessions WHERE session_id=?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    BuildSessionRecord session = mapSession(rs);
                    return Optional.of(session.withModules(
                            findModules(conn, List.of(session.sessionId())).getOrDefault(session.sessionId(), List.of())));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to find session " + sessionId, e);
        }
    }

    @Override
    public List<BuildSessionRecord> listSessions(int limit) {
        String sql = "SELECT session_id, project_name, git_branch, git_commit, status, started_at, finished_at, " +
                "total_tests, passed_tests, failed_tests, skipped_tests, total_blobs, cached_blobs, " +
                "serial_duration_ms, wall_duration_ms, failure_reason FROM dist_sessions ORDER BY started_at DESC";
        List<BuildSessionRecord> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setMaxRows(Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(mapSession(rs));
                }
            }
            Map<String, List<String>> modules = findModules(conn, result.stream().map(BuildSessionRecord::sessionId).toList());
            List<BuildSessionRecord> withModules = new ArrayList<>(result.size());
            for (BuildSessionRecord session : result) {
                withModules.add(session.withModules(modules.getOrDefault(session.sessionId(), List.of())));
            }
            return withModules;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to list sessions", e);
        }
    }

    @Override
    public synchronized void saveChangedFiles(String sessionId, List<String> changedFiles) {
        if (changedFiles == null || changedFiles.isEmpty()) {
            return;
        }
        String deleteSql = "DELETE FROM dist_session_changes WHERE session_id=?";
        String insertSql = "INSERT INTO dist_session_changes (session_id, file_path) VALUES (?, ?)";
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement del = conn.prepareStatement(deleteSql)) {
                del.setString(1, sessionId);
                del.executeUpdate();
            }
            try (PreparedStatement ins = conn.prepareStatement(insertSql)) {
                for (String path : changedFiles.stream().distinct().toList()) {
                    ins.setString(1, sessionId);
                    ins.setString(2, truncate(path, 1024));
                    ins.addBatch();
                }
                ins.executeBatch();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to save changed files for " + sessionId, e);
        }
    }

    @Override
    public List<String> findChangedFiles(String sessionId) {
        String sql = "SELECT file_path FROM dist_session_changes WHERE session_id=? ORDER BY file_path ASC";
        List<String> files = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    files.add(rs.getString(1));
                }
            }
            return files;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read changed files for " + sessionId, e);
        }
    }

    @Override
    public synchronized void upsertWorker(WorkerRecord w) {
        String capsJson = truncate(DistributionJson.toJson(w.capabilities() != null ? w.capabilities() : Map.of()), 4096);
        String updateSql = "UPDATE dist_workers SET host=?, capabilities_json=?, state=?, active_partition_id=?, " +
                "last_heartbeat_at=?, registered_at=?, cache_stats_json=? WHERE worker_id=?";
        String insertSql = "INSERT INTO dist_workers (worker_id, host, capabilities_json, state, active_partition_id, " +
                "last_heartbeat_at, registered_at, cache_stats_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        String cacheJson = w.cacheStats() != null ? DistributionJson.toJson(w.cacheStats()) : null;
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                ps.setString(1, w.host());
                ps.setString(2, capsJson);
                ps.setString(3, w.state().name());
                ps.setString(4, w.activePartitionId());
                ps.setLong(5, w.lastHeartbeatAt());
                ps.setLong(6, w.registeredAt());
                ps.setString(7, cacheJson);
                ps.setString(8, w.workerId());
                if (ps.executeUpdate() > 0) {
                    return;
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                ps.setString(1, w.workerId());
                ps.setString(2, w.host());
                ps.setString(3, capsJson);
                ps.setString(4, w.state().name());
                ps.setString(5, w.activePartitionId());
                ps.setLong(6, w.lastHeartbeatAt());
                ps.setLong(7, w.registeredAt());
                ps.setString(8, cacheJson);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to upsert worker " + w.workerId(), e);
        }
    }

    @Override
    public List<WorkerRecord> listWorkers() {
        String sql = "SELECT worker_id, host, capabilities_json, state, active_partition_id, " +
                "last_heartbeat_at, registered_at, cache_stats_json FROM dist_workers ORDER BY registered_at ASC";
        List<WorkerRecord> workers = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String workerId = rs.getString("worker_id");
                Map<String, String> caps = readWorkerCapabilities(rs.getString("capabilities_json"), workerId);
                WorkerCacheStats cacheStats = readWorkerCacheStats(rs.getString("cache_stats_json"), workerId);
                workers.add(new WorkerRecord(
                        workerId,
                        rs.getString("host"),
                        caps,
                        WorkerState.valueOf(rs.getString("state")),
                        rs.getString("active_partition_id"),
                        rs.getLong("last_heartbeat_at"),
                        rs.getLong("registered_at"),
                        cacheStats
                ));
            }
            return workers;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to list workers", e);
        }
    }

    private static Map<String, String> readWorkerCapabilities(String json, String workerId) {
        if (json != null && !json.isBlank()) {
            try {
                Map<String, String> capabilities = DistributionJson.fromJson(json, STRING_MAP_TYPE);
                if (capabilities != null) {
                    return capabilities;
                }
            } catch (JacksonException e) {
                LOG.warn("Ignoring malformed capabilities JSON for worker {}", workerId);
            }
        }
        return Map.of();
    }

    private static WorkerCacheStats readWorkerCacheStats(String json, String workerId) {
        if (json != null && !json.isBlank()) {
            try {
                return DistributionJson.fromJson(json, WorkerCacheStats.class);
            } catch (JacksonException e) {
                LOG.warn("Ignoring malformed cache statistics JSON for worker {}", workerId);
            }
        }
        return null;
    }

    @Override
    public synchronized void savePartition(PartitionRecord p) {
        String capsJson = truncate(DistributionJson.toJson(p.requiredCapabilities() != null ? p.requiredCapabilities() : Map.of()), 4096);
        String unitsJson = DistributionJson.toJson(p.workUnits() != null ? p.workUnits() : List.of());
        String inputsJson = DistributionJson.toJson(p.inputPaths() != null ? p.inputPaths() : List.of());
        String outputsJson = DistributionJson.toJson(p.outputPaths() != null ? p.outputPaths() : List.of());
        String producedJson = DistributionJson.toJson(p.producedOutputs() != null ? p.producedOutputs() : List.of());
        String updateSql = "UPDATE dist_partitions SET session_id=?, worker_id=?, state=?, " +
                "estimated_duration_ms=?, started_at=?, finished_at=?, required_caps_json=?, work_units_json=?, " +
                "module_name=?, fork_workspace=?, working_directory=?, inputs_json=?, outputs_json=?, " +
                "produced_outputs_json=?, error_message=? " +
                "WHERE partition_id=? AND attempt=?";
        String insertSql = "INSERT INTO dist_partitions (partition_id, session_id, worker_id, state, attempt, " +
                "estimated_duration_ms, started_at, finished_at, required_caps_json, work_units_json, " +
                "module_name, fork_workspace, working_directory, inputs_json, outputs_json, produced_outputs_json, error_message) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                ps.setString(1, p.sessionId());
                ps.setString(2, p.workerId());
                ps.setString(3, p.state().name());
                ps.setLong(4, p.estimatedDurationMs());
                ps.setLong(5, p.startedAt());
                ps.setLong(6, p.finishedAt());
                ps.setString(7, capsJson);
                ps.setString(8, unitsJson);
                ps.setString(9, truncate(p.moduleName(), 255));
                ps.setBoolean(10, p.forkWorkspace());
                ps.setString(11, truncate(p.workingDirectory(), 1024));
                ps.setString(12, inputsJson);
                ps.setString(13, outputsJson);
                ps.setString(14, producedJson);
                ps.setString(15, truncate(p.errorMessage(), 4096));
                ps.setString(16, p.partitionId());
                ps.setInt(17, p.attempt());
                if (ps.executeUpdate() > 0) {
                    return;
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                ps.setString(1, p.partitionId());
                ps.setString(2, p.sessionId());
                ps.setString(3, p.workerId());
                ps.setString(4, p.state().name());
                ps.setInt(5, p.attempt());
                ps.setLong(6, p.estimatedDurationMs());
                ps.setLong(7, p.startedAt());
                ps.setLong(8, p.finishedAt());
                ps.setString(9, capsJson);
                ps.setString(10, unitsJson);
                ps.setString(11, truncate(p.moduleName(), 255));
                ps.setBoolean(12, p.forkWorkspace());
                ps.setString(13, truncate(p.workingDirectory(), 1024));
                ps.setString(14, inputsJson);
                ps.setString(15, outputsJson);
                ps.setString(16, producedJson);
                ps.setString(17, truncate(p.errorMessage(), 4096));
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to save partition " + p.partitionId(), e);
        }
    }

    @Override
    public List<PartitionRecord> listPartitions(String sessionId) {
        String sql = "SELECT partition_id, session_id, worker_id, state, attempt, estimated_duration_ms, " +
                "started_at, finished_at, required_caps_json, work_units_json, module_name, fork_workspace, " +
                "working_directory, inputs_json, outputs_json, produced_outputs_json, error_message " +
                "FROM dist_partitions WHERE session_id=? " +
                "ORDER BY CASE WHEN started_at > 0 THEN 0 ELSE 1 END ASC, started_at ASC, partition_id ASC, attempt ASC";
        List<PartitionRecord> partitions = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String capsRaw = rs.getString("required_caps_json");
                    String unitsRaw = rs.getString("work_units_json");
                    String inputsRaw = rs.getString("inputs_json");
                    String outputsRaw = rs.getString("outputs_json");
                    String producedRaw = rs.getString("produced_outputs_json");
                    partitions.add(new PartitionRecord(
                            rs.getString("partition_id"),
                            rs.getString("session_id"),
                            rs.getString("worker_id"),
                            PartitionState.valueOf(rs.getString("state")),
                            rs.getInt("attempt"),
                            rs.getLong("estimated_duration_ms"),
                            rs.getLong("started_at"),
                            rs.getLong("finished_at"),
                            capsRaw == null || capsRaw.isBlank() ? Map.of() : DistributionJson.fromJson(capsRaw, STRING_MAP_TYPE),
                            unitsRaw == null || unitsRaw.isBlank() ? List.of() : DistributionJson.fromJson(unitsRaw, WORK_UNITS_TYPE),
                            rs.getString("module_name"),
                            rs.getBoolean("fork_workspace"),
                            rs.getString("working_directory"),
                            inputsRaw == null || inputsRaw.isBlank() ? List.of() : DistributionJson.fromJson(inputsRaw, STRING_LIST_TYPE),
                            outputsRaw == null || outputsRaw.isBlank() ? List.of() : DistributionJson.fromJson(outputsRaw, STRING_LIST_TYPE),
                            producedRaw == null || producedRaw.isBlank() ? List.of() : DistributionJson.fromJson(producedRaw, STRING_LIST_TYPE),
                            rs.getString("error_message")
                    ));
                }
            }
            return partitions;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to list partitions for " + sessionId, e);
        }
    }

    @Override
    public synchronized void saveWorkSubmission(String sessionId, SubmitWorkResponse response) {
        if (sessionId == null || response == null || response.submissionId() == null) {
            return;
        }
        String responseJson = DistributionJson.toJson(response);
        long now = System.currentTimeMillis();
        String submissionId = truncate(response.submissionId(), 128);
        String updateSql = "UPDATE dist_work_submissions SET response_json=?, recorded_at=? " +
                "WHERE session_id=? AND submission_id=?";
        String insertSql = "INSERT INTO dist_work_submissions (session_id, submission_id, response_json, recorded_at) " +
                "VALUES (?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                ps.setString(1, responseJson);
                ps.setLong(2, now);
                ps.setString(3, sessionId);
                ps.setString(4, submissionId);
                if (ps.executeUpdate() > 0) {
                    return;
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                ps.setString(1, sessionId);
                ps.setString(2, submissionId);
                ps.setString(3, responseJson);
                ps.setLong(4, now);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to save work submission " + submissionId + " for session " + sessionId, e);
        }
    }

    @Override
    public Optional<SubmitWorkResponse> findWorkSubmission(String sessionId, String submissionId) {
        if (sessionId == null || submissionId == null) {
            return Optional.empty();
        }
        String sql = "SELECT response_json FROM dist_work_submissions WHERE session_id=? AND submission_id=?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sessionId);
            ps.setString(2, truncate(submissionId, 128));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String json = rs.getString(1);
                    if (json != null && !json.isBlank()) {
                        return Optional.of(DistributionJson.fromJson(json, SubmitWorkResponse.class));
                    }
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to find work submission " + submissionId + " for session " + sessionId, e);
        }
    }

    @Override
    public synchronized void saveTestExecution(TestExecutionRecord t) {
        String endpointsJson = truncate(DistributionJson.toJson(t.endpoints() != null ? t.endpoints() : List.of()), 4096);
        String updateSql = "UPDATE dist_test_executions SET session_id=?, partition_id=?, worker_id=?, " +
                "work_unit_key=?, class_name=?, test_name=?, status=?, duration_ms=?, attempt=?, flaky=?, " +
                "error_message=?, stack_trace=?, endpoints_json=?, recorded_at=?, unique_id=? WHERE id=?";
        String insertSql = "INSERT INTO dist_test_executions (id, session_id, partition_id, worker_id, " +
                "work_unit_key, class_name, test_name, status, duration_ms, attempt, flaky, error_message, " +
                "stack_trace, endpoints_json, recorded_at, unique_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                ps.setString(1, t.sessionId());
                ps.setString(2, t.partitionId());
                ps.setString(3, t.workerId());
                ps.setString(4, truncate(t.workUnitKey(), 512));
                ps.setString(5, truncate(t.className(), 512));
                ps.setString(6, truncate(t.testName(), 512));
                ps.setString(7, t.status().name());
                ps.setLong(8, t.durationMs());
                ps.setInt(9, t.attempt());
                ps.setBoolean(10, t.flaky());
                ps.setString(11, truncate(t.errorMessage(), 4096));
                ps.setString(12, t.stackTrace());
                ps.setString(13, endpointsJson);
                ps.setLong(14, t.recordedAt());
                ps.setString(15, truncate(t.uniqueId(), 1024));
                ps.setString(16, t.id());
                if (ps.executeUpdate() > 0) {
                    return;
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                ps.setString(1, t.id());
                ps.setString(2, t.sessionId());
                ps.setString(3, t.partitionId());
                ps.setString(4, t.workerId());
                ps.setString(5, truncate(t.workUnitKey(), 512));
                ps.setString(6, truncate(t.className(), 512));
                ps.setString(7, truncate(t.testName(), 512));
                ps.setString(8, t.status().name());
                ps.setLong(9, t.durationMs());
                ps.setInt(10, t.attempt());
                ps.setBoolean(11, t.flaky());
                ps.setString(12, truncate(t.errorMessage(), 4096));
                ps.setString(13, t.stackTrace());
                ps.setString(14, endpointsJson);
                ps.setLong(15, t.recordedAt());
                ps.setString(16, truncate(t.uniqueId(), 1024));
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to save test execution " + t.id(), e);
        }
    }

    @Override
    public List<TestExecutionRecord> listTestExecutions(String sessionId) {
        String sql = "SELECT id, session_id, partition_id, worker_id, work_unit_key, class_name, test_name, " +
                "status, duration_ms, attempt, flaky, error_message, stack_trace, endpoints_json, recorded_at, unique_id " +
                "FROM dist_test_executions WHERE session_id=? ORDER BY recorded_at ASC";
        List<TestExecutionRecord> records = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String endpointsRaw = rs.getString("endpoints_json");
                    records.add(new TestExecutionRecord(
                            rs.getString("id"),
                            rs.getString("session_id"),
                            rs.getString("partition_id"),
                            rs.getString("worker_id"),
                            rs.getString("work_unit_key"),
                            rs.getString("class_name"),
                            rs.getString("test_name"),
                            TestOutcomeStatus.valueOf(rs.getString("status")),
                            rs.getLong("duration_ms"),
                            rs.getInt("attempt"),
                            rs.getBoolean("flaky"),
                            rs.getString("error_message"),
                            rs.getString("stack_trace"),
                            endpointsRaw == null || endpointsRaw.isBlank() ? List.of() : DistributionJson.fromJson(endpointsRaw, STRING_LIST_TYPE),
                            rs.getLong("recorded_at"),
                            rs.getString("unique_id")
                    ));
                }
            }
            return records;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to list test executions for " + sessionId, e);
        }
    }

    @Override
    public Map<String, Long> findHistoricalDurationsMs(Collection<String> workUnitKeys, int recentLimit) {
        if (workUnitKeys == null || workUnitKeys.isEmpty()) {
            return Map.of();
        }
        int runLimit = Math.max(1, recentLimit);
        Map<String, Long> durations = new LinkedHashMap<>();
        // Older records can lack a session ID. Treat each of those records as an independent run.
        String sql = """
                SELECT session_id, COALESCE(attempt, 1) AS run_attempt,
                       CASE WHEN TRIM(COALESCE(session_id, '')) = '' THEN id ELSE NULL END AS legacy_id,
                       SUM(CASE WHEN duration_ms > 0 THEN CAST(duration_ms AS DECIMAL(38, 0)) ELSE 0 END) AS total_ms,
                       MAX(recorded_at) AS last_recorded_at
                FROM dist_test_executions
                WHERE work_unit_key = ? AND status IN ('PASSED', 'FAILED')
                GROUP BY session_id, COALESCE(attempt, 1),
                         CASE WHEN TRIM(COALESCE(session_id, '')) = '' THEN id ELSE NULL END
                ORDER BY last_recorded_at DESC NULLS LAST, run_attempt DESC, session_id, legacy_id
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String key : workUnitKeys) {
                ps.setString(1, key);
                Set<String> seenSessions = new HashSet<>();
                BigDecimal sum = BigDecimal.ZERO;
                int count = 0;
                try (ResultSet rs = ps.executeQuery()) {
                    while (count < runLimit && rs.next()) {
                        String sessionId = rs.getString("session_id");
                        if (sessionId != null && !sessionId.isBlank() && !seenSessions.add(sessionId)) {
                            continue;
                        }
                        sum = sum.add(rs.getBigDecimal("total_ms").max(BigDecimal.ONE));
                        count++;
                    }
                }
                if (count > 0) {
                    BigDecimal average = sum.divide(BigDecimal.valueOf(count), 0, RoundingMode.HALF_UP);
                    durations.put(key, average.min(BigDecimal.valueOf(Long.MAX_VALUE)).longValueExact());
                }
            }
            return durations;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query historical durations", e);
        }
    }

    @Override
    public List<TestHistorySummary> listTestHistory(int limit) {
        String sql = "SELECT work_unit_key, class_name, test_name, " +
                "AVG(duration_ms) AS avg_ms, MAX(duration_ms) AS max_ms, COUNT(*) AS total_runs, " +
                "SUM(CASE WHEN status='PASSED' THEN 1 ELSE 0 END) AS passed_runs, " +
                "SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_runs, " +
                "SUM(CASE WHEN flaky=TRUE THEN 1 ELSE 0 END) AS flaky_runs " +
                "FROM dist_test_executions GROUP BY work_unit_key, class_name, test_name ORDER BY avg_ms DESC";
        // Latest outcome of every test in one ordered pass (a per-test lookup scanned the whole table per row).
        // Latest outcome per test: the rows at each test's newest recorded_at (one or a few per test), not the table.
        String latestStatusSql = "SELECT e.work_unit_key, e.class_name, e.test_name, e.status, e.attempt, e.id " +
                "FROM dist_test_executions e JOIN (SELECT work_unit_key, class_name, test_name, MAX(recorded_at) AS last_at " +
                "FROM dist_test_executions GROUP BY work_unit_key, class_name, test_name) m " +
                "ON COALESCE(e.work_unit_key, '') = COALESCE(m.work_unit_key, '') " +
                "AND COALESCE(e.class_name, '') = COALESCE(m.class_name, '') " +
                "AND COALESCE(e.test_name, '') = COALESCE(m.test_name, '') AND e.recorded_at = m.last_at " +
                "ORDER BY e.attempt, e.id";
        List<TestHistorySummary> history = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             PreparedStatement latestPs = conn.prepareStatement(latestStatusSql)) {
            ps.setMaxRows(Math.max(1, limit));
            List<String[]> keys = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String key = rs.getString("work_unit_key");
                    String className = rs.getString("class_name");
                    String testName = rs.getString("test_name");
                    keys.add(new String[] {key, className, testName});
                    history.add(new TestHistorySummary(
                            key,
                            className,
                            testName,
                            rs.getLong("avg_ms"),
                            rs.getLong("max_ms"),
                            rs.getInt("total_runs"),
                            rs.getInt("passed_runs"),
                            rs.getInt("failed_runs"),
                            rs.getInt("flaky_runs"),
                            TestOutcomeStatus.PASSED
                    ));
                }
            }
            Map<String, TestOutcomeStatus> latest = new java.util.HashMap<>();
            java.util.Set<String> wanted = new java.util.HashSet<>();
            for (String[] k : keys) {
                wanted.add(historyKey(k[0], k[1], k[2]));
            }
            latestPs.setFetchSize(2_000);
            try (ResultSet lrs = latestPs.executeQuery()) {
                while (lrs.next()) {
                    String k = historyKey(lrs.getString(1), lrs.getString(2), lrs.getString(3));
                    String rawStatus = lrs.getString(4);
                    if (wanted.contains(k) && rawStatus != null && !rawStatus.isBlank()) {
                        latest.put(k, TestOutcomeStatus.valueOf(rawStatus));
                    }
                }
            }
            for (int i = 0; i < history.size(); i++) {
                TestHistorySummary h = history.get(i);
                TestOutcomeStatus status = latest.get(historyKey(h.workUnitKey(), h.className(), h.testName()));
                if (status != null && status != h.lastStatus()) {
                    history.set(i, new TestHistorySummary(h.workUnitKey(), h.className(), h.testName(),
                            h.avgDurationMs(), h.maxDurationMs(), h.totalRuns(), h.passedRuns(), h.failedRuns(),
                            h.flakyRuns(), status));
                }
            }
            return history;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query test history", e);
        }
    }

    @Override
    public synchronized void close() {
        if (jdbcUrl != null && jdbcUrl.toLowerCase(Locale.ROOT).startsWith("jdbc:hsqldb:file:")) {
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute("SHUTDOWN");
            } catch (SQLException e) {
                LOG.debug("HSQLDB shutdown notice: {}", e.getMessage());
            }
        }
        try {
            dataSource.close();
        } catch (SQLException e) {
            LOG.debug("Error closing datasource: {}", e.getMessage());
        }
    }

    private static String historyKey(String workUnitKey, String className, String testName) {
        return (workUnitKey != null ? workUnitKey : "") + '\u0000' + (className != null ? className : "") + '\u0000'
                + (testName != null ? testName : "");
    }

    /**
     * Modules that submitted partitions to each session, in the order they first started (Maven reactor order;
     * modules none of whose partitions started yet come last). One query per chunk of sessions.
     */
    private static Map<String, List<String>> findModules(Connection conn, List<String> sessionIds) throws SQLException {
        Map<String, List<String>> modules = new java.util.HashMap<>();
        int chunk = 200;
        for (int from = 0; from < sessionIds.size(); from += chunk) {
            List<String> ids = sessionIds.subList(from, Math.min(sessionIds.size(), from + chunk));
            String sql = "SELECT session_id, module_name, MIN(CASE WHEN started_at > 0 THEN started_at END) AS first_start "
                    + "FROM dist_partitions WHERE module_name IS NOT NULL AND session_id IN ("
                    + String.join(",", java.util.Collections.nCopies(ids.size(), "?")) + ") "
                    + "GROUP BY session_id, module_name";
            Map<String, List<Object[]>> rows = new java.util.HashMap<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 0; i < ids.size(); i++) {
                    ps.setString(i + 1, ids.get(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long firstStart = rs.getLong("first_start");
                        rows.computeIfAbsent(rs.getString("session_id"), k -> new ArrayList<>()).add(new Object[] {
                                rs.getString("module_name"), rs.wasNull() ? Long.MAX_VALUE : firstStart});
                    }
                }
            }
            rows.forEach((session, list) -> modules.put(session, list.stream()
                    .sorted(java.util.Comparator.<Object[]>comparingLong(r -> (Long) r[1]).thenComparing(r -> (String) r[0]))
                    .map(r -> (String) r[0])
                    .toList()));
        }
        return modules;
    }

    private static BuildSessionRecord mapSession(ResultSet rs) throws SQLException {
        return new BuildSessionRecord(
                rs.getString("session_id"),
                rs.getString("project_name"),
                rs.getString("git_branch"),
                rs.getString("git_commit"),
                SessionStatus.valueOf(rs.getString("status")),
                rs.getLong("started_at"),
                rs.getLong("finished_at"),
                rs.getInt("total_tests"),
                rs.getInt("passed_tests"),
                rs.getInt("failed_tests"),
                rs.getInt("skipped_tests"),
                rs.getInt("total_blobs"),
                rs.getInt("cached_blobs"),
                rs.getLong("serial_duration_ms"),
                rs.getLong("wall_duration_ms"),
                rs.getString("failure_reason")
        );
    }

    private static String truncate(String s, int maxLen) {
        if (s == null || s.length() <= maxLen) {
            return s;
        }
        return s.substring(0, maxLen);
    }
}

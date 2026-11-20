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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

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
import org.citrusframework.distribution.model.WorkerRecord;
import org.citrusframework.distribution.model.WorkerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
                        "wall_duration_ms BIGINT)",
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
                        "registered_at BIGINT)",
                "CREATE TABLE IF NOT EXISTS dist_partitions (" +
                        "partition_id VARCHAR(64) PRIMARY KEY, " +
                        "session_id VARCHAR(64), " +
                        "worker_id VARCHAR(128), " +
                        "state VARCHAR(32), " +
                        "attempt INT, " +
                        "estimated_duration_ms BIGINT, " +
                        "started_at BIGINT, " +
                        "finished_at BIGINT, " +
                        "required_caps_json VARCHAR(4096), " +
                        "work_units_json " + longText + ")",
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
                        "recorded_at BIGINT)"
        );

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            for (String sql : ddl) {
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize distribution schema on " + jdbcUrl, e);
        }
    }

    @Override
    public synchronized void saveSession(BuildSessionRecord s) {
        String updateSql = "UPDATE dist_sessions SET project_name=?, git_branch=?, git_commit=?, status=?, " +
                "started_at=?, finished_at=?, total_tests=?, passed_tests=?, failed_tests=?, skipped_tests=?, " +
                "total_blobs=?, cached_blobs=?, serial_duration_ms=?, wall_duration_ms=? WHERE session_id=?";
        String insertSql = "INSERT INTO dist_sessions (session_id, project_name, git_branch, git_commit, status, " +
                "started_at, finished_at, total_tests, passed_tests, failed_tests, skipped_tests, " +
                "total_blobs, cached_blobs, serial_duration_ms, wall_duration_ms) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
                ps.setString(15, s.sessionId());
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
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to save build session " + s.sessionId(), e);
        }
    }

    @Override
    public synchronized void updateSessionStatus(String sessionId, SessionStatus status, long finishedAt, SessionSummary summary) {
        String sql = "UPDATE dist_sessions SET status=?, finished_at=?, total_tests=?, passed_tests=?, " +
                "failed_tests=?, skipped_tests=?, total_blobs=?, cached_blobs=?, serial_duration_ms=?, wall_duration_ms=? " +
                "WHERE session_id=?";
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
            ps.setString(11, sessionId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to update session status for " + sessionId, e);
        }
    }

    @Override
    public Optional<BuildSessionRecord> findSession(String sessionId) {
        String sql = "SELECT session_id, project_name, git_branch, git_commit, status, started_at, finished_at, " +
                "total_tests, passed_tests, failed_tests, skipped_tests, total_blobs, cached_blobs, " +
                "serial_duration_ms, wall_duration_ms FROM dist_sessions WHERE session_id=?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapSession(rs));
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
                "serial_duration_ms, wall_duration_ms FROM dist_sessions ORDER BY started_at DESC";
        List<BuildSessionRecord> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setMaxRows(Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(mapSession(rs));
                }
            }
            return result;
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
                "last_heartbeat_at=?, registered_at=? WHERE worker_id=?";
        String insertSql = "INSERT INTO dist_workers (worker_id, host, capabilities_json, state, active_partition_id, " +
                "last_heartbeat_at, registered_at) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                ps.setString(1, w.host());
                ps.setString(2, capsJson);
                ps.setString(3, w.state().name());
                ps.setString(4, w.activePartitionId());
                ps.setLong(5, w.lastHeartbeatAt());
                ps.setLong(6, w.registeredAt());
                ps.setString(7, w.workerId());
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
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to upsert worker " + w.workerId(), e);
        }
    }

    @Override
    public List<WorkerRecord> listWorkers() {
        String sql = "SELECT worker_id, host, capabilities_json, state, active_partition_id, " +
                "last_heartbeat_at, registered_at FROM dist_workers ORDER BY registered_at ASC";
        List<WorkerRecord> workers = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String capsRaw = rs.getString("capabilities_json");
                Map<String, String> caps = capsRaw == null || capsRaw.isBlank()
                        ? Map.of()
                        : DistributionJson.fromJson(capsRaw, STRING_MAP_TYPE);
                workers.add(new WorkerRecord(
                        rs.getString("worker_id"),
                        rs.getString("host"),
                        caps,
                        WorkerState.valueOf(rs.getString("state")),
                        rs.getString("active_partition_id"),
                        rs.getLong("last_heartbeat_at"),
                        rs.getLong("registered_at")
                ));
            }
            return workers;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to list workers", e);
        }
    }

    @Override
    public synchronized void savePartition(PartitionRecord p) {
        String capsJson = truncate(DistributionJson.toJson(p.requiredCapabilities() != null ? p.requiredCapabilities() : Map.of()), 4096);
        String unitsJson = DistributionJson.toJson(p.workUnits() != null ? p.workUnits() : List.of());
        String updateSql = "UPDATE dist_partitions SET session_id=?, worker_id=?, state=?, attempt=?, " +
                "estimated_duration_ms=?, started_at=?, finished_at=?, required_caps_json=?, work_units_json=? " +
                "WHERE partition_id=?";
        String insertSql = "INSERT INTO dist_partitions (partition_id, session_id, worker_id, state, attempt, " +
                "estimated_duration_ms, started_at, finished_at, required_caps_json, work_units_json) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                ps.setString(1, p.sessionId());
                ps.setString(2, p.workerId());
                ps.setString(3, p.state().name());
                ps.setInt(4, p.attempt());
                ps.setLong(5, p.estimatedDurationMs());
                ps.setLong(6, p.startedAt());
                ps.setLong(7, p.finishedAt());
                ps.setString(8, capsJson);
                ps.setString(9, unitsJson);
                ps.setString(10, p.partitionId());
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
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to save partition " + p.partitionId(), e);
        }
    }

    @Override
    public List<PartitionRecord> listPartitions(String sessionId) {
        String sql = "SELECT partition_id, session_id, worker_id, state, attempt, estimated_duration_ms, " +
                "started_at, finished_at, required_caps_json, work_units_json FROM dist_partitions " +
                "WHERE session_id=? ORDER BY started_at ASC, partition_id ASC";
        List<PartitionRecord> partitions = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String capsRaw = rs.getString("required_caps_json");
                    String unitsRaw = rs.getString("work_units_json");
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
                            unitsRaw == null || unitsRaw.isBlank() ? List.of() : DistributionJson.fromJson(unitsRaw, WORK_UNITS_TYPE)
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
                "error_message=?, stack_trace=?, endpoints_json=?, recorded_at=? WHERE id=?";
        String insertSql = "INSERT INTO dist_test_executions (id, session_id, partition_id, worker_id, " +
                "work_unit_key, class_name, test_name, status, duration_ms, attempt, flaky, error_message, " +
                "stack_trace, endpoints_json, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
                ps.setString(15, t.id());
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
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to save test execution " + t.id(), e);
        }
    }

    @Override
    public List<TestExecutionRecord> listTestExecutions(String sessionId) {
        String sql = "SELECT id, session_id, partition_id, worker_id, work_unit_key, class_name, test_name, " +
                "status, duration_ms, attempt, flaky, error_message, stack_trace, endpoints_json, recorded_at " +
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
                            rs.getLong("recorded_at")
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
        Map<String, Long> durations = new LinkedHashMap<>();
        String sql = "SELECT duration_ms FROM dist_test_executions WHERE work_unit_key=? AND status IN ('PASSED','FAILED') " +
                "ORDER BY recorded_at DESC";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setMaxRows(Math.max(1, recentLimit));
            for (String key : workUnitKeys) {
                ps.setString(1, key);
                long sum = 0L;
                int count = 0;
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        sum += Math.max(1L, rs.getLong(1));
                        count++;
                    }
                }
                if (count > 0) {
                    durations.put(key, sum / count);
                }
            }
            return durations;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query historical durations", e);
        }
    }

    @Override
    public List<TestHistorySummary> listTestHistory(int limit) {
        String sql = "SELECT work_unit_key, MAX(class_name) AS class_name, MAX(test_name) AS test_name, " +
                "AVG(duration_ms) AS avg_ms, COUNT(*) AS total_runs, " +
                "SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_runs, " +
                "SUM(CASE WHEN flaky=TRUE THEN 1 ELSE 0 END) AS flaky_runs " +
                "FROM dist_test_executions GROUP BY work_unit_key ORDER BY avg_ms DESC";
        List<TestHistorySummary> history = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setMaxRows(Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    history.add(new TestHistorySummary(
                            rs.getString("work_unit_key"),
                            rs.getString("class_name"),
                            rs.getString("test_name"),
                            rs.getLong("avg_ms"),
                            rs.getInt("total_runs"),
                            rs.getInt("failed_runs"),
                            rs.getInt("flaky_runs")
                    ));
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
                rs.getLong("wall_duration_ms")
        );
    }

    private static String truncate(String s, int maxLen) {
        if (s == null || s.length() <= maxLen) {
            return s;
        }
        return s.substring(0, maxLen);
    }
}

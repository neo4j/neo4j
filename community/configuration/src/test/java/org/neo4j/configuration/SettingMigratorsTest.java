/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.BootloaderSettings.lib_directory;
import static org.neo4j.configuration.BootloaderSettings.run_directory;
import static org.neo4j.configuration.BootloaderSettings.windows_service_name;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.ON_HEAP;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionTracingLevel.SAMPLE;
import static org.neo4j.configuration.GraphDatabaseSettings.bookmark_ready_timeout;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_time;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_tx;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_volume;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_iops_limit;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_policy;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_hints_error;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_lenient_create_relationship;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_min_replan_interval;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_parser_version;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_planner;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_render_plan_descriptions;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.database_dumps_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.forbid_exhaustive_shortestpath;
import static org.neo4j.configuration.GraphDatabaseSettings.forbid_shortestpath_common_nodes;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.licenses_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.load_csv_file_url_root;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.max_concurrent_transactions;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_database_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_prefetch_allowlist;
import static org.neo4j.configuration.GraphDatabaseSettings.plugin_dir;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_allowlist;
import static org.neo4j.configuration.GraphDatabaseSettings.query_statistics_divergence_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.script_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.track_query_allocation;
import static org.neo4j.configuration.GraphDatabaseSettings.track_query_cpu_time;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_log_buffer_size;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_monitor_check_interval;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_sampling_percentage;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_tracing_level;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_max_off_heap_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_memory_allocation;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_block_cache_size;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_max_cacheable_block_size;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.connectors.BoltConnectorInternalSettings.thread_pool_shutdown_wait_time;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.io.ByteUnit;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.FormattedLogFormat;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class SettingMigratorsTest {
    @Inject
    private TestDirectory testDirectory;

    @Test
    void testConnectorOldFormatMigration() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                Arrays.asList(
                        "dbms.connector.bolt.unsupported_thread_pool_shutdown_wait_time=1s",
                        "dbms.connector.bolt.enabled=true",
                        "dbms.connector.bolt.type=BOLT",
                        "dbms.connector.http.enabled=true",
                        "dbms.connector.https.enabled=true"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertTrue(config.get(BoltConnector.enabled));
        assertTrue(config.get(HttpConnector.enabled));
        assertTrue(config.get(HttpsConnector.enabled));
        assertEquals(Duration.ofSeconds(1), config.get(thread_pool_shutdown_wait_time));

        var warnConfigMatcher = assertThat(logProvider).forClass(Config.class).forLevel(WARN);
        warnConfigMatcher
                .containsMessages(
                        "Use of deprecated setting 'dbms.connector.http.enabled'. It is replaced by 'server.http.enabled'.")
                .containsMessages(
                        "Use of deprecated setting 'dbms.connector.https.enabled'. It is replaced by 'server.https.enabled'.");
    }

    @Test
    void warnOnLegacyUnsupportedSettingUsage() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("unsupported.tools.batch_inserter.batch_size=42"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessages("Use of deprecated setting 'unsupported.tools.batch_inserter.batch_size'. "
                        + "It is replaced by 'internal.tools.batch_inserter.batch_size'.");
    }

    @Test
    void testMemorySettingsRename() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "dbms.tx_state.max_off_heap_memory=6g",
                        "dbms.tx_state.off_heap.max_cacheable_block_size=4096",
                        "dbms.tx_state.off_heap.block_cache_size=256"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessageWithArguments(
                        "Use of deprecated setting '%s'. It is replaced by '%s'.",
                        "dbms.tx_state.max_off_heap_memory", tx_state_max_off_heap_memory.name())
                .containsMessageWithArguments(
                        "Use of deprecated setting '%s'. It is replaced by '%s'.",
                        "dbms.tx_state.off_heap.max_cacheable_block_size",
                        tx_state_off_heap_max_cacheable_block_size.name())
                .containsMessageWithArguments(
                        "Use of deprecated setting '%s'. It is replaced by '%s'.",
                        "dbms.tx_state.off_heap.block_cache_size", tx_state_off_heap_block_cache_size.name());

        assertEquals(BYTES.parse("6g"), config.get(tx_state_max_off_heap_memory));
        assertEquals(4096, config.get(tx_state_off_heap_max_cacheable_block_size));
        assertEquals(256, config.get(tx_state_off_heap_block_cache_size));
    }

    @Test
    void transactionCypherMaxAllocations() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("cypher.query_max_allocations=6g"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessageWithArguments(
                        "The setting cypher.query_max_allocations is removed and replaced by %s.",
                        memory_transaction_max_size.name());
        assertEquals(BYTES.parse("6g"), config.get(memory_transaction_max_size));
    }

    @Test
    void transactionCypherMaxAllocationsConflict() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("cypher.query_max_allocations=6g", memory_transaction_max_size.name() + "=7g"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessageWithArguments(
                        "The setting cypher.query_max_allocations is removed and replaced by %s. Since both are set, %s will take "
                                + "precedence and the value of cypher.query_max_allocations, %s, will be ignored.",
                        memory_transaction_max_size.name(), memory_transaction_max_size.name(), "6g");
        assertEquals(BYTES.parse("7g"), config.get(memory_transaction_max_size));
    }

    @Test
    void testWhitelistRename() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of("dbms.memory.pagecache.warmup.preload.whitelist=a", "dbms.security.procedures.whitelist=a,b"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessageWithArguments(
                        "Use of deprecated setting '%s'. It is replaced by '%s'.",
                        "dbms.memory.pagecache.warmup.preload.whitelist", pagecache_warmup_prefetch_allowlist.name())
                .containsMessageWithArguments(
                        "Use of deprecated setting '%s'. It is replaced by '%s'.",
                        "dbms.security.procedures.whitelist", procedure_allowlist.name());

        assertEquals("a", config.get(pagecache_warmup_prefetch_allowlist));
        assertEquals(List.of("a", "b"), config.get(procedure_allowlist));
    }

    @Test
    void testDatababaseRename() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.memory.transaction.datababase_max_size=1g"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessageWithArguments(
                        "Use of deprecated setting '%s'. It is replaced by '%s'.",
                        "dbms.memory.transaction.datababase_max_size", memory_transaction_database_max_size.name());

        assertEquals(1073741824L, config.get(memory_transaction_database_max_size));
    }

    @Test
    void windowsServiceNameMigration() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.windows_service_name=foo-bar"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertEquals("foo-bar", config.get(windows_service_name));
    }

    @Test
    void checkpointSettingsMigration() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "dbms.checkpoint=PERIODIC",
                        "dbms.checkpoint.interval.time=10m",
                        "dbms.checkpoint.interval.tx=17",
                        "dbms.checkpoint.interval.volume=125m",
                        "dbms.checkpoint.iops.limit=456"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertEquals(GraphDatabaseSettings.CheckpointPolicy.PERIODIC, config.get(check_point_policy));
        assertEquals(Duration.ofMinutes(10), config.get(check_point_interval_time));
        assertEquals(17, config.get(check_point_interval_tx));
        assertEquals(ByteUnit.mebiBytes(125), config.get(check_point_interval_volume));
        assertEquals(456, config.get(check_point_iops_limit));
    }

    @Test
    void directoriesSettingsMigration() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "dbms.directories.neo4j_home=/a",
                        "dbms.directories.data=/b",
                        "dbms.directories.transaction.logs.root=/c",
                        "dbms.directories.script.root=/d",
                        "dbms.directories.dumps.root=/e",
                        "dbms.directories.import=/f",
                        "dbms.directories.plugins=/g",
                        "dbms.directories.logs=/h",
                        "dbms.directories.licenses=/i",
                        "dbms.directories.run=/j",
                        "dbms.directories.lib=/k"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertEquals("/a", config.get(neo4j_home).toString());
        assertEquals("/b", config.get(data_directory).toString());
        assertEquals("/c", config.get(transaction_logs_root_path).toString());
        assertEquals("/d", config.get(script_root_path).toString());
        assertEquals("/e", config.get(database_dumps_root_path).toString());
        assertEquals("/f", config.get(load_csv_file_url_root).toString());
        assertEquals("/g", config.get(plugin_dir).toString());
        assertEquals("/h", config.get(logs_directory).toString());
        assertEquals("/i", config.get(licenses_directory).toString());

        assertEquals("/j", config.get(run_directory).toString());
        assertEquals("/k", config.get(lib_directory).toString());
    }

    @Test
    void refuseToBeLeaderShouldBeMigrated() throws IOException {
        // given
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("causal_clustering.refuse_to_be_leader=true"));

        // when
        Config config = Config.newBuilder().fromFile(confFile).build();

        // then
        assertThat(config.get(read_only_database_default)).isTrue();
        assertThat(Config.defaults().get(read_only_database_default)).isFalse();
    }

    @Test
    void readOnlySettingMigration() throws IOException {
        var configuration = testDirectory.createFile("test.conf");
        Files.write(configuration, List.of("dbms.read_only=true"));

        var logProvider = new AssertableLogProvider();
        var config = Config.newBuilder().fromFile(configuration).build();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(config.get(read_only_database_default)).isTrue();
        assertThat(Config.defaults().get(read_only_database_default)).isFalse();
        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessages(
                        "Use of deprecated setting 'dbms.read_only'. It is replaced by 'dbms.databases.default_to_read_only'");
    }

    @Test
    void removedSettingMigration() throws IOException {
        var configuration = testDirectory.createFile("test.conf");
        Files.write(configuration, List.of("causal_clustering.multi_dc_license=foo"));

        var logProvider = new AssertableLogProvider();
        var config = Config.newBuilder().fromFile(configuration).build();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessages(
                        "Setting 'causal_clustering.multi_dc_license' is removed. It no longer has any effect.");
    }

    @Test
    void logFormatMigrator() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("unsupported.dbms.logs.format=JSON_FORMAT"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        AssertableLogProvider logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));
        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessages("Use of deprecated setting 'unsupported.dbms.logs.format'");
        assertThat(config.get(GraphDatabaseSettings.store_internal_log_format)).isEqualTo(FormattedLogFormat.JSON);
        assertThat(config.get(GraphDatabaseSettings.store_user_log_format)).isEqualTo(FormattedLogFormat.JSON);
        assertThat(config.get(GraphDatabaseSettings.log_query_format)).isEqualTo(FormattedLogFormat.JSON);

        Files.write(confFile, List.of("unsupported.dbms.logs.format=FOO"));
        config = Config.newBuilder().fromFile(confFile).build();
        logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));
        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessages("Unrecognized value for 'unsupported.dbms.logs.format'. Was FOO");
    }

    @Test
    void migrateCypherSettingsIntoDbmsNamespace() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "cypher.default_language_version=4.4",
                        "cypher.forbid_exhaustive_shortestpath=true",
                        "cypher.forbid_shortestpath_common_nodes=false",
                        "cypher.hints_error=true",
                        "cypher.lenient_create_relationship=false",
                        "cypher.min_replan_interval=11s",
                        "cypher.planner=COST",
                        "cypher.render_plan_description=true",
                        " cypher.statistics_divergence_threshold=0.42"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertEquals(GraphDatabaseSettings.CypherParserVersion.V_44, config.get(cypher_parser_version));
        assertEquals(true, config.get(forbid_exhaustive_shortestpath));
        assertEquals(false, config.get(forbid_shortestpath_common_nodes));
        assertEquals(true, config.get(cypher_hints_error));
        assertEquals(false, config.get(cypher_lenient_create_relationship));
        assertEquals(Duration.ofSeconds(11), config.get(cypher_min_replan_interval));
        assertEquals(GraphDatabaseSettings.CypherPlanner.COST, config.get(cypher_planner));
        assertEquals(true, config.get(cypher_render_plan_descriptions));
        assertEquals(0.42, config.get(query_statistics_divergence_threshold), 0.01);
    }

    @Test
    void migrateTxLogsAndStateSettings() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "dbms.tx_log.buffer.size=134072",
                        "dbms.tx_log.preallocate=false",
                        "dbms.tx_log.rotation.retention_policy=3 days",
                        "dbms.tx_log.rotation.size=34mb",
                        "dbms.tx_state.memory_allocation=ON_HEAP"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertEquals(ByteUnit.bytes(134072), config.get(transaction_log_buffer_size));
        assertFalse(config.get(preallocate_logical_logs));
        assertEquals("3 days", config.get(keep_logical_logs));
        assertEquals(ByteUnit.mebiBytes(34), config.get(logical_log_rotation_threshold));
        assertEquals(ON_HEAP, config.get(tx_state_memory_allocation));
    }

    @Test
    void migrateTransactionAndMonitoringSettings() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "dbms.track_query_allocation=false",
                        "dbms.track_query_cpu_time=true",
                        "dbms.transaction.bookmark_ready_timeout=100s",
                        "dbms.transaction.concurrent.maximum=17",
                        "dbms.transaction.monitor.check.interval=4s",
                        "dbms.transaction.sampling.percentage=78",
                        "dbms.transaction.timeout=10s",
                        "dbms.transaction.tracing.level=SAMPLE"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertFalse(config.get(track_query_allocation));
        assertTrue(config.get(track_query_cpu_time));
        assertEquals(Duration.ofSeconds(100), config.get(bookmark_ready_timeout));
        assertEquals(17, config.get(max_concurrent_transactions));
        assertEquals(Duration.ofSeconds(4), config.get(transaction_monitor_check_interval));
        assertEquals(78, config.get(transaction_sampling_percentage));
        assertEquals(Duration.ofSeconds(10), config.get(transaction_timeout));
        assertEquals(SAMPLE, config.get(transaction_tracing_level));
    }
}

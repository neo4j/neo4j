/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.configuration;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.BootloaderSettings.additional_jvm;
import static org.neo4j.configuration.BootloaderSettings.gc_logging_enabled;
import static org.neo4j.configuration.BootloaderSettings.gc_logging_options;
import static org.neo4j.configuration.BootloaderSettings.gc_logging_rotation_keep_number;
import static org.neo4j.configuration.BootloaderSettings.gc_logging_rotation_size;
import static org.neo4j.configuration.BootloaderSettings.initial_heap_size;
import static org.neo4j.configuration.BootloaderSettings.lib_directory;
import static org.neo4j.configuration.BootloaderSettings.max_heap_size;
import static org.neo4j.configuration.BootloaderSettings.run_directory;
import static org.neo4j.configuration.BootloaderSettings.windows_service_name;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.upgrade_processors;
import static org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel.INFO;
import static org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel.VERBOSE;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.ON_HEAP;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionTracingLevel.SAMPLE;
import static org.neo4j.configuration.GraphDatabaseSettings.bookmark_ready_timeout;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_time;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_tx;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_volume;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_iops_limit;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_policy;
import static org.neo4j.configuration.GraphDatabaseSettings.csv_buffer_size;
import static org.neo4j.configuration.GraphDatabaseSettings.csv_legacy_quote_escaping;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_hints_error;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_lenient_create_relationship;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_min_replan_interval;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_planner;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_render_plan_descriptions;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.database_dumps_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.configuration.GraphDatabaseSettings.default_listen_address;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files;
import static org.neo4j.configuration.GraphDatabaseSettings.filewatcher_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.forbid_exhaustive_shortestpath;
import static org.neo4j.configuration.GraphDatabaseSettings.forbid_shortestpath_common_nodes;
import static org.neo4j.configuration.GraphDatabaseSettings.index_background_sampling_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.index_sample_size_limit;
import static org.neo4j.configuration.GraphDatabaseSettings.index_sampling_update_percentage;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.licenses_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.load_csv_file_url_root;
import static org.neo4j.configuration.GraphDatabaseSettings.lock_acquisition_timeout;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_annotation_data_format;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_early_raw_logging_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_obfuscate_literals;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_parameter_logging_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_query_plan;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_transaction_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.log_queries_transactions_level;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.max_concurrent_transactions;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_database_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_global_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_buffered_flush_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_direct_io;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_flush_buffer_size_in_pages;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_scan_prefetch;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_prefetch;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_prefetch_allowlist;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_profiling_interval;
import static org.neo4j.configuration.GraphDatabaseSettings.plugin_dir;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_store_files;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_allowlist;
import static org.neo4j.configuration.GraphDatabaseSettings.query_cache_size;
import static org.neo4j.configuration.GraphDatabaseSettings.query_log_max_parameter_length;
import static org.neo4j.configuration.GraphDatabaseSettings.query_statistics_divergence_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.script_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.shutdown_transaction_end_timeout;
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
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.fs.FileSystemUtils.pathToString;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings.AnnotationDataFormat;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.ByteUnit;
import org.neo4j.logging.AssertableLogProvider;
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
        assertEquals(ofSeconds(1), config.get(thread_pool_shutdown_wait_time));

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
    void testLegacyOffHeapMemorySettingsRename() throws IOException {
        final var legacySetting = "server.memory.off_heap.max_size";

        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of(legacySetting + "=6g"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessageWithArguments(
                        "Use of deprecated setting '%s'. It is replaced by '%s'.",
                        legacySetting, tx_state_max_off_heap_memory.name());

        assertEquals(BYTES.parse("6g"), config.get(tx_state_max_off_heap_memory));
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
        assertEquals(mebiBytes(125), config.get(check_point_interval_volume));
        assertEquals(456, config.get(check_point_iops_limit));
    }

    @Test
    void directoriesSettingsMigration() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        var a = testDirectory.directory("a").toAbsolutePath();
        var b = testDirectory.directory("b").toAbsolutePath();
        var c = testDirectory.directory("c").toAbsolutePath();
        var d = testDirectory.directory("d").toAbsolutePath();
        var e = testDirectory.directory("e").toAbsolutePath();
        var f = testDirectory.directory("f").toAbsolutePath();
        var g = testDirectory.directory("g").toAbsolutePath();
        var h = testDirectory.directory("h").toAbsolutePath();
        var i = testDirectory.directory("i").toAbsolutePath();
        var j = testDirectory.directory("j").toAbsolutePath();
        var k = testDirectory.directory("k").toAbsolutePath();

        Files.write(
                confFile,
                List.of(
                        "dbms.directories.neo4j_home=" + pathToString(a),
                        "dbms.directories.data=" + pathToString(b),
                        "dbms.directories.transaction.logs.root=" + pathToString(c),
                        "dbms.directories.script.root=" + pathToString(d),
                        "dbms.directories.dumps.root=" + pathToString(e),
                        "dbms.directories.import=" + pathToString(f),
                        "dbms.directories.plugins=" + pathToString(g),
                        "dbms.directories.logs=" + pathToString(h),
                        "dbms.directories.licenses=" + pathToString(i),
                        "dbms.directories.run=" + pathToString(j),
                        "dbms.directories.lib=" + pathToString(k)));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertEquals(a, config.get(neo4j_home));
        assertEquals(b, config.get(data_directory));
        assertEquals(c, config.get(transaction_logs_root_path));
        assertEquals(d, config.get(script_root_path));
        assertEquals(e, config.get(database_dumps_root_path));
        assertEquals(f, config.get(load_csv_file_url_root));
        assertEquals(g, config.get(plugin_dir));
        assertEquals(h, config.get(logs_directory));
        assertEquals(i, config.get(licenses_directory));

        assertEquals(j, config.get(run_directory));
        assertEquals(k, config.get(lib_directory));
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
                .containsMessages(String.format(
                        "Use of deprecated setting 'dbms.read_only'. It is replaced by '%s'",
                        read_only_database_default.name()));
    }

    @Test
    void removedSettingMigration() throws IOException {
        var configuration = testDirectory.createFile("test.conf");
        Files.write(configuration, List.of("dbms.allow_upgrade=false"));

        var logProvider = new AssertableLogProvider();
        var config = Config.newBuilder().fromFile(configuration).build();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessages("Setting 'dbms.allow_upgrade' is removed. It no longer has any effect.");
    }

    @Test
    void migrateCypherSettingsIntoDbmsNamespace() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "cypher.forbid_exhaustive_shortestpath=true",
                        "cypher.forbid_shortestpath_common_nodes=false",
                        "cypher.hints_error=true",
                        "cypher.lenient_create_relationship=false",
                        "cypher.min_replan_interval=11s",
                        "cypher.planner=COST",
                        "cypher.render_plan_description=true",
                        "cypher.statistics_divergence_threshold=0.42"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertEquals(true, config.get(forbid_exhaustive_shortestpath));
        assertEquals(false, config.get(forbid_shortestpath_common_nodes));
        assertEquals(true, config.get(cypher_hints_error));
        assertEquals(false, config.get(cypher_lenient_create_relationship));
        assertEquals(ofSeconds(11), config.get(cypher_min_replan_interval));
        assertEquals(GraphDatabaseSettings.CypherPlanner.COST, config.get(cypher_planner));
        assertEquals(true, config.get(cypher_render_plan_descriptions));
        assertEquals(0.42, config.get(query_statistics_divergence_threshold), 0.01);
    }

    @Test
    void migrateCypherQueryCacheSizeSetting() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("server.db.query_cache_size=100"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertEquals(100, config.get(query_cache_size));
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
        assertEquals(mebiBytes(34), config.get(logical_log_rotation_threshold));
        assertEquals(ON_HEAP, config.get(tx_state_memory_allocation));
    }

    @Test
    void migrateWatcherSetting() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.filewatcher.enabled=false"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertFalse(config.get(filewatcher_enabled));
    }

    @Test
    void migrateLockAcquisitionSetting() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.lock.acquisition.timeout=15m"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertEquals(Duration.ofMinutes(15), config.get(lock_acquisition_timeout));
    }

    @Test
    void migrateCsvImportSetting() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile, List.of("dbms.import.csv.buffer_size=123", "dbms.import.csv.legacy_quote_escaping=false"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertEquals(123, config.get(csv_buffer_size));
        assertFalse(config.get(csv_legacy_quote_escaping));
    }

    @Test
    void migrateTransactionAndMonitoringSettings() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
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

        assertTrue(config.get(track_query_cpu_time));
        assertEquals(ofSeconds(100), config.get(bookmark_ready_timeout));
        assertEquals(17, config.get(max_concurrent_transactions));
        assertEquals(ofSeconds(4), config.get(transaction_monitor_check_interval));
        assertEquals(78, config.get(transaction_sampling_percentage));
        assertEquals(ofSeconds(10), config.get(transaction_timeout));
        assertEquals(SAMPLE, config.get(transaction_tracing_level));
    }

    @Test
    void migrateGcLogsSettings() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "dbms.logs.gc.enabled=true",
                        "dbms.logs.gc.options=niceOptions",
                        "dbms.logs.gc.rotation.keep_number=7",
                        "dbms.logs.gc.rotation.size=5m"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertTrue(config.get(gc_logging_enabled));
        assertEquals("niceOptions", config.get(gc_logging_options));
        assertEquals(7, config.get(gc_logging_rotation_keep_number));
        assertEquals(ByteUnit.mebiBytes(5), config.get(gc_logging_rotation_size));
    }

    @Test
    void migrateProcessorNumberToInternalNamespace() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.upgrade_max_processors=7"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        assertEquals(7, config.get(upgrade_processors));
    }

    @Test
    void migratePageCacheWarmupSettings() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "dbms.memory.pagecache.warmup.enable=true",
                        "dbms.memory.pagecache.warmup.preload=false",
                        "dbms.memory.pagecache.warmup.preload.allowlist=*index*",
                        "dbms.memory.pagecache.warmup.profile.interval=5s"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertTrue(config.get(pagecache_warmup_enabled));
        assertFalse(config.get(pagecache_warmup_prefetch));
        assertEquals("*index*", config.get(pagecache_warmup_prefetch_allowlist));
        assertEquals(ofSeconds(5), config.get(pagecache_warmup_profiling_interval));
    }

    @Test
    void migrateShutdownTimeoutAndPreallocations() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile, List.of("dbms.shutdown_transaction_end_timeout=17m", "dbms.store.files.preallocate=false"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertEquals(Duration.ofMinutes(17), config.get(shutdown_transaction_end_timeout));
        assertFalse(config.get(preallocate_store_files));
    }

    @Test
    void migrateQueryCacheSize() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.query_cache_size=132"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertEquals(132, config.get(query_cache_size));
    }

    @Test
    void migrateTransactionMemorySettings() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "dbms.memory.transaction.database_max_size=11m",
                        "dbms.memory.transaction.global_max_size=111m",
                        "dbms.memory.transaction.max_size=1111m"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertEquals(mebiBytes(11), config.get(memory_transaction_database_max_size));
        assertEquals(mebiBytes(111), config.get(memory_transaction_global_max_size));
        assertEquals(mebiBytes(1111), config.get(memory_transaction_max_size));
    }

    @Test
    void migrateGroupAndRecoverySettings() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of("dbms.relationship_grouping_threshold=4242", "dbms.recovery.fail_on_missing_files=true"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        assertEquals(4242, config.get(dense_node_threshold));
        assertTrue(config.get(fail_on_missing_files));
    }

    @Test
    void migrateDefaultAddress() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of("dbms.default_listen_address=localhost1", "dbms.default_advertised_address=otherhost"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        assertEquals(new SocketAddress("localhost1"), config.get(default_listen_address));
        assertEquals(new SocketAddress("otherhost"), config.get(default_advertised_address));
    }

    @Test
    void migrateQueryLogsSettings() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "dbms.logs.query.transaction.threshold=7d",
                        "dbms.logs.query.transaction.enabled=INFO",
                        "dbms.logs.query.threshold=8m",
                        "dbms.logs.query.plan_description_enabled=true",
                        "dbms.logs.query.parameter_logging_enabled=false",
                        "dbms.logs.query.obfuscate_literals=true",
                        "dbms.logs.query.max_parameter_length=9",
                        "dbms.logs.query.enabled=VERBOSE",
                        "dbms.logs.query.early_raw_logging_enabled=true"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertEquals(Duration.ofDays(7), config.get(log_queries_transaction_threshold));
        assertEquals(INFO, config.get(log_queries_transactions_level));
        assertEquals(Duration.ofMinutes(8), config.get(log_queries_threshold));
        assertTrue(config.get(log_queries_query_plan));
        assertFalse(config.get(log_queries_parameter_logging_enabled));
        assertTrue(config.get(log_queries_obfuscate_literals));
        assertEquals(9, config.get(query_log_max_parameter_length));
        assertEquals(VERBOSE, config.get(log_queries));
        assertTrue(config.get(log_queries_early_raw_logging_enabled));
    }

    @Test
    void migrateJVMAdditional() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "dbms.jvm.additional=-XX:+UseG1GC",
                        "dbms.jvm.additional=-XX:-OmitStackTraceInFastThrow",
                        "dbms.jvm.additional=-XX:+TrustFinalNonStaticFields"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        assertThat(config.get(additional_jvm))
                .isEqualToIgnoringNewLines(
                        """
                -XX:+UseG1GC
                -XX:-OmitStackTraceInFastThrow
                -XX:+TrustFinalNonStaticFields""");
    }

    @Test
    void migrateSamplingSettings() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "dbms.index_sampling.background_enabled=true",
                        "dbms.index_sampling.sample_size_limit=1048577",
                        "dbms.index_sampling.update_percentage=75"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertTrue(config.get(index_background_sampling_enabled));
        assertEquals(1048577, config.get(index_sample_size_limit));
        assertEquals(75, config.get(index_sampling_update_percentage));
    }

    @Test
    void migratePageCacheAndMemorySettings() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of(
                        "dbms.memory.pagecache.size=1G",
                        "dbms.memory.pagecache.scan.prefetchers=8",
                        "dbms.memory.pagecache.flush.buffer.size_in_pages=129",
                        "dbms.memory.pagecache.flush.buffer.enabled=true",
                        "dbms.memory.pagecache.directio=true",
                        "dbms.memory.off_heap.max_size=4G",
                        "dbms.memory.off_heap.max_cacheable_block_size=2M",
                        "dbms.memory.off_heap.block_cache_size=124",
                        "dbms.memory.heap.max_size=512M",
                        "dbms.memory.heap.initial_size=511M"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertEquals(gibiBytes(1), config.get(pagecache_memory));
        assertEquals(8, config.get(pagecache_scan_prefetch));
        assertEquals(129, config.get(pagecache_flush_buffer_size_in_pages));
        assertEquals(true, config.get(pagecache_buffered_flush_enabled));
        assertEquals(true, config.get(pagecache_direct_io));

        assertEquals(gibiBytes(4), config.get(tx_state_max_off_heap_memory));
        assertEquals(mebiBytes(2), config.get(tx_state_off_heap_max_cacheable_block_size));
        assertEquals(124, config.get(tx_state_off_heap_block_cache_size));

        assertEquals(mebiBytes(512), config.get(max_heap_size));
        assertEquals(mebiBytes(511), config.get(initial_heap_size));
    }

    @Test
    void autoUpgradeMigrationWithBothSet() {
        var logProvider = new AssertableLogProvider();
        Config config = Config.newBuilder()
                .setRaw(Map.of(
                        "internal.dbms.allow_single_automatic_upgrade",
                        "true",
                        automatic_upgrade_enabled.name(),
                        "false"))
                .build();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(config.get(automatic_upgrade_enabled)).isFalse();

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessages(
                        "Use of deprecated setting 'internal.dbms.allow_single_automatic_upgrade'. It is replaced by");
    }

    @Test
    void autoUpgradeMigration() {
        var logProvider = new AssertableLogProvider();
        Config config = Config.newBuilder()
                .setRaw(Map.of("internal.dbms.allow_single_automatic_upgrade", "false"))
                .build();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(config.get(automatic_upgrade_enabled)).isFalse();

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessages(
                        "Use of deprecated setting 'internal.dbms.allow_single_automatic_upgrade'. It is replaced by");
    }

    @Test
    void annotationDataAsJson() {
        var logProvider = new AssertableLogProvider();
        Config config = Config.newBuilder()
                .setRaw(Map.of("db.logs.query.annotation_data_as_json_enabled", "true"))
                .build();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(config.get(log_queries_annotation_data_format)).isEqualTo(AnnotationDataFormat.FLAT_JSON);

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessages(
                        "Use of deprecated setting 'db.logs.query.annotation_data_as_json_enabled'. It is replaced by");
    }

    @Test
    void annotationDataAsJsonConflict() {
        var logProvider = new AssertableLogProvider();
        Config config = Config.newBuilder()
                .setRaw(Map.of(
                        "db.logs.query.annotation_data_as_json_enabled",
                        "true",
                        log_queries_annotation_data_format.name(),
                        AnnotationDataFormat.CYPHER.name()))
                .build();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(config.get(log_queries_annotation_data_format)).isEqualTo(AnnotationDataFormat.CYPHER);

        assertThat(logProvider)
                .forClass(Config.class)
                .forLevel(WARN)
                .containsMessages(
                        "Use of deprecated setting 'db.logs.query.annotation_data_as_json_enabled'. It is replaced by")
                .containsMessages(" is already configured, ignoring db.logs.query.annotation_data_as_json_enabled.");
    }
}

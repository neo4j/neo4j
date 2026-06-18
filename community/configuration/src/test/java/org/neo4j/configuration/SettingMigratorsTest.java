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
import static org.assertj.core.api.Assertions.within;
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

        assertThat(config.get(BoltConnector.enabled)).isTrue();
        assertThat(config.get(HttpConnector.enabled)).isTrue();
        assertThat(config.get(HttpsConnector.enabled)).isTrue();
        assertThat(config.get(thread_pool_shutdown_wait_time)).isEqualTo(ofSeconds(1));

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
        assertThat(config.get(memory_transaction_max_size)).isEqualTo(BYTES.parse("6g"));
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
        assertThat(config.get(memory_transaction_max_size)).isEqualTo(BYTES.parse("7g"));
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

        assertThat(config.get(pagecache_warmup_prefetch_allowlist)).isEqualTo("a");
        assertThat(config.get(procedure_allowlist)).isEqualTo(List.of("a", "b"));
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

        assertThat(config.get(memory_transaction_database_max_size)).isEqualTo(1073741824L);
    }

    @Test
    void windowsServiceNameMigration() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.windows_service_name=foo-bar"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(config.get(windows_service_name)).isEqualTo("foo-bar");
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

        assertThat(config.get(check_point_policy)).isEqualTo(GraphDatabaseSettings.CheckpointPolicy.PERIODIC);
        assertThat(config.get(check_point_interval_time)).isEqualTo(Duration.ofMinutes(10));
        assertThat(config.get(check_point_interval_tx)).isEqualTo(17);
        assertThat(config.get(check_point_interval_volume)).isEqualTo(mebiBytes(125));
        assertThat(config.get(check_point_iops_limit)).isEqualTo(456);
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

        assertThat(config.get(neo4j_home)).isEqualTo(a);
        assertThat(config.get(data_directory)).isEqualTo(b);
        assertThat(config.get(transaction_logs_root_path)).isEqualTo(c);
        assertThat(config.get(script_root_path)).isEqualTo(d);
        assertThat(config.get(database_dumps_root_path)).isEqualTo(e);
        assertThat(config.get(load_csv_file_url_root)).isEqualTo(f);
        assertThat(config.get(plugin_dir)).isEqualTo(g);
        assertThat(config.get(logs_directory)).isEqualTo(h);
        assertThat(config.get(licenses_directory)).isEqualTo(i);

        assertThat(config.get(run_directory)).isEqualTo(j);
        assertThat(config.get(lib_directory)).isEqualTo(k);
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

        assertThat(config.get(forbid_exhaustive_shortestpath)).isTrue();
        assertThat(config.get(forbid_shortestpath_common_nodes)).isFalse();
        assertThat(config.get(cypher_hints_error)).isTrue();
        assertThat(config.get(cypher_lenient_create_relationship)).isFalse();
        assertThat(config.get(cypher_min_replan_interval)).isEqualTo(ofSeconds(11));
        assertThat(config.get(cypher_planner)).isEqualTo(GraphDatabaseSettings.CypherPlanner.COST);
        assertThat(config.get(cypher_render_plan_descriptions)).isTrue();
        assertThat(config.get(query_statistics_divergence_threshold)).isCloseTo(0.42, within(0.01));
    }

    @Test
    void migrateCypherQueryCacheSizeSetting() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("server.db.query_cache_size=100"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger(logProvider.getLog(Config.class));

        assertThat(config.get(query_cache_size)).isEqualTo(100);
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

        assertThat(config.get(transaction_log_buffer_size)).isEqualTo(ByteUnit.bytes(134072));
        assertThat(config.get(preallocate_logical_logs)).isFalse();
        assertThat(config.get(keep_logical_logs)).isEqualTo("3 days");
        assertThat(config.get(logical_log_rotation_threshold)).isEqualTo(mebiBytes(34));
    }

    @Test
    void migrateWatcherSetting() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.filewatcher.enabled=false"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertThat(config.get(filewatcher_enabled)).isFalse();
    }

    @Test
    void migrateLockAcquisitionSetting() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.lock.acquisition.timeout=15m"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertThat(config.get(lock_acquisition_timeout)).isEqualTo(Duration.ofMinutes(15));
    }

    @Test
    void migrateCsvImportSetting() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile, List.of("dbms.import.csv.buffer_size=123", "dbms.import.csv.legacy_quote_escaping=false"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertThat(config.get(csv_buffer_size)).isEqualTo(123);
        assertThat(config.get(csv_legacy_quote_escaping)).isFalse();
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

        assertThat(config.get(track_query_cpu_time)).isTrue();
        assertThat(config.get(bookmark_ready_timeout)).isEqualTo(ofSeconds(100));
        assertThat(config.get(max_concurrent_transactions)).isEqualTo(17);
        assertThat(config.get(transaction_monitor_check_interval)).isEqualTo(ofSeconds(4));
        assertThat(config.get(transaction_sampling_percentage)).isEqualTo(78);
        assertThat(config.get(transaction_timeout)).isEqualTo(ofSeconds(10));
        assertThat(config.get(transaction_tracing_level)).isEqualTo(SAMPLE);
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

        assertThat(config.get(gc_logging_enabled)).isTrue();
        assertThat(config.get(gc_logging_options)).isEqualTo("niceOptions");
        assertThat(config.get(gc_logging_rotation_keep_number)).isEqualTo(7);
        assertThat(config.get(gc_logging_rotation_size)).isEqualTo(ByteUnit.mebiBytes(5));
    }

    @Test
    void migrateProcessorNumberToInternalNamespace() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.upgrade_max_processors=7"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        assertThat(config.get(upgrade_processors)).isEqualTo(7);
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

        assertThat(config.get(pagecache_warmup_enabled)).isTrue();
        assertThat(config.get(pagecache_warmup_prefetch)).isFalse();
        assertThat(config.get(pagecache_warmup_prefetch_allowlist)).isEqualTo("*index*");
        assertThat(config.get(pagecache_warmup_profiling_interval)).isEqualTo(ofSeconds(5));
    }

    @Test
    void migrateShutdownTimeoutAndPreallocations() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile, List.of("dbms.shutdown_transaction_end_timeout=17m", "dbms.store.files.preallocate=false"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertThat(config.get(shutdown_transaction_end_timeout)).isEqualTo(Duration.ofMinutes(17));
        assertThat(config.get(preallocate_store_files)).isFalse();
    }

    @Test
    void migrateQueryCacheSize() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(confFile, List.of("dbms.query_cache_size=132"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertThat(config.get(query_cache_size)).isEqualTo(132);
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

        assertThat(config.get(memory_transaction_database_max_size)).isEqualTo(mebiBytes(11));
        assertThat(config.get(memory_transaction_global_max_size)).isEqualTo(mebiBytes(111));
        assertThat(config.get(memory_transaction_max_size)).isEqualTo(mebiBytes(1111));
    }

    @Test
    void migrateGroupAndRecoverySettings() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of("dbms.relationship_grouping_threshold=4242", "dbms.recovery.fail_on_missing_files=true"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        assertThat(config.get(dense_node_threshold)).isEqualTo(4242);
        assertThat(config.get(fail_on_missing_files)).isTrue();
    }

    @Test
    void migrateDefaultAddress() throws IOException {
        Path confFile = testDirectory.createFile("test.conf");
        Files.write(
                confFile,
                List.of("dbms.default_listen_address=localhost1", "dbms.default_advertised_address=otherhost"));

        Config config = Config.newBuilder().fromFile(confFile).build();
        assertThat(config.get(default_listen_address)).isEqualTo(new SocketAddress("localhost1"));
        assertThat(config.get(default_advertised_address)).isEqualTo(new SocketAddress("otherhost"));
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

        assertThat(config.get(log_queries_transaction_threshold)).isEqualTo(Duration.ofDays(7));
        assertThat(config.get(log_queries_transactions_level)).isEqualTo(INFO);
        assertThat(config.get(log_queries_threshold)).isEqualTo(Duration.ofMinutes(8));
        assertThat(config.get(log_queries_query_plan)).isTrue();
        assertThat(config.get(log_queries_parameter_logging_enabled)).isFalse();
        assertThat(config.get(log_queries_obfuscate_literals)).isTrue();
        assertThat(config.get(query_log_max_parameter_length)).isEqualTo(9);
        assertThat(config.get(log_queries)).isEqualTo(VERBOSE);
        assertThat(config.get(log_queries_early_raw_logging_enabled)).isTrue();
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
        assertThat(config.get(additional_jvm)).isEqualToIgnoringNewLines("""
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

        assertThat(config.get(index_background_sampling_enabled)).isTrue();
        assertThat(config.get(index_sample_size_limit)).isEqualTo(1048577);
        assertThat(config.get(index_sampling_update_percentage)).isEqualTo(75);
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
                        "dbms.memory.heap.max_size=512M",
                        "dbms.memory.heap.initial_size=511M"));

        Config config = Config.newBuilder().fromFile(confFile).build();

        assertThat(config.get(pagecache_memory)).isEqualTo(gibiBytes(1));
        assertThat(config.get(pagecache_scan_prefetch)).isEqualTo(8);
        assertThat(config.get(pagecache_flush_buffer_size_in_pages)).isEqualTo(129);
        assertThat(config.get(pagecache_buffered_flush_enabled)).isTrue();
        assertThat(config.get(pagecache_direct_io)).isTrue();

        assertThat(config.get(max_heap_size)).isEqualTo(mebiBytes(512));
        assertThat(config.get(initial_heap_size)).isEqualTo(mebiBytes(511));
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

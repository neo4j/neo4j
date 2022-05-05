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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_time;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_tx;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_volume;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_iops_limit;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_policy;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_database_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_prefetch_allowlist;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_allowlist;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_max_off_heap_memory;
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
}

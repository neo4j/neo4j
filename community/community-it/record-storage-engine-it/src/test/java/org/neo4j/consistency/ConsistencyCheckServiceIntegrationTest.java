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
package org.neo4j.consistency;

import static java.nio.file.Files.exists;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.test.mockito.mock.Property.property;
import static org.neo4j.test.mockito.mock.Property.set;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService.Result;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLog;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryPools;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Clocks;

@TestDirectoryExtension
@Neo4jLayoutExtension
public class ConsistencyCheckServiceIntegrationTest {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    private GraphStoreFixture fixture;

    @BeforeEach
    void setUp() {
        fixture = new GraphStoreFixture(testDirectory) {
            @Override
            protected void generateInitialData(GraphDatabaseService graphDb) {
                try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
                    Node node1 = set(tx.createNode());
                    Node node2 = set(tx.createNode(), property("key", "exampleValue"));
                    node1.createRelationshipTo(node2, RelationshipType.withName("C"));
                    tx.commit();
                }
            }

            @Override
            protected Map<Setting<?>, Object> getConfig() {
                return settings();
            }
        };
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    @Test
    void reportNotUsedRelationshipReferencedInChain() throws Exception {
        prepareDbWithDeletedRelationshipPartOfTheChain();

        ConsistencyCheckService.Result result = consistencyCheckService().runFullConsistencyCheck();

        assertFalse(result.isSuccessful());

        Path reportFile = result.reportFile();
        assertTrue(exists(reportFile), "Consistency check report file should be generated.");
        assertThat(Files.readString(reportFile))
                .as("Expected to see report about not deleted relationship record present as part of a chain")
                .contains("The relationship record is not in use, but referenced from relationships chain.");
    }

    @Test
    void tracePageCacheAccessOnConsistencyCheck() throws ConsistencyCheckIncompleteException {
        prepareDbWithDeletedRelationshipPartOfTheChain();
        var pageCacheTracer = new DefaultPageCacheTracer();
        fixture.close();
        JobScheduler jobScheduler = JobSchedulerFactory.createScheduler();
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                testDirectory.getFileSystem(),
                Config.defaults(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8)),
                pageCacheTracer,
                NullLog.getInstance(),
                jobScheduler,
                Clocks.nanoClock(),
                new MemoryPools(false));
        try (Lifespan life = new Lifespan(jobScheduler);
                PageCache pageCache = pageCacheFactory.getOrCreatePageCache()) {
            var result = consistencyCheckService()
                    .with(pageCache)
                    .with(new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER))
                    .runFullConsistencyCheck();

            assertFalse(result.isSuccessful());
            assertThat(pageCacheTracer.pins()).isGreaterThanOrEqualTo(74);
            assertThat(pageCacheTracer.unpins()).isEqualTo(pageCacheTracer.pins());
            assertThat(pageCacheTracer.hits()).isGreaterThanOrEqualTo(35);
            assertThat(pageCacheTracer.faults()).isGreaterThanOrEqualTo(39);
        }
    }

    @Test
    void shouldFailOnDatabaseInNeedOfRecovery() throws IOException {
        nonRecoveredDatabase();
        var e = assertThrows(ConsistencyCheckIncompleteException.class, () -> consistencyCheckService()
                .runFullConsistencyCheck());
        assertThat(e.getCause().getMessage())
                .contains("Active logical log detected, this might be a source of inconsistencies.");
    }

    @Test
    void ableToDeleteDatabaseDirectoryAfterConsistencyCheckRun()
            throws ConsistencyCheckIncompleteException, IOException {
        prepareDbWithDeletedRelationshipPartOfTheChain();
        Result consistencyCheck = consistencyCheckService().runFullConsistencyCheck();
        assertFalse(consistencyCheck.isSuccessful());
        // using commons file utils since they do not forgive not closed file descriptors on windows
        org.apache.commons.io.FileUtils.deleteDirectory(
                fixture.databaseLayout().databaseDirectory().toFile());
    }

    @Test
    void shouldSucceedIfStoreIsConsistent() throws Exception {
        // when
        final var result = consistencyCheckService().runFullConsistencyCheck();

        // then
        assertThat(result.isSuccessful()).isTrue();
        final var reportFile = result.reportFile();
        assertThat(fs.fileExists(reportFile))
                .as("Unexpected generation of consistency check report file: " + reportFile)
                .isFalse();
    }

    @Nested
    class ShouldFailIfTheStoreIsInconsistent {
        Path logsDir;
        Config config;
        Path reportFile;
        ConsistencyCheckService consistencyCheckService;

        @BeforeEach
        void makeInconsistent() throws KernelException {
            breakNodeStore();
            logsDir = testDirectory.homePath();
            config = Config.newBuilder()
                    .set(settings())
                    .set(GraphDatabaseSettings.logs_directory, logsDir)
                    .build();
            consistencyCheckService = consistencyCheckService().with(config);
        }

        @AfterEach
        void assertions() throws ConsistencyCheckIncompleteException {
            final var result = consistencyCheckService.runFullConsistencyCheck();
            assertThat(result.isSuccessful())
                    .as("inconsistent database should have failing result")
                    .isFalse();
            assertThat(result.reportFile())
                    .as("expected report file path")
                    .isEqualTo(reportFile)
                    .as("report created")
                    .exists()
                    .isRegularFile();
        }

        Path defaultLogFileName(Date date) {
            return Path.of(
                    "inconsistencies-%s.report".formatted(new SimpleDateFormat("yyyy-MM-dd.HH.mm.ss").format(date)));
        }

        @Test
        void defaultReportFile() {
            final var date = new Date();
            reportFile = logsDir.resolve(defaultLogFileName(date));
            consistencyCheckService = consistencyCheckService.with(date);
        }

        @Test
        void providedReportDirectory() {
            final var date = new Date();
            final var otherDirectory = testDirectory.directory("other");
            reportFile = otherDirectory.resolve(defaultLogFileName(date));
            consistencyCheckService = consistencyCheckService.with(date).with(otherDirectory);
        }

        @Test
        void providedReportFile() {
            reportFile = testDirectory.file("consistency-check.report");
            consistencyCheckService = consistencyCheckService.with(reportFile);
        }
    }

    @Test
    void shouldNotReportDuplicateForHugeLongValues() throws Exception {
        // given
        String propertyKey = "itemId";
        Label label = Label.label("Item");
        fixture.apply(tx -> tx.schema()
                .constraintFor(label)
                .assertPropertyIsUnique(propertyKey)
                .create());
        fixture.apply(tx -> {
            set(tx.createNode(label), property(propertyKey, 973305894188596880L));
            set(tx.createNode(label), property(propertyKey, 973305894188596864L));
        });

        // when
        Result result = consistencyCheckService().runFullConsistencyCheck();

        // then
        assertTrue(result.isSuccessful());
    }

    @Test
    void shouldSkipNonExistentIndexStatisticsStore() throws Exception {
        // given
        fixture.close();
        testDirectory
                .getFileSystem()
                .deleteFile(RecordDatabaseLayout.convert(databaseLayout).indexStatisticsStore());

        // when
        var result = new ConsistencyCheckService(fixture.databaseLayout())
                .with(testDirectory.getFileSystem())
                .with(Config.defaults(settings()))
                .runFullConsistencyCheck();

        // then
        assertThat(result.isSuccessful()).isTrue();
    }

    @Test
    void shouldReportMissingSchemaIndex() throws Exception {
        // given
        Label label = Label.label("label");
        String propKey = "propKey";
        createIndex(label, propKey);
        fixture.close();

        // when
        Path schemaDir = findFile(databaseLayout, "schema");
        FileUtils.deleteDirectory(schemaDir);

        Result result = consistencyCheckService().runFullConsistencyCheck();

        // then
        assertTrue(result.isSuccessful());
        Path reportFile = result.reportFile();
        assertTrue(exists(reportFile), "Consistency check report file should be generated.");
        assertThat(Files.readString(reportFile))
                .as("Expected to see report about schema index not being online")
                .contains("schema rule")
                .contains("not online");
    }

    private void createIndex(Label label, String propKey) {
        fixture.apply(tx -> tx.schema().indexFor(label).on(propKey).create());
        fixture.apply(tx -> tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES));
    }

    private static Path findFile(DatabaseLayout databaseLayout, String targetFile) {
        Path file = databaseLayout.file(targetFile);
        if (Files.notExists(file)) {
            fail("Could not find file " + targetFile);
        }
        return file;
    }

    private void prepareDbWithDeletedRelationshipPartOfTheChain() {
        RelationshipType relationshipType = RelationshipType.withName("testRelationshipType");
        fixture.apply(tx -> {
            Node node1 = set(tx.createNode());
            Node node2 = set(tx.createNode(), property("key", "value"));
            node1.createRelationshipTo(node2, relationshipType);
            node1.createRelationshipTo(node2, relationshipType);
            node1.createRelationshipTo(node2, relationshipType);
            node1.createRelationshipTo(node2, relationshipType);
            node1.createRelationshipTo(node2, relationshipType);
            node1.createRelationshipTo(node2, relationshipType);
        });

        NeoStores neoStores = fixture.neoStores();
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        RelationshipRecord relationshipRecord = new RelationshipRecord(-1);
        var storeCursors = fixture.getStoreCursors();
        try (var cursor = storeCursors.readCursor(RELATIONSHIP_CURSOR)) {
            relationshipStore.getRecordByCursor(
                    4, relationshipRecord, RecordLoad.FORCE, cursor, EmptyMemoryTracker.INSTANCE);
        }
        relationshipRecord.setInUse(false);
        try (var storeCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            relationshipStore.updateRecord(relationshipRecord, storeCursor, NULL_CONTEXT, storeCursors);
        }
    }

    private void nonRecoveredDatabase() throws IOException {
        Path tmpLogDir = testDirectory.homePath().resolve("logs");
        fs.mkdir(tmpLogDir);

        RelationshipType relationshipType = RelationshipType.withName("testRelationshipType");
        fixture.apply(tx -> {
            Node node1 = set(tx.createNode());
            Node node2 = set(tx.createNode(), property("key", "value"));
            node1.createRelationshipTo(node2, relationshipType);
        });
        Path[] txLogs =
                fs.listFiles(LogFilesBuilder.logFilesBasedOnlyBuilder(databaseLayout.getTransactionLogsDirectory(), fs)
                        .build()
                        .logFilesDirectory());
        for (Path file : txLogs) {
            fs.copyToDirectory(file, tmpLogDir);
        }
        fixture.close();
        for (Path txLog : txLogs) {
            fs.deleteFile(txLog);
        }

        for (Path file :
                LogFilesBuilder.logFilesBasedOnlyBuilder(tmpLogDir, fs).build().logFiles()) {
            fs.moveToDirectory(file, databaseLayout.getTransactionLogsDirectory());
        }
    }

    protected Map<Setting<?>, Object> settings() {
        Map<Setting<?>, Object> defaults = new HashMap<>();
        defaults.put(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8));
        defaults.put(GraphDatabaseSettings.logs_directory, databaseLayout.databaseDirectory());
        return defaults;
    }

    private void breakNodeStore() throws KernelException {
        fixture.apply(new GraphStoreFixture.Transaction() {
            @Override
            protected void transactionData(
                    GraphStoreFixture.TransactionDataBuilder tx, GraphStoreFixture.IdGenerator next) {
                tx.create(new NodeRecord(next.node()).initialize(false, -1, false, next.relationship(), 0));
            }
        });
    }

    private ConsistencyCheckService consistencyCheckService() {
        fixture.close();
        return new ConsistencyCheckService(fixture.databaseLayout())
                .with(testDirectory.getFileSystem())
                .with(Config.defaults(settings()));
    }
}

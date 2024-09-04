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
package org.neo4j.kernel.impl.index.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_FAILED;
import static org.neo4j.test.TestLabels.LABEL_ONE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.IndexingTestUtil;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.index.SetInitialStateInNativeIndex;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
public class IndexCleanupIT {
    private static final String propertyKey = "key";

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    private DatabaseManagementService managementService;
    private GraphDatabaseAPI db;

    @AfterEach
    void tearDown() {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    private static Stream<Arguments> indexProviders() {
        return Stream.of(
                Arguments.of(AllIndexProviderDescriptors.RANGE_DESCRIPTOR, IndexType.RANGE),
                Arguments.of(AllIndexProviderDescriptors.POINT_DESCRIPTOR, IndexType.POINT));
    }

    @ParameterizedTest
    @MethodSource("indexProviders")
    void mustClearIndexDirectoryOnDropWhileOnline(IndexProviderDescriptor provider, IndexType indexType)
            throws IOException, KernelException {
        configureDb();
        createIndex(db, indexType, provider, true);

        Path[] providerDirectories = providerDirectories(fs, db);
        for (Path providerDirectory : providerDirectories) {
            assertTrue(
                    fs.listFiles(providerDirectory).length > 0,
                    "expected there to be at least one index per existing provider map");
        }

        dropAllIndexes();

        assertNoIndexFilesExisting(providerDirectories);
    }

    @ParameterizedTest
    @MethodSource("indexProviders")
    void mustClearIndexDirectoryOnDropWhileFailed(IndexProviderDescriptor provider, IndexType indexType)
            throws IOException, KernelException {
        configureDb();
        createIndex(db, indexType, provider, true);
        SetInitialStateInNativeIndex setInitialStateInNativeIndex =
                new SetInitialStateInNativeIndex(BYTE_FAILED, provider);
        restartDatabase(setInitialStateInNativeIndex);
        // Index should be failed at this point

        try (Transaction tx = db.beginTx()) {
            for (IndexDefinition index : tx.schema().getIndexes()) {
                // ignore the lookup indexes which are there by default and have nothing to do with this test
                if (index.getIndexType() == org.neo4j.graphdb.schema.IndexType.LOOKUP) {
                    continue;
                }
                IndexState indexState = tx.schema().getIndexState(index);
                assertEquals(IndexState.FAILED, indexState, "expected index state to be " + IndexState.FAILED);
            }
            tx.commit();
        }

        // when
        dropAllIndexes();

        // then
        assertNoIndexFilesExisting(providerDirectories(fs, db));
    }

    @ParameterizedTest
    @MethodSource("indexProviders")
    void mustClearIndexDirectoryOnDropWhilePopulating(IndexProviderDescriptor provider, IndexType indexType)
            throws InterruptedException, IOException, KernelException {
        // given
        Barrier.Control midPopulation = new Barrier.Control();
        IndexMonitor.MonitorAdapter trappingMonitor = new IndexMonitor.MonitorAdapter() {
            @Override
            public void indexPopulationScanStarting(IndexDescriptor[] indexDescriptors) {
                midPopulation.reached();
            }
        };
        configureDb();
        createSomeData();
        Monitors monitors = db.getDependencyResolver().resolveDependency(Monitors.class);
        monitors.addMonitorListener(trappingMonitor);
        createIndex(db, indexType, provider, false);

        midPopulation.await();
        Path[] providerDirectories = providerDirectories(fs, db);
        for (Path providerDirectory : providerDirectories) {
            assertTrue(
                    fs.listFiles(providerDirectory).length > 0,
                    "expected there to be at least one index per existing provider map");
        }

        // when
        dropAllIndexes();
        midPopulation.release();

        assertNoIndexFilesExisting(providerDirectories);
    }

    private void assertNoIndexFilesExisting(Path[] providerDirectories) throws IOException {
        for (Path providerDirectory : providerDirectories) {
            assertEquals(0, fs.listFiles(providerDirectory).length, "expected there to be no index files");
        }
    }

    private void dropAllIndexes() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().getIndexes().forEach(IndexDefinition::drop);
            tx.commit();
        }
    }

    private void restartDatabase(SetInitialStateInNativeIndex action) throws IOException {
        var openOptions = db.getDependencyResolver()
                .resolveDependency(StorageEngine.class)
                .getOpenOptions();
        managementService.shutdown();
        action.run(fs, databaseLayout, openOptions);
        configureDb();
    }

    private void configureDb() {
        managementService = new TestDatabaseManagementServiceBuilder(databaseLayout).build();
        db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
    }

    private static void createIndex(
            GraphDatabaseService db,
            IndexType indexType,
            IndexProviderDescriptor providerDescriptor,
            boolean awaitOnline)
            throws KernelException {
        try (TransactionImpl tx = (TransactionImpl) db.beginTx()) {
            IndexingTestUtil.createNodePropIndexWithSpecifiedProvider(
                    tx, providerDescriptor, LABEL_ONE, propertyKey, indexType);
            tx.commit();
        }
        if (awaitOnline) {
            try (Transaction tx = db.beginTx()) {
                tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
                tx.commit();
            }
        }
    }

    private static Path[] providerDirectories(FileSystemAbstraction fs, GraphDatabaseAPI db) throws IOException {
        DatabaseLayout databaseLayout = db.databaseLayout();
        Path dbDir = databaseLayout.databaseDirectory();
        Path schemaDir = dbDir.resolve("schema");
        Path indexDir = schemaDir.resolve("index");
        return fs.listFiles(indexDir);
    }

    private void createSomeData() {
        try (Transaction tx = db.beginTx()) {
            tx.createNode().setProperty(propertyKey, "abc");
            tx.commit();
        }
    }
}

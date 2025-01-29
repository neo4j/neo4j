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
package org.neo4j.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.Values.utf8Value;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.database.DatabaseMemoryTrackers;
import org.neo4j.kernel.impl.api.index.IndexPopulationJob;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.RandomValues;

@TestDirectoryExtension
class IndexPopulationIT {
    @Inject
    private TestDirectory directory;

    private GraphDatabaseAPI database;
    private ExecutorService executorService;
    private AssertableLogProvider logProvider;
    private DatabaseManagementService managementService;
    private Monitors monitors;

    @BeforeEach
    void setUp() {
        monitors = new Monitors();
        logProvider = new AssertableLogProvider(true);
        managementService = new TestDatabaseManagementServiceBuilder(directory.homePath())
                .setInternalLogProvider(logProvider)
                .setMonitors(monitors)
                .build();
        database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        executorService = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown() {
        executorService.shutdown();
        managementService.shutdown();
    }

    @Test
    void trackMemoryOnIndexPopulation() throws InterruptedException {
        Label nodeLabel = Label.label("nodeLabel");
        var propertyName = "testProperty";
        var indexName = "testIndex";

        try (Transaction transaction = database.beginTx()) {
            var node = transaction.createNode(nodeLabel);
            node.setProperty(propertyName, RandomStringUtils.insecure().nextAscii(1024));
            transaction.commit();
        }

        var monitors = database.getDependencyResolver().resolveDependency(Monitors.class);
        var memoryTrackers = database.getDependencyResolver().resolveDependency(DatabaseMemoryTrackers.class);
        var otherTracker = memoryTrackers.getOtherTracker();
        var estimatedHeapBefore = otherTracker.estimatedHeapMemory();
        var usedNativeBefore = otherTracker.usedNativeMemory();
        AtomicLong peakUsage = new AtomicLong();
        CountDownLatch populationJobCompleted = new CountDownLatch(1);
        monitors.addMonitorListener(new IndexMonitor.MonitorAdapter() {
            @Override
            public void populationCompleteOn(IndexDescriptor descriptor) {
                peakUsage.set(Math.max(otherTracker.usedNativeMemory(), peakUsage.get()));
            }

            @Override
            public void populationJobCompleted(long peakDirectMemoryUsage) {
                populationJobCompleted.countDown();
            }
        });

        try (Transaction transaction = database.beginTx()) {
            transaction
                    .schema()
                    .indexFor(nodeLabel)
                    .on(propertyName)
                    .withName(indexName)
                    .create();
            transaction.commit();
        }

        waitForOnlineIndexes();
        populationJobCompleted.await();

        long nativeMemoryAfterIndexCompletion = otherTracker.usedNativeMemory();
        assertEquals(estimatedHeapBefore, otherTracker.estimatedHeapMemory());
        assertEquals(usedNativeBefore, nativeMemoryAfterIndexCompletion);
        assertThat(peakUsage.get()).isGreaterThan(nativeMemoryAfterIndexCompletion);
    }

    @Test
    void indexCreationDoNotBlockQueryExecutions() throws Exception {
        Label nodeLabel = Label.label("nodeLabel");
        try (Transaction transaction = database.beginTx()) {
            transaction.createNode(nodeLabel);
            transaction.commit();
        }

        try (Transaction transaction = database.beginTx()) {
            transaction
                    .schema()
                    .indexFor(Label.label("testLabel"))
                    .on("testProperty")
                    .create();

            Future<Number> countFuture = executorService.submit(countNodes());
            assertEquals(1, countFuture.get().intValue());

            transaction.commit();
        }
    }

    @Test
    void createIndexesFromDifferentTransactionsWithoutBlocking() throws ExecutionException, InterruptedException {
        long numberOfIndexesBeforeTest = countIndexes();
        Label nodeLabel = Label.label("nodeLabel2");
        String testProperty = "testProperty";
        try (Transaction transaction = database.beginTx()) {
            transaction
                    .schema()
                    .indexFor(Label.label("testLabel2"))
                    .on(testProperty)
                    .create();

            Future<?> creationFuture = executorService.submit(createIndexForLabelAndProperty(nodeLabel, testProperty));
            creationFuture.get();
            transaction.commit();
        }
        waitForOnlineIndexes();

        assertEquals(numberOfIndexesBeforeTest + 2, countIndexes());
    }

    @Test
    void indexCreationDoNotBlockWritesOnOtherLabel() throws ExecutionException, InterruptedException {
        Label markerLabel = Label.label("testLabel3");
        Label nodesLabel = Label.label("testLabel4");
        try (Transaction transaction = database.beginTx()) {
            transaction.schema().indexFor(markerLabel).on("testProperty").create();

            Future<?> creation = executorService.submit(createNodeWithLabel(nodesLabel));
            creation.get();

            transaction.commit();
        }

        try (Transaction transaction = database.beginTx()) {
            try (ResourceIterator<Node> nodes = transaction.findNodes(nodesLabel)) {
                assertEquals(1, Iterators.count(nodes));
            }
        }
    }

    @Test
    void shutdownDatabaseDuringIndexPopulations() {
        AssertableLogProvider assertableLogProvider = new AssertableLogProvider(true);
        Path storeDir = directory.directory("shutdownDbTest");
        Label testLabel = Label.label("testLabel");
        String propertyName = "testProperty";
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(storeDir)
                .setInternalLogProvider(assertableLogProvider)
                .build();
        GraphDatabaseService shutDownDb = managementService.database(DEFAULT_DATABASE_NAME);
        prePopulateDatabase(shutDownDb, testLabel, propertyName);

        try (Transaction transaction = shutDownDb.beginTx()) {
            transaction.schema().indexFor(testLabel).on(propertyName).create();
            transaction.commit();
        }
        managementService.shutdown();
        assertThat(assertableLogProvider)
                .forClass(IndexPopulationJob.class)
                .forLevel(ERROR)
                .doesNotHaveAnyLogs();
    }

    @Test
    void mustLogPhaseTracker() {
        Label nodeLabel = Label.label("testLabel5");
        String key = "key";
        String value = "hej";
        try (Transaction transaction = database.beginTx()) {
            transaction.createNode(nodeLabel).setProperty(key, value);
            transaction.commit();
        }

        // when
        try (Transaction tx = database.beginTx()) {
            tx.schema().indexFor(nodeLabel).on(key).create();
            tx.commit();
        }
        waitForOnlineIndexes();

        // then
        try (Transaction tx = database.beginTx();
                ResourceIterator<Node> nodes = tx.findNodes(nodeLabel, key, value)) {
            long nodeCount = Iterators.count(nodes);
            assertEquals(1, nodeCount, "expected exactly one hit in index but was ");
            nodes.close();
            tx.commit();
        }
        assertThat(logProvider)
                .forClass(IndexPopulationJob.class)
                .forLevel(DEBUG)
                .containsMessages("TIME/PHASE Final:");
    }

    @Test
    void concurrentUpdatesPopulationOfManyIndexesOnSameSchema() throws InterruptedException, KernelException {
        Label nodeLabel = Label.label("nodeLabel");
        var propertyName = "testProperty";
        var rangeIndex = "rangeIndex";
        var textIndex = "textIndex";
        var concurrentValue = "concurrentValue";

        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch signalLatch = new CountDownLatch(1);

        monitors.addMonitorListener(new PopulationScanCompleteBlock(rangeIndex, blockLatch, signalLatch));

        try (var transaction = database.beginTx()) {
            var node = transaction.createNode(nodeLabel);
            node.setProperty(propertyName, "initialValue");
            transaction.commit();
        }

        try (var transaction = database.beginTx()) {
            transaction
                    .schema()
                    .indexFor(nodeLabel)
                    .on(propertyName)
                    .withIndexType(IndexType.RANGE)
                    .withName(rangeIndex)
                    .create();
            transaction
                    .schema()
                    .indexFor(nodeLabel)
                    .on(propertyName)
                    .withIndexType(IndexType.TEXT)
                    .withName(textIndex)
                    .create();
            transaction.commit();
        }

        Assertions.assertThat(signalLatch.await(1, MINUTES)).isTrue();

        // scan complete
        // new transaction can add to the concurrent queue that will be processed on flip
        try (var concurrentUpdater = database.beginTx()) {
            var node = concurrentUpdater.createNode(nodeLabel);
            node.setProperty(propertyName, concurrentValue);
            concurrentUpdater.commit();
        }

        // release population flip
        blockLatch.countDown();
        waitForOnlineIndexes();

        assertThat(nodeValueExistsInIndex(propertyName, concurrentValue, rangeIndex))
                .isTrue();
        assertThat(nodeValueExistsInIndex(propertyName, concurrentValue, textIndex))
                .isTrue();
    }

    private boolean nodeValueExistsInIndex(String propertyName, String value, String indexName) throws KernelException {
        try (var transaction = database.beginTx()) {
            var ktx = ((TransactionImpl) transaction).kernelTransaction();
            TokenRead tokenRead = ktx.tokenRead();
            int propertyId = tokenRead.propertyKey(propertyName);
            var query = PropertyIndexQuery.exact(propertyId, utf8Value(value.getBytes(UTF_8)));

            var index = ktx.schemaRead().indexGetForName(indexName);
            try (var cursor = ktx.cursors().allocateNodeValueIndexCursor(NULL_CONTEXT, INSTANCE)) {
                var indexSession = ktx.dataRead().indexReadSession(index);
                ktx.dataRead().nodeIndexSeek(ktx.queryContext(), indexSession, cursor, unconstrained(), query);
                return cursor.next();
            }
        }
    }

    private static void prePopulateDatabase(GraphDatabaseService database, Label testLabel, String propertyName) {
        final RandomValues randomValues = RandomValues.create();

        try (Transaction transaction = database.beginTx()) {
            for (int j = 0; j < 10_000; j++) {
                Node node = transaction.createNode(testLabel);
                Object property = randomValues.nextValue().asObject();
                node.setProperty(propertyName, property);
            }
            transaction.commit();
        }
    }

    private Runnable createNodeWithLabel(Label label) {
        return () -> {
            try (Transaction transaction = database.beginTx()) {
                transaction.createNode(label);
                transaction.commit();
            }
        };
    }

    private long countIndexes() {
        try (Transaction transaction = database.beginTx()) {
            return Iterables.count(transaction.schema().getIndexes());
        }
    }

    private Runnable createIndexForLabelAndProperty(Label label, String propertyKey) {
        return () -> {
            try (Transaction transaction = database.beginTx()) {
                transaction.schema().indexFor(label).on(propertyKey).create();
                transaction.commit();
            }

            waitForOnlineIndexes();
        };
    }

    private void waitForOnlineIndexes() {
        try (Transaction transaction = database.beginTx()) {
            transaction.schema().awaitIndexesOnline(2, MINUTES);
            transaction.commit();
        }
    }

    private Callable<Number> countNodes() {
        return () -> {
            try (Transaction transaction = database.beginTx()) {
                Result result = transaction.execute("MATCH (n) RETURN count(n) as count");
                Map<String, Object> resultMap = result.next();
                return (Number) resultMap.get("count");
            }
        };
    }

    private static class PopulationScanCompleteBlock extends IndexMonitor.MonitorAdapter {
        private final String indexName;
        private final CountDownLatch blockLatch;
        private final CountDownLatch signalLatch;

        public PopulationScanCompleteBlock(String indexName, CountDownLatch blockLatch, CountDownLatch signalLatch) {
            this.indexName = indexName;
            this.blockLatch = blockLatch;
            this.signalLatch = signalLatch;
        }

        @Override
        public void indexPopulationScanComplete(IndexDescriptor[] indexDescriptors) {
            if (Arrays.stream(indexDescriptors).map(IndexDescriptor::getName).anyMatch(indexName::equals)) {
                signalLatch.countDown();
                try {
                    blockLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}

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

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.batchimport.api.Configuration.DEFAULT;
import static org.neo4j.batchimport.api.Monitor.NO_MONITOR;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.index_population_queue_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.internal.batchimport.DefaultAdditionalIds.EMPTY;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.batchimport.api.InputIterable;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.batchimport.api.input.PropertySizeCalculator;
import org.neo4j.batchimport.api.input.ReadableGroups;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckService.Result;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.AnyTokens;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.batchimport.GeneratingInputIterator;
import org.neo4j.internal.batchimport.ParallelBatchImporter;
import org.neo4j.internal.batchimport.RandomsStates;
import org.neo4j.internal.batchimport.input.BadCollector;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.helpers.TimeUtil;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.RandomValues;

/**
 * Idea is to test a {@link MultipleIndexPopulator} with a bunch of indexes, some of which can fail randomly.
 * Also updates are randomly streaming in during population. In the end all the indexes should have been populated
 * with correct data.
 */
@ExtendWith(RandomExtension.class)
@TestDirectoryExtension
class MultipleIndexPopulationStressIT {
    private static final String[] TOKENS = new String[] {"One", "Two", "Three", "Four"};
    private ExecutorService executor;

    @Inject
    private RandomSupport random;

    @Inject
    private TestDirectory directory;

    @Inject
    private DefaultFileSystemAbstraction fileSystemAbstraction;

    private boolean expectingNLI = true;
    private boolean expectingRTI = true;

    @AfterEach
    public void tearDown() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Test
    public void populateMultipleIndexWithSeveralNodesMultiThreaded() throws Exception {
        prepareAndRunTest(10, 0, TimeUnit.SECONDS.toMillis(5), index_population_queue_threshold.defaultValue());
    }

    @Test
    public void populateMultipleIndexWithSeveralRelationshipsMultiThreaded() throws Exception {
        prepareAndRunTest(10, 10, TimeUnit.SECONDS.toMillis(5), index_population_queue_threshold.defaultValue());
    }

    @Test
    public void populateMultipleIndexWithSeveralDenseNodesMultiThreaded() throws Exception {
        prepareAndRunTest(10, 1000, TimeUnit.SECONDS.toMillis(5), index_population_queue_threshold.defaultValue());
    }

    @Test
    public void shouldPopulateMultipleIndexPopulatorsUnderStressMultiThreaded() throws Exception {
        int concurrentUpdatesQueueFlushThreshold = random.nextInt(100, 5000);
        readConfigAndRunTest(concurrentUpdatesQueueFlushThreshold);
    }

    private void readConfigAndRunTest(int concurrentUpdatesQueueFlushThreshold) throws Exception {
        // GIVEN a database with random data in it
        long nodeCount = SettingValueParsers.parseLongWithUnit(
                System.getProperty(getClass().getName() + ".nodes", "200k"));
        long relCount = SettingValueParsers.parseLongWithUnit(
                System.getProperty(getClass().getName() + ".relationships", "200k"));
        long duration =
                TimeUtil.parseTimeMillis.apply(System.getProperty(getClass().getName() + ".duration", "5s"));
        prepareAndRunTest(nodeCount, relCount, duration, concurrentUpdatesQueueFlushThreshold);
    }

    private void prepareAndRunTest(
            long nodeCount, long relCount, long durationMillis, int concurrentUpdatesQueueFlushThreshold)
            throws Exception {
        createRandomData(nodeCount, relCount);
        // randomly drop the initial token indexes in the database
        if (random.nextBoolean()) {
            dropIndexes();
        }
        long endTime = currentTimeMillis() + durationMillis;

        // WHEN/THEN run tests for at least the specified durationMillis
        while (currentTimeMillis() < endTime) {
            runTest(nodeCount, relCount, concurrentUpdatesQueueFlushThreshold);
        }
    }

    private void runTest(long nodeCount, long relCount, int concurrentUpdatesQueueFlushThreshold) throws Exception {
        // WHEN creating the indexes under stressful updates
        populateDbAndIndexes(nodeCount, relCount);
        Config config = Config.newBuilder()
                .set(neo4j_home, directory.homePath())
                .set(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8))
                .set(index_population_queue_threshold, concurrentUpdatesQueueFlushThreshold)
                .build();
        Result result = new ConsistencyCheckService(RecordDatabaseLayout.of(config))
                .with(config)
                .runFullConsistencyCheck();
        assertThat(result.isSuccessful())
                .as("Database consistency")
                .withFailMessage(
                        "%nExpecting database to be consistent, but it was not.%n%s%nDetailed report: '%s'%n",
                        result.summary(), result.reportFile())
                .isTrue();
        dropIndexes();
    }

    private void populateDbAndIndexes(long nodeCount, long relCount) throws InterruptedException {
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder(directory.homePath()).build();
        final GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
        try {
            try (var tx = db.beginTx();
                    var softly = new AutoCloseableSoftAssertions()) {
                softly.assertThat(Iterables.count(tx.getAllNodes()))
                        .as("Number of nodes")
                        .isEqualTo(nodeCount);
                softly.assertThat(Iterables.count(tx.getAllRelationships()))
                        .as("Number of relationships")
                        .isEqualTo(relCount);
            }
            createIndexes(db);
            final AtomicBoolean end = new AtomicBoolean();
            executor = Executors.newCachedThreadPool();
            for (int i = 0; i < 10; i++) {
                executor.submit(() -> {
                    ChangeRandomEntities changeRandomEntities =
                            new ChangeRandomEntities(db, RandomValues.create(), nodeCount, relCount);
                    while (!end.get()) {
                        changeRandomEntities.node();
                        changeRandomEntities.relationship();
                    }
                });
            }

            while (!indexesAreOnline(db)) {
                Thread.sleep(100);
            }
            end.set(true);
            executor.shutdown();
            executor.awaitTermination(10, SECONDS);
            executor = null;
        } finally {
            managementService.shutdown();
        }
    }

    private void dropIndexes() {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(directory.homePath())
                .setConfig(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8))
                .build();
        GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = db.beginTx()) {
            tx.schema().getIndexes().forEach(IndexDefinition::drop);
            tx.commit();
        } finally {
            managementService.shutdown();
        }

        expectingNLI = false;
        expectingRTI = false;
    }

    private static boolean indexesAreOnline(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            for (IndexDefinition index : tx.schema().getIndexes()) {
                switch (tx.schema().getIndexState(index)) {
                    case ONLINE:
                        break; // Good
                    case POPULATING:
                        return false; // Still populating
                    case FAILED:
                        fail(index + " entered failed state: " + tx.schema().getIndexFailure(index));
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            tx.commit();
        }
        return true;
    }

    /**
     * Create a bunch of indexes in a single transaction. This will have all the indexes being built
     * using a single store scan... and this is the gist of what we're testing.
     */
    private void createIndexes(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            createTokenIndexes(tx);
            createNodePropertyIndexes(tx);
            createRelationshipPropertyIndexes(tx);
            tx.commit();
        }
    }

    private void createTokenIndexes(Transaction tx) {
        if (!expectingNLI && random.nextBoolean()) {
            tx.schema().indexFor(AnyTokens.ANY_LABELS).create();
            expectingNLI = true;
        }
        if (!expectingRTI && random.nextBoolean()) {
            tx.schema().indexFor(AnyTokens.ANY_RELATIONSHIP_TYPES).create();
            expectingRTI = true;
        }
    }

    private void createNodePropertyIndexes(Transaction tx) {
        for (String label : random.selection(TOKENS, 3, 3, false)) {
            for (String propertyKey : random.selection(TOKENS, 3, 3, false)) {
                tx.schema().indexFor(Label.label(label)).on(propertyKey).create();
            }
        }
    }

    private void createRelationshipPropertyIndexes(Transaction tx) {
        for (String type : random.selection(TOKENS, 3, 3, false)) {
            for (String propertyKey : random.selection(TOKENS, 3, 3, false)) {
                tx.schema()
                        .indexFor(RelationshipType.withName(type))
                        .on(propertyKey)
                        .create();
            }
        }
    }

    private static class ChangeRandomEntities {
        private final GraphDatabaseService db;
        private final RandomValues random;
        private final long nodeCount;
        private final long relCount;

        ChangeRandomEntities(GraphDatabaseService db, RandomValues random, long nodeCount, long relCount) {
            this.db = db;
            this.random = random;
            this.nodeCount = nodeCount;
            this.relCount = relCount;
        }

        void node() {
            changeRandomEntity(Transaction::getNodeById, nodeCount);
        }

        void relationship() {
            changeRandomEntity(Transaction::getRelationshipById, relCount);
        }

        private void changeRandomEntity(BiFunction<Transaction, Long, Entity> getEntityById, long count) {
            if (count < 1) {
                return;
            }

            try (Transaction tx = db.beginTx()) {
                long id = random.nextLong(count);
                Entity entity = getEntityById.apply(tx, id);
                Object[] keys = Iterables.asCollection(entity.getPropertyKeys()).toArray();
                String key = (String) random.among(keys);
                if (random.nextFloat() < 0.1) { // REMOVE
                    entity.removeProperty(key);
                } else { // CHANGE
                    entity.setProperty(key, random.nextValue().asObject());
                }
                tx.commit();
            } catch (
                    NotFoundException
                            e) { // It's OK, it happens if some other thread deleted that property in between us reading
                // it and
                // removing or setting it
            }
        }
    }

    private void createRandomData(long nodeCount, long relCount) throws Exception {
        Config config = Config.defaults(neo4j_home, directory.homePath());
        try (RandomDataInput input = new RandomDataInput(nodeCount, relCount);
                JobScheduler jobScheduler = new ThreadPoolJobScheduler()) {
            RecordDatabaseLayout layout = RecordDatabaseLayout.of(config);
            IndexImporterFactory indexImporterFactory = new IndexImporterFactoryImpl();
            BatchImporter importer = new ParallelBatchImporter(
                    layout,
                    fileSystemAbstraction,
                    PageCacheTracer.NULL,
                    DEFAULT,
                    NullLogService.getInstance(),
                    ExecutionMonitor.INVISIBLE,
                    EMPTY,
                    new EmptyLogTailMetadata(config),
                    config,
                    NO_MONITOR,
                    jobScheduler,
                    Collector.EMPTY,
                    TransactionLogInitializer.getLogFilesInitializer(),
                    indexImporterFactory,
                    INSTANCE,
                    NULL_CONTEXT_FACTORY);
            importer.doImport(input);
        }
    }

    private class RandomEntityGenerator extends GeneratingInputIterator<RandomValues> {
        RandomEntityGenerator(long count, Generator<RandomValues> randomsGenerator) {
            super(count, 1_000, new RandomsStates(random.seed()), randomsGenerator, 0);
        }
    }

    private class RandomDataInput implements Input, AutoCloseable {
        private final long nodeCount;
        private final long relCount;
        private final BadCollector badCollector;

        RandomDataInput(long nodeCount, long relCount) {
            this.nodeCount = nodeCount > 0 ? nodeCount : 0;
            this.relCount = nodeCount > 0 && relCount > 0 ? relCount : 0;
            this.badCollector = createBadCollector();
        }

        @Override
        public InputIterable nodes(Collector badCollector) {
            return () -> new RandomEntityGenerator(nodeCount, (state, visitor, id) -> {
                visitor.id(id);
                visitor.labels(random.selection(TOKENS, 1, TOKENS.length, false));
                properties(visitor);
            });
        }

        @Override
        public InputIterable relationships(Collector badCollector) {
            return () -> new RandomEntityGenerator(relCount, (state, visitor, id) -> {
                visitor.startId(random.nextLong(nodeCount));
                visitor.type(random.among(TOKENS));
                visitor.endId(random.nextLong(nodeCount));
                properties(visitor);
            });
        }

        @Override
        public IdType idType() {
            return IdType.ACTUAL;
        }

        @Override
        public ReadableGroups groups() {
            return ReadableGroups.EMPTY;
        }

        private void properties(InputEntityVisitor visitor) {
            String[] keys = random.randomValues().selection(TOKENS, 1, TOKENS.length, false);
            for (String key : keys) {
                visitor.property(key, random.nextValueAsObject());
            }
        }

        private BadCollector createBadCollector() {
            try {
                return new BadCollector(
                        fileSystemAbstraction.openAsOutputStream(
                                directory.homePath().resolve("bad"), false),
                        0,
                        0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Estimates calculateEstimates(PropertySizeCalculator valueSizeCalculator) {
            long labelCount = nodeCount * TOKENS.length / 2;
            long nodePropCount = nodeCount * TOKENS.length / 2;
            long nodePropSize = nodePropCount * Long.BYTES;
            long relPropCount = relCount * TOKENS.length / 2;
            long relPropSize = relPropCount * Long.BYTES;
            return Input.knownEstimates(
                    nodeCount, relCount, nodePropCount, relPropCount, nodePropSize, relPropSize, labelCount);
        }

        @Override
        public void close() {
            badCollector.close();
        }
    }
}

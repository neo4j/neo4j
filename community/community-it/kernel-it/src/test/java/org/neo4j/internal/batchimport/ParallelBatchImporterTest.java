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
package org.neo4j.internal.batchimport;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.batchimport.api.input.Input.knownEstimates;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.batchimport.DefaultAdditionalIds.EMPTY;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterables.stream;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.logging.log4j.LogConfig.DEBUG_LOG;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;
import org.assertj.core.description.Description;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.InputIterable;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckService.Result;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ProcessorAssignmentStrategies;
import org.neo4j.internal.batchimport.staging.StageExecution;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Values;

@Neo4jLayoutExtension
@ExtendWith(RandomExtension.class)
public class ParallelBatchImporterTest {
    private static final int NODE_COUNT = 10_000;
    private static final int RELATIONSHIPS_PER_NODE = 5;
    private static final int RELATIONSHIP_COUNT = NODE_COUNT * RELATIONSHIPS_PER_NODE;
    private static final int RELATIONSHIP_TYPES = 3;
    private static final int NUMBER_OF_ID_GROUPS = 5;

    @Inject
    private RandomSupport random;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    private InputIdGenerator inputIdGenerator;
    private final Configuration config = new Configuration() {
        @Override
        public int batchSize() {
            // Set to extra low to exercise the internals a bit more.
            return 100;
        }

        @Override
        public int maxNumberOfWorkerThreads() {
            // Let's really crank up the number of threads to try and flush out all and any parallelization issues.
            int cores = Runtime.getRuntime().availableProcessors();
            return random.intBetween(cores, cores + 100);
        }

        @Override
        public long maxOffHeapMemory() {
            // This calculation is just to try and hit some sort of memory limit so that relationship import
            // is split up into multiple rounds. Also to see that relationship group defragmentation works
            // well when doing multiple rounds.
            double ratio = NODE_COUNT / 1_000D;
            long mebi = mebiBytes(1);
            return random.nextInt((int) (ratio * mebi / 2), (int) (ratio * mebi));
        }
    };

    private static Stream<Arguments> params() {
        return Stream.of(
                // Long input ids, actual node id input
                arguments(new LongInputIdGenerator(), IdType.INTEGER),
                // String input ids, generate ids from stores
                arguments(new StringInputIdGenerator(), IdType.STRING));
    }

    @ParameterizedTest
    @MethodSource("params")
    void shouldImportCsvData(InputIdGenerator inputIdGenerator, IdType idType) throws Exception {
        this.inputIdGenerator = inputIdGenerator;

        // GIVEN
        ExecutionMonitor processorAssigner =
                ProcessorAssignmentStrategies.eagerRandomSaturation(config.maxNumberOfWorkerThreads());
        CapturingMonitor monitor = new CapturingMonitor(processorAssigner);

        boolean successful = false;
        Groups groups = new Groups();
        IdGroupDistribution groupDistribution =
                new IdGroupDistribution(NODE_COUNT, NUMBER_OF_ID_GROUPS, random.random(), groups);
        long nodeRandomSeed = random.nextLong();
        long relationshipRandomSeed = random.nextLong();
        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        JobScheduler jobScheduler = new ThreadPoolJobScheduler();
        // This will have statistically half the nodes be considered dense
        Config dbConfig = Config.defaults(GraphDatabaseSettings.dense_node_threshold, RELATIONSHIPS_PER_NODE * 2);
        augmentConfig(dbConfig);
        IndexImporterFactoryImpl indexImporterFactory = new IndexImporterFactoryImpl();
        final BatchImporter inserter = new ParallelBatchImporter(
                databaseLayout,
                fs,
                pageCacheTracer,
                config,
                NullLogService.getInstance(),
                monitor,
                EMPTY,
                new EmptyLogTailMetadata(dbConfig),
                dbConfig,
                Monitor.NO_MONITOR,
                jobScheduler,
                Collector.EMPTY,
                TransactionLogInitializer.getLogFilesInitializer(),
                indexImporterFactory,
                INSTANCE,
                contextFactory);
        LongAdder propertyCount = new LongAdder();
        LongAdder relationshipCount = new LongAdder();
        try {
            // WHEN
            inserter.doImport(Input.input(
                    nodes(
                            nodeRandomSeed,
                            NODE_COUNT,
                            config.batchSize(),
                            inputIdGenerator,
                            groupDistribution,
                            propertyCount),
                    relationships(
                            relationshipRandomSeed,
                            RELATIONSHIP_COUNT,
                            config.batchSize(),
                            inputIdGenerator,
                            groupDistribution,
                            propertyCount,
                            relationshipCount),
                    idType,
                    knownEstimates(
                            NODE_COUNT,
                            RELATIONSHIP_COUNT,
                            NODE_COUNT * TOKENS.length / 2,
                            RELATIONSHIP_COUNT * TOKENS.length / 2,
                            NODE_COUNT * TOKENS.length / 2 * Long.BYTES,
                            RELATIONSHIP_COUNT * TOKENS.length / 2 * Long.BYTES,
                            NODE_COUNT * TOKENS.length / 2),
                    groups));

            assertThat(pageCacheTracer.pins()).isGreaterThan(0);
            assertThat(pageCacheTracer.pins()).isEqualTo(pageCacheTracer.unpins());
            assertThat(pageCacheTracer.pins())
                    .isEqualTo(Math.addExact(pageCacheTracer.faults(), pageCacheTracer.hits()));

            // THEN
            DatabaseManagementService managementService =
                    getDBMSBuilder(databaseLayout).build();
            GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = db.beginTx()) {
                inputIdGenerator.reset();
                verifyData(
                        NODE_COUNT,
                        RELATIONSHIP_COUNT,
                        db,
                        tx,
                        groupDistribution,
                        nodeRandomSeed,
                        relationshipRandomSeed);
                tx.commit();
            } finally {
                managementService.shutdown();
            }
            assertThat(mentionsCountsStoreRebuild(databaseLayout)).isFalse();
            assertConsistent(databaseLayout);
            successful = true;
        } finally {
            jobScheduler.close();
            if (!successful) {
                Path failureFile = databaseLayout.databaseDirectory().resolve("input");
                try (PrintStream out = new PrintStream(Files.newOutputStream(failureFile))) {
                    out.println("Seed used in this failing run: " + random.seed());
                    out.println(inputIdGenerator);
                    inputIdGenerator.reset();
                    out.println();
                    out.println("Processor assignments");
                    out.println(processorAssigner);
                }
                System.err.println("Additional debug information stored in " + failureFile);
            }
        }
    }

    private boolean mentionsCountsStoreRebuild(DatabaseLayout databaseLayout) throws IOException {
        var config = Config.newBuilder()
                .set(
                        GraphDatabaseSettings.neo4j_home,
                        databaseLayout.getNeo4jLayout().homeDirectory())
                .build();
        var debugLogPath = config.get(GraphDatabaseSettings.logs_directory).resolve(DEBUG_LOG);
        try (var lines = Files.lines(debugLogPath)) {
            return lines.anyMatch(line -> line.contains("Missing counts store, rebuilding it")
                    && line.contains("[" + databaseLayout.getDatabaseName()));
        }
    }

    static void assertConsistent(DatabaseLayout databaseLayout) throws ConsistencyCheckIncompleteException {
        Result result = new ConsistencyCheckService(databaseLayout)
                .with(Config.defaults(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8)))
                .runFullConsistencyCheck();
        assertThat(result.isSuccessful())
                .as(new Description() {
                    @Override
                    public String value() {
                        var builder = new StringBuilder("Database contains inconsistencies. " + result);
                        if (result.reportFile() != null) {
                            builder.append(format("%nInconsistencies:"));
                            try (var lines = Files.lines(result.reportFile())) {
                                lines.forEach(line -> builder.append(format("%n%s", line)));
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                        return builder.toString();
                    }
                })
                .isTrue();
    }

    protected void augmentConfig(Config config) {}

    protected TestDatabaseManagementServiceBuilder getDBMSBuilder(DatabaseLayout layout) {
        return new TestDatabaseManagementServiceBuilder(layout);
    }

    private static class ExistingId {
        private final Object id;
        private final long nodeIndex;

        ExistingId(Object id, long nodeIndex) {
            this.id = id;
            this.nodeIndex = nodeIndex;
        }
    }

    public abstract static class InputIdGenerator {
        abstract void reset();

        abstract Object nextNodeId(RandomValues random, long item);

        abstract ExistingId randomExisting(RandomValues random);

        abstract Object miss(RandomValues random, Object id, float chance);

        abstract boolean isMiss(Object id);

        static String randomType(RandomValues random) {
            return "TYPE" + random.nextInt(RELATIONSHIP_TYPES);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }

    private static class LongInputIdGenerator extends InputIdGenerator {
        @Override
        void reset() {}

        @Override
        synchronized Object nextNodeId(RandomValues random, long item) {
            return item;
        }

        @Override
        ExistingId randomExisting(RandomValues random) {
            long index = random.nextInt(NODE_COUNT);
            return new ExistingId(index, index);
        }

        @Override
        Object miss(RandomValues random, Object id, float chance) {
            return random.nextFloat() < chance ? (Long) id + 100_000_000 : id;
        }

        @Override
        boolean isMiss(Object id) {
            return (Long) id >= 100_000_000;
        }
    }

    private static class StringInputIdGenerator extends InputIdGenerator {
        private final String[] strings = new String[NODE_COUNT];

        @Override
        void reset() {
            Arrays.fill(strings, null);
        }

        @Override
        Object nextNodeId(RandomValues random, long item) {
            byte[] randomBytes = random.nextByteArray(10, 10).asObjectCopy();
            String result = UUID.nameUUIDFromBytes(randomBytes).toString();
            strings[toIntExact(item)] = result;
            return result;
        }

        @Override
        ExistingId randomExisting(RandomValues random) {
            int index = random.nextInt(strings.length);
            return new ExistingId(strings[index], index);
        }

        @Override
        Object miss(RandomValues random, Object id, float chance) {
            return random.nextFloat() < chance ? "_" + id : id;
        }

        @Override
        boolean isMiss(Object id) {
            return ((String) id).startsWith("_");
        }
    }

    private void verifyData(
            int nodeCount,
            int relationshipCount,
            GraphDatabaseService db,
            Transaction tx,
            IdGroupDistribution groups,
            long nodeRandomSeed,
            long relationshipRandomSeed)
            throws IOException {
        // Read all nodes, relationships and properties ad verify against the input data.
        LongAdder propertyCount = new LongAdder();
        try (InputIterator nodes = nodes(
                                nodeRandomSeed, nodeCount, config.batchSize(), inputIdGenerator, groups, propertyCount)
                        .iterator();
                InputIterator relationships = relationships(
                                relationshipRandomSeed,
                                relationshipCount,
                                config.batchSize(),
                                inputIdGenerator,
                                groups,
                                propertyCount,
                                new LongAdder())
                        .iterator();
                ResourceIterable<Node> dbNodes = tx.getAllNodes()) {
            Map<String, Node> nodeByInputId = new HashMap<>(nodeCount);
            for (final var node : dbNodes) {
                String id = (String) node.getProperty("id");
                assertNull(nodeByInputId.put(id, node));
            }

            int verifiedNodes = 0;
            long allNodesScanLabelCount = 0;
            InputChunk chunk = nodes.newChunk();
            InputEntity input = new InputEntity();
            while (nodes.next(chunk)) {
                while (chunk.next(input)) {
                    String iid = uniqueId(input.idGroup, input.objectId);
                    Node node = nodeByInputId.get(iid);
                    assertNodeEquals(input, node);
                    verifiedNodes++;
                    assertDegrees(node);
                    allNodesScanLabelCount += Iterables.count(node.getLabels());
                }
            }
            assertEquals(nodeCount, verifiedNodes);

            // Labels
            long labelScanStoreEntryCount = stream(tx.getAllLabels())
                    .mapToLong(l -> Iterators.count(tx.findNodes(l)))
                    .sum();

            assertEquals(
                    allNodesScanLabelCount,
                    labelScanStoreEntryCount,
                    format(
                            "Expected label scan store and node store to have same number labels. But %n"
                                    + "#labelsInNodeStore=%d%n"
                                    + "#labelsInLabelScanStore=%d%n",
                            allNodesScanLabelCount, labelScanStoreEntryCount));

            // Relationships
            chunk = relationships.newChunk();
            Map<String, Relationship> relationshipByName = new HashMap<>();
            try (ResourceIterable<Relationship> allRelationships = tx.getAllRelationships()) {
                for (Relationship relationship : allRelationships) {
                    relationshipByName.put((String) relationship.getProperty("id"), relationship);
                }
            }
            int verifiedRelationships = 0;
            while (relationships.next(chunk)) {
                while (chunk.next(input)) {
                    if (!inputIdGenerator.isMiss(input.objectStartId) && !inputIdGenerator.isMiss(input.objectEndId)) {
                        // A relationship referring to missing nodes. The InputIdGenerator is expected to generate
                        // some (very few) of those. Skip it.
                        String name = (String) propertyOf(input, "id");
                        Relationship relationship = relationshipByName.get(name);
                        assertNotNull(relationship, "Expected there to be a relationship with name '" + name + "'");
                        assertEquals(
                                nodeByInputId.get(uniqueId(input.startIdGroup, input.objectStartId)),
                                relationship.getStartNode());
                        assertEquals(
                                nodeByInputId.get(uniqueId(input.endIdGroup, input.objectEndId)),
                                relationship.getEndNode());
                        assertRelationshipEquals(input, relationship);
                    }
                    verifiedRelationships++;
                }
            }
            assertEquals(relationshipCount, verifiedRelationships);
        }
    }

    private static void assertDegrees(Node node) {
        for (RelationshipType type : node.getRelationshipTypes()) {
            for (Direction direction : Direction.values()) {
                long degree = node.getDegree(type, direction);
                long actualDegree = count(node.getRelationships(direction, type));
                assertEquals(actualDegree, degree);
            }
        }
    }

    private static String uniqueId(Group group, Object id) {
        return group.name() + "_" + id;
    }

    private static Object propertyOf(InputEntity input, String key) {
        Object[] properties = input.properties();
        for (int i = 0; i < properties.length; i++) {
            if (properties[i++].equals(key)) {
                return properties[i];
            }
        }
        throw new IllegalStateException(key + " not found on " + input);
    }

    private static void assertRelationshipEquals(InputEntity input, Relationship relationship) {
        // properties
        assertPropertiesEquals(input, relationship);

        // type
        assertEquals(input.stringType, relationship.getType().name());
    }

    private static void assertNodeEquals(InputEntity input, Node node) {
        // properties
        assertPropertiesEquals(input, node);

        // labels
        Set<String> expectedLabels = asSet(input.labels());
        for (Label label : node.getLabels()) {
            assertTrue(expectedLabels.remove(label.name()));
        }
        assertTrue(expectedLabels.isEmpty());
    }

    private static void assertPropertiesEquals(InputEntity input, Entity entity) {
        Object[] properties = input.properties();
        for (int i = 0; i < properties.length; i++) {
            String key = (String) properties[i++];
            Object value = properties[i];
            assertPropertyValueEquals(input, entity, key, value, entity.getProperty(key));
        }
    }

    private static void assertPropertyValueEquals(
            InputEntity input, Entity entity, String key, Object expected, Object array) {
        if (expected.getClass().isArray()) {
            int length = Array.getLength(expected);
            assertEquals(length, Array.getLength(array), input + ", " + entity);
            for (int i = 0; i < length; i++) {
                assertPropertyValueEquals(input, entity, key, Array.get(expected, i), Array.get(array, i));
            }
        } else {
            assertEquals(Values.of(expected), Values.of(array), input + ", " + entity + " for key:" + key);
        }
    }

    private InputIterable relationships(
            final long randomSeed,
            final long count,
            int batchSize,
            final InputIdGenerator idGenerator,
            final IdGroupDistribution groups,
            LongAdder propertyCount,
            LongAdder relationshipCount) {
        return () -> new GeneratingInputIterator<>(
                count,
                batchSize,
                new RandomsStates(randomSeed),
                (randoms, visitor, id) -> {
                    int thisPropertyCount = randomProperties(randoms, "Name " + id, visitor);
                    ExistingId startNodeExistingId = idGenerator.randomExisting(randoms);
                    Group startNodeGroup = groups.groupOf(startNodeExistingId.nodeIndex);
                    ExistingId endNodeExistingId = idGenerator.randomExisting(randoms);
                    Group endNodeGroup = groups.groupOf(endNodeExistingId.nodeIndex);

                    // miss some
                    Object startNode = idGenerator.miss(randoms, startNodeExistingId.id, 0.001f);
                    Object endNode = idGenerator.miss(randoms, endNodeExistingId.id, 0.001f);

                    if (!inputIdGenerator.isMiss(startNode) && !inputIdGenerator.isMiss(endNode)) {
                        relationshipCount.increment();
                        propertyCount.add(thisPropertyCount);
                    }
                    visitor.startId(startNode, startNodeGroup);
                    visitor.endId(endNode, endNodeGroup);

                    String type = InputIdGenerator.randomType(randoms);
                    if (randoms.nextFloat() < 0.00005) {
                        // Let there be a small chance of introducing a one-off relationship
                        // with a type that no, or at least very few, other relationships have.
                        type += "_odd";
                    }
                    visitor.type(type);
                },
                0);
    }

    private static InputIterable nodes(
            final long randomSeed,
            final long count,
            int batchSize,
            final InputIdGenerator inputIdGenerator,
            final IdGroupDistribution groups,
            LongAdder propertyCount) {
        return () -> new GeneratingInputIterator<>(
                count,
                batchSize,
                new RandomsStates(randomSeed),
                (randoms, visitor, id) -> {
                    Object nodeId = inputIdGenerator.nextNodeId(randoms, id);
                    Group group = groups.groupOf(id);
                    visitor.id(nodeId, group);
                    propertyCount.add(randomProperties(randoms, uniqueId(group, nodeId), visitor));
                    visitor.labels(randoms.selection(TOKENS, 0, TOKENS.length, true));
                },
                0);
    }

    private static final String[] TOKENS = {"token1", "token2", "token3", "token4", "token5", "token6", "token7"};

    private static int randomProperties(RandomValues randoms, Object id, InputEntityVisitor visitor) {
        String[] keys = randoms.selection(TOKENS, 0, TOKENS.length, false);
        for (String key : keys) {
            visitor.property(key, randoms.nextValue().asObject());
        }
        visitor.property("id", id);
        return keys.length + 1 /*the 'id' property*/;
    }

    private static class CapturingMonitor implements ExecutionMonitor {
        private final ExecutionMonitor delegate;
        private String additionalInformation;

        CapturingMonitor(ExecutionMonitor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void initialize(DependencyResolver dependencyResolver) {
            delegate.initialize(dependencyResolver);
        }

        @Override
        public void start(StageExecution execution) {
            delegate.start(execution);
        }

        @Override
        public void end(StageExecution execution, long totalTimeMillis) {
            delegate.end(execution, totalTimeMillis);
        }

        @Override
        public void done(boolean successful, long totalTimeMillis, String additionalInformation) {
            this.additionalInformation = additionalInformation;
            delegate.done(successful, totalTimeMillis, additionalInformation);
        }

        @Override
        public long checkIntervalMillis() {
            return delegate.checkIntervalMillis();
        }

        @Override
        public void check(StageExecution execution) {
            delegate.check(execution);
        }
    }
}

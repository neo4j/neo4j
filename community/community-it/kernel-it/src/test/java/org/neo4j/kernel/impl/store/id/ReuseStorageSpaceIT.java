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
package org.neo4j.kernel.impl.store.id;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.ProcessUtils.start;
import static org.neo4j.test.Race.throwing;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.description.Description;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.RandomValues;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
@Timeout(value = 20, unit = MINUTES)
class ReuseStorageSpaceIT {
    // Data size control center
    private static final int DATA_SIZE_PER_TRANSACTION = 10;
    private static final int CREATION_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int NUMBER_OF_TRANSACTIONS_PER_THREAD = 100;

    private static final int CUSTOM_EXIT_CODE = 99;
    private static final String[] TOKENS = {"One", "Two", "Three", "Four", "Five", "Six"};

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomSupport random;

    @Test
    void shouldReuseStorageSpaceWhenCreatingDeletingAndRestarting() throws Exception {
        shouldReuseStorageSpace(Operation.CREATE_DELETE, Operation.CREATE_DELETE, ReuseStorageSpaceIT::sameProcess);
    }

    @Test
    void shouldReuseStorageSpaceWhenDeletingCreatingAndRestarting() throws Exception {
        shouldReuseStorageSpace(Operation.CREATE, Operation.DELETE_CREATE, ReuseStorageSpaceIT::sameProcess);
    }

    @Test
    void shouldReuseStorageSpaceWhenCreatingDeletingAndCrashing() throws Exception {
        shouldReuseStorageSpace(
                Operation.CREATE_DELETE, Operation.CREATE_DELETE, ReuseStorageSpaceIT::crashingChildProcess);
    }

    @Test
    void shouldReuseStorageSpaceWhenDeletingCreatingAndCrashing() throws Exception {
        shouldReuseStorageSpace(Operation.CREATE, Operation.DELETE_CREATE, ReuseStorageSpaceIT::crashingChildProcess);
    }

    @Test
    void shouldPrioritizeFreelistWhenConcurrentlyAllocating() throws Exception {
        DatabaseManagementService dbms = new TestDatabaseManagementServiceBuilder(directory.homePath())
                // This test specifically exercises the ID caches and refilling of those as it goes, so the smaller the
                // better for this test
                .setConfig(GraphDatabaseInternalSettings.force_small_id_cache, true)
                .build();
        try {
            // given
            GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
            int numNodes = 40_000;
            MutableLongSet nodeIds = createNodes(db, numNodes);
            try (Transaction tx = db.beginTx()) {
                nodeIds.forEach(nodeId -> tx.getNodeById(nodeId).delete());
                tx.commit();
            }
            db.getDependencyResolver().resolveDependency(IdController.class).maintenance();

            // First create 40,000 nodes, then delete them, ensure ID maintenance has run and allocate concurrently
            int numThreads = 4;
            Collection<Callable<MutableLongSet>> allocators = new ArrayList<>();
            for (int i = 0; i < numThreads; i++) {
                allocators.add(() -> createNodes(db, numNodes / numThreads));
            }
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            List<Future<MutableLongSet>> results = executor.invokeAll(allocators);
            MutableLongSet reallocatedNodeIds = LongSets.mutable.withInitialCapacity(numNodes);
            for (Future<MutableLongSet> result : results) {
                reallocatedNodeIds.addAll(result.get());
            }
            assertThat(reallocatedNodeIds).as(diff(nodeIds, reallocatedNodeIds)).isEqualTo(nodeIds);
        } finally {
            dbms.shutdown();
        }
    }

    private Description diff(MutableLongSet nodeIds, MutableLongSet reallocatedNodeIds) {
        return new Description() {
            @Override
            public String value() {
                StringBuilder builder = new StringBuilder();
                nodeIds.forEach(nodeId -> {
                    if (!reallocatedNodeIds.contains(nodeId)) {
                        builder.append(format("%n<%d", nodeId));
                    }
                });
                reallocatedNodeIds.forEach(nodeId -> {
                    if (!nodeIds.contains(nodeId)) {
                        builder.append(format("%n>%d", nodeId));
                    }
                });
                return builder.toString();
            }
        };
    }

    private MutableLongSet createNodes(GraphDatabaseAPI db, int numNodes) {
        MutableLongSet nodeIds = LongSets.mutable.withInitialCapacity(numNodes);
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < numNodes; i++) {
                Node node = tx.createNode();
                nodeIds.add(node.getId());
            }
            tx.commit();
        }
        return nodeIds;
    }

    private void shouldReuseStorageSpace(Operation initialState, Operation operation, Launcher launcher)
            throws Exception {
        // given the data inserted into a db and knowledge about its size
        Path storeDirectory = directory.homePath();
        long seed = random.seed();
        Sizes initialStoreSizes = withDb(storeDirectory, db -> initialState.perform(db, seed));

        // when going into a loop deleting, re-creating, crashing and recovering that db
        for (int i = 0; i < 3; i++) {
            Pair<Integer, Sizes> result = launcher.launch(storeDirectory, seed, operation);
            assertEquals(CUSTOM_EXIT_CODE, result.getLeft());
            Sizes storeFileSizesNow = result.getRight();

            Sizes diff = storeFileSizesNow.diffAgainst(initialStoreSizes);
            long storeFilesDiff = diff.sum();
            int round = i;
            assertEquals(
                    0,
                    storeFilesDiff,
                    () -> format(
                            "Initial sizes %s%n%nStore sizes after operation (round %d)%s%n%nDiff between the two above %s%n",
                            initialStoreSizes, round, storeFileSizesNow, diff));
        }
    }

    private static Pair<Integer, Sizes> sameProcess(Path storeDirectory, long seed, Operation operation) {
        return Pair.of(CUSTOM_EXIT_CODE, withDb(storeDirectory, db -> operation.perform(db, seed)));
    }

    private static Pair<Integer, Sizes> crashingChildProcess(Path storeDirectory, long seed, Operation operation)
            throws Exception {
        // See "main" method in this class
        var process = start(
                ReuseStorageSpaceIT.class.getCanonicalName(),
                storeDirectory.toAbsolutePath().toString(),
                String.valueOf(seed),
                operation.name());

        // then storage size should be comparable (the store part, not the logs and all that)
        int exitCode = process.waitFor();
        Sizes storeFileSizes = withDb(storeDirectory, db -> {});
        return Pair.of(exitCode, storeFileSizes);
    }

    /**
     * This test spawns sub processes and kills them. This is their main method.
     */
    public static void main(String[] args) {
        Path storeDirectory = Path.of(args[0]).toAbsolutePath();
        long seed = Long.parseLong(args[1]);
        Operation operation = Operation.valueOf(args[2]);
        withDb(storeDirectory, db -> {
            operation.perform(db, seed);
            System.exit(CUSTOM_EXIT_CODE); // <-- so that we know that we got to the crash correctly
        });
    }

    private static Sizes withDb(Path storeDir, ThrowingConsumer<GraphDatabaseAPI, Exception> transaction) {
        DatabaseManagementService dbms = new TestDatabaseManagementServiceBuilder(storeDir)
                // This test specifically exercises the ID caches and refilling of those as it goes, so the smaller the
                // better for this test
                .setConfig(GraphDatabaseInternalSettings.force_small_id_cache, true)
                .setConfig(GraphDatabaseInternalSettings.strictly_prioritize_id_freelist, true)
                .build();
        try {
            GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
            transaction.accept(db);
            return new Sizes(db);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            dbms.shutdown();
        }
    }

    /**
     * The seed will make this method create the same data every time.
     * @param db {@link GraphDatabaseService} db.
     * @param seed starting seed for the randomness.
     */
    private static void createStuff(GraphDatabaseService db, long seed) {
        Race race = new Race();
        AtomicLong createdNodes = new AtomicLong();
        AtomicLong createdRelationships = new AtomicLong();
        AtomicLong nextSeed = new AtomicLong(seed);
        race.addContestants(
                CREATION_THREADS,
                throwing(() -> {
                    RandomValues random = RandomValues.create(new Random(nextSeed.getAndIncrement()));
                    int nodeCount = 0;
                    int relationshipCount = 0;
                    for (int t = 0; t < NUMBER_OF_TRANSACTIONS_PER_THREAD; t++) {
                        try (Transaction tx = db.beginTx()) {
                            // Nodes
                            Node[] nodes = new Node[DATA_SIZE_PER_TRANSACTION];
                            for (int n = 0; n < nodes.length; n++) {
                                Node node = nodes[n] =
                                        tx.createNode(labels(random.selection(TOKENS, 0, TOKENS.length, false)));
                                setProperties(random, node);
                                nodeCount++;
                            }

                            // Relationships
                            for (int r = 0; r < nodes.length; r++) {
                                Relationship relationship = random.among(nodes)
                                        .createRelationshipTo(random.among(nodes), withName(random.among(TOKENS)));
                                setProperties(random, relationship);
                                relationshipCount++;
                            }
                            tx.commit();
                        }
                    }
                    createdNodes.addAndGet(nodeCount);
                    createdRelationships.addAndGet(relationshipCount);
                }),
                1);
        race.goUnchecked();
    }

    /**
     * Deletes all nodes and relationships and their associated properties from the db, leaving the db effectively empty.
     */
    private static void deleteStuff(GraphDatabaseService db) {
        batchedDelete(db, Transaction::getAllRelationships, Relationship::delete);
        batchedDelete(db, Transaction::getAllNodes, Node::delete);
    }

    private static <ENTITY> void batchedDelete(
            GraphDatabaseService db,
            Function<Transaction, ResourceIterable<ENTITY>> provider,
            Consumer<ENTITY> deleter) {
        int deleted;
        do {
            deleted = 0;
            try (Transaction tx = db.beginTx();
                    ResourceIterable<ENTITY> entities = provider.apply(tx);
                    ResourceIterator<ENTITY> iterator = entities.iterator()) {
                for (; iterator.hasNext() && deleted < 10_000; deleted++) {
                    ENTITY entity = iterator.next();
                    deleter.accept(entity);
                }
                tx.commit();
            }
        } while (deleted > 0);
    }

    private static void setProperties(RandomValues random, Entity entity) {
        for (String propertyKey : random.selection(TOKENS, 0, TOKENS.length, false)) {
            entity.setProperty(propertyKey, random.nextValue().asObject());
        }
    }

    private static Label[] labels(String[] names) {
        Label[] labels = new Label[names.length];
        for (int i = 0; i < names.length; i++) {
            labels[i] = label(names[i]);
        }
        return labels;
    }

    private static class Sizes {
        private final Map<String, Long> sizes;

        Sizes(GraphDatabaseAPI db) {
            sizes = new HashMap<>();
            IdGeneratorFactory idGeneratorFactory =
                    db.getDependencyResolver().resolveDependency(IdGeneratorFactory.class);
            idGeneratorFactory.visit(idGenerator -> {
                if (idGenerator.hasOnlySingleIds()) {
                    sizes.put(idGenerator.idType().name(), idGenerator.getHighId());
                }
                // Otherwise there's no way we can guarantee perfect ID reuse
            });
        }

        private Sizes(Map<String, Long> sizes) {
            this.sizes = sizes;
        }

        Sizes diffAgainst(Sizes other) {
            Map<String, Long> diff = new HashMap<>();
            for (Map.Entry<String, Long> entry : sizes.entrySet()) {
                Long otherSize = other.sizes.get(entry.getKey());
                if (otherSize != null) {
                    long diffSize = entry.getValue() - otherSize;
                    if (diffSize != 0) {
                        diff.put(entry.getKey(), diffSize);
                    }
                }
            }
            return new Sizes(diff);
        }

        @Override
        public String toString() {
            List<Map.Entry<String, Long>> nonEmptyEntries = sizes.entrySet().stream()
                    .filter(e -> e.getValue() != 0)
                    .sorted(Map.Entry.comparingByKey())
                    .toList();
            long sum = sum();
            return format(
                    "SUM %s(%d):%n%s",
                    ByteUnit.bytesToString(sum), sum, StringUtils.join(nonEmptyEntries, format("%n")));
        }

        long sum() {
            return sum(all -> true);
        }

        long sum(Predicate<String> filter) {
            return sizes.entrySet().stream()
                    .filter(e -> filter.test(e.getKey()))
                    .mapToLong(Map.Entry::getValue)
                    .sum();
        }
    }

    private enum Operation {
        CREATE {
            @Override
            void perform(GraphDatabaseAPI db, long seed) {
                createStuff(db, seed);
            }
        },
        CREATE_DELETE {
            @Override
            public void perform(GraphDatabaseAPI db, long seed) {
                createStuff(db, seed);
                deleteStuff(db);
            }
        },
        DELETE_CREATE {
            @Override
            public void perform(GraphDatabaseAPI db, long seed) throws InterruptedException {
                deleteStuff(db);
                db.getDependencyResolver().resolveDependency(IdController.class).maintenance();
                createStuff(db, seed);
            }
        };

        abstract void perform(GraphDatabaseAPI db, long seed) throws Exception;
    }

    interface Launcher {
        Pair<Integer, Sizes> launch(Path storeDirectory, long seed, Operation operation) throws Exception;
    }
}

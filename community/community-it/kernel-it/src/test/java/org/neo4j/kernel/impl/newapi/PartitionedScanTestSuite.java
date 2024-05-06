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
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.WorkerContext;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.newapi.PartitionedScanFactories.PartitionedScanFactory;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.Query;
import org.neo4j.kernel.impl.newapi.TestUtils.PartitionedScanAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.Tokens;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith({SoftAssertionsExtension.class, RandomExtension.class})
@ImpermanentDbmsExtension
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class PartitionedScanTestSuite<QUERY extends Query<?>, SESSION, CURSOR extends Cursor> {
    @Inject
    private GraphDatabaseService db;

    @Inject
    protected RandomSupport random;

    @InjectSoftAssertions
    protected SoftAssertions softly;

    @Inject
    protected StorageEngine storageEngine;

    @Inject
    protected Kernel kernel;

    abstract Queries<QUERY> setupDatabase();

    protected Queries<QUERY> queries;
    protected int maxNumberOfPartitions;
    protected PartitionedScanFactory<QUERY, SESSION, CURSOR> factory;

    PartitionedScanTestSuite(TestSuite<QUERY, SESSION, CURSOR> testSuite) {
        factory = testSuite.getFactory();
    }

    @BeforeAll
    protected void setup() {
        // given  setting up the database
        // when   the queries and expected matches are generated
        queries = setupDatabase();
        // then   require there to be some queries to test against
        assumeThat(queries.valid())
                .as("there are valid queries to test against")
                .isNotEmpty();

        maxNumberOfPartitions = calculateMaxNumberOfPartitions(queries.valid().queries());
    }

    protected final KernelTransaction beginTx() {
        return ((TransactionImpl) db.beginTx()).kernelTransaction();
    }

    @Test
    final void shouldThrowWithEntityTypeComplementSeekOrScan() throws KernelException {
        try (var tx = beginTx()) {
            final var query = getFirstValidQuery();

            // given  a read session with a mismatched entity type to the seek/scan
            // when   partitioned scan constructed
            // then   IndexNotApplicableKernelException should be thrown
            softly.assertThatThrownBy(
                            () -> factory.getEntityTypeComplimentFactory()
                                    .partitionedScan(
                                            tx, factory.getSession(tx, query.indexName()), Integer.MAX_VALUE, query),
                            "should throw with mismatched entity type seek/scan method, and given index session")
                    .isInstanceOf(IndexNotApplicableKernelException.class)
                    .hasMessageContaining("can not be performed on index");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    final void shouldThrowWithNonPositivePartitions(int desiredNumberOfPartitions) throws KernelException {
        try (var tx = beginTx()) {
            // given  an invalid desiredNumberOfPartitions
            // when   partitioned scan constructed
            // then   IllegalArgumentException should be thrown
            softly.assertThatThrownBy(
                            () -> factory.partitionedScan(tx, desiredNumberOfPartitions, getFirstValidQuery()),
                            "desired number of partitions must be positive")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContainingAll("Expected positive", "value");
        }
    }

    @Test
    final void shouldThrowOnConstructionWithTransactionState() throws KernelException {
        try (var tx = beginTx()) {
            // given  transaction state
            createState(tx);
            softly.assertThat(tx.dataRead().transactionStateHasChanges())
                    .as("transaction state")
                    .isTrue();

            // when   partitioned scan constructed
            // then   IllegalStateException should be thrown
            softly.assertThatThrownBy(
                            () -> factory.partitionedScan(tx, Integer.MAX_VALUE, getFirstValidQuery()),
                            "should throw on construction of scan, with transaction state")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Transaction contains changes; PartitionScan is only valid in Read-Only transactions.");
        }
    }

    @Test
    final void shouldThrowWithInvalidQuery() throws KernelException {
        assumeThat(queries.invalid())
                .as("there are invalid queries to test against")
                .isNotEmpty();

        try (var tx = beginTx()) {
            for (final var query : queries.invalid()) {
                // given  an invalid query
                // when   partitioned scan constructed
                // then   IndexNotApplicableKernelException should be thrown
                softly.assertThatThrownBy(
                                () -> factory.partitionedScan(tx, Integer.MAX_VALUE, query),
                                "should throw with an invalid query")
                        .isInstanceOf(IndexNotApplicableKernelException.class)
                        .hasMessageContaining("This index does not support partitioned scan for this query");
            }
        }
    }

    abstract static class WithoutData<QUERY extends Query<?>, SESSION, CURSOR extends Cursor>
            extends PartitionedScanTestSuite<QUERY, SESSION, CURSOR> {
        WithoutData(TestSuite<QUERY, SESSION, CURSOR> testSuite) {
            super(testSuite);
        }

        @ParameterizedTest
        @EnumSource(PartitionedScanAPI.class)
        final void shouldHandleEmptyDatabase(PartitionedScanAPI api) throws KernelException {
            try (var tx = beginTx();
                    var entities = factory.getCursor(tx.cursors()).with(tx.cursorContext());
                    var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext()) {
                for (final var entry : queries.valid()) {
                    final var query = entry.getKey();
                    // given  an empty database
                    // when   scanning
                    final var scan = factory.partitionedScan(tx, Integer.MAX_VALUE, query);
                    while (api.reservePartition(scan, entities, tx, executionContext)) {
                        // then   no data should be found, and should not throw
                        softly.assertThat(entities.next())
                                .as("no data should be found for %s", query)
                                .isFalse();
                    }
                }

                executionContext.complete();
            }
        }
    }

    abstract static class WithData<QUERY extends Query<?>, SESSION, CURSOR extends Cursor>
            extends PartitionedScanTestSuite<QUERY, SESSION, CURSOR> {
        WithData(TestSuite<QUERY, SESSION, CURSOR> testSuite) {
            super(testSuite);
        }

        @Override
        @BeforeAll
        protected void setup() {
            // given  setting up the database
            // when   the maximum number of partitions is calculated
            super.setup();
            // then   there should be at least enough to test partitioning
            assumeThat(maxNumberOfPartitions)
                    .as("max number of partitions is enough to test partitions")
                    .isGreaterThan(1);
        }

        @ParameterizedTest
        @EnumSource(PartitionedScanAPI.class)
        final void shouldScanSubsetOfEntriesWithSinglePartition(PartitionedScanAPI api) throws KernelException {
            try (var tx = beginTx();
                    var entities = factory.getCursor(tx.cursors()).with(tx.cursorContext());
                    var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext()) {
                for (final var entry : queries.valid()) {
                    final var query = entry.getKey();
                    final var expectedMatches = entry.getValue();

                    // given  a database with entries
                    // when   partitioning the scan
                    final var scan = factory.partitionedScan(tx, maxNumberOfPartitions, query);

                    // then   the number of partitions can be less, but no more than the max number of partitions
                    softly.assertThat(scan.getNumberOfPartitions())
                            .as("number of partitions")
                            .isGreaterThan(0)
                            .isLessThanOrEqualTo(maxNumberOfPartitions);

                    // given  a partition
                    final var found = new HashSet<Long>();
                    api.reservePartition(scan, entities, tx, executionContext);
                    while (entities.next()) {
                        // when   inspecting the found entities
                        // then   there should be no duplicates
                        softly.assertThat(found.add(factory.getEntityReference(entities)))
                                .as("no duplicate")
                                .isTrue();
                    }

                    // then   the entities found should be a subset of all entities that would have matched that query
                    if (!expectedMatches.containsAll(found)) {
                        // only use softly if we see that there's a mismatch because the call is absurdly ultra slow
                        softly.assertThat(expectedMatches)
                                .as("subset of all matches for %s", query)
                                .containsAll(found);
                    }
                }

                executionContext.complete();
            }
        }

        @ParameterizedTest
        @EnumSource(PartitionedScanAPI.class)
        final void shouldCreateNoMorePartitionsThanPossible(PartitionedScanAPI api) throws KernelException {
            singleThreadedCheck(api, Integer.MAX_VALUE);
        }

        @ParameterizedTest(name = "desiredNumberOfPartitions={0}")
        @MethodSource("rangeFromOneToMaxPartitions")
        final void shouldScanAllEntriesWithGivenNumberOfPartitionsSingleThreaded(int desiredNumberOfPartitions)
                throws KernelException {
            singleThreadedCheck(PartitionedScanAPI.NEW, desiredNumberOfPartitions);
        }

        @ParameterizedTest(name = "desiredNumberOfPartitions={0}")
        @MethodSource("rangeFromOneToMaxPartitions")
        final void shouldScanMultiplePartitionsInParallelWithSameNumberOfThreads(int desiredNumberOfPartitions)
                throws KernelException {
            multiThreadedCheck(desiredNumberOfPartitions, desiredNumberOfPartitions);
        }

        @ParameterizedTest(name = "desiredNumberOfThreads={0}")
        @MethodSource("rangeFromOneToMaxPartitions")
        final void shouldScanMultiplePartitionsInParallelWithFewerThreads(int desiredNumberOfTheads)
                throws KernelException {
            multiThreadedCheck(maxNumberOfPartitions, desiredNumberOfTheads);
        }

        private void singleThreadedCheck(PartitionedScanAPI api, int desiredNumberOfPartitions) throws KernelException {
            try (var tx = beginTx();
                    var entities = factory.getCursor(tx.cursors()).with(tx.cursorContext());
                    var statement = tx.acquireStatement();
                    var executionContext = tx.createExecutionContext()) {
                for (final var entry : queries.valid()) {
                    final var query = entry.getKey();
                    final var expectedMatches = entry.getValue();

                    // given  a database with entries
                    // when   partitioning the scan
                    final var scan = factory.partitionedScan(tx, desiredNumberOfPartitions, query);

                    // then   the number of partitions can be less, but no more than the desired number of partitions
                    softly.assertThat(scan.getNumberOfPartitions())
                            .as("number of partitions")
                            .isGreaterThan(0)
                            .isLessThanOrEqualTo(desiredNumberOfPartitions)
                            .isLessThanOrEqualTo(maxNumberOfPartitions);

                    // given  each partition
                    final var found = new HashSet<Long>();
                    while (api.reservePartition(scan, entities, tx, executionContext)) {
                        while (entities.next()) {
                            // when   inspecting the found entities
                            // then   there should be no duplicates
                            softly.assertThat(found.add(factory.getEntityReference(entities)))
                                    .as("no duplicate")
                                    .isTrue();
                        }
                    }

                    // then   all the entities with matching the query should be found
                    if (!expectedMatches.equals(found)) {
                        // only use softly if we see that there's a mismatch because the call is absurdly ultra slow
                        softly.assertThat(found)
                                .as("only the expected data found matching %s", query)
                                .containsExactlyInAnyOrderElementsOf(expectedMatches);
                    }
                }

                executionContext.complete();
            }
        }

        private void multiThreadedCheck(int desiredNumberOfPartitions, int numberOfThreads) throws KernelException {
            try (var tx = beginTx()) {
                for (final var entry : queries.valid()) {
                    final var query = entry.getKey();
                    final var expectedMatches = entry.getValue();

                    // given  a database with entries
                    // when   partitioning the scan
                    final var scan = factory.partitionedScan(tx, desiredNumberOfPartitions, query);

                    // then   the number of partitions can be less, but no more than the desired number of partitions
                    softly.assertThat(scan.getNumberOfPartitions())
                            .as("number of partitions")
                            .isGreaterThan(0)
                            .isLessThanOrEqualTo(desiredNumberOfPartitions)
                            .isLessThanOrEqualTo(maxNumberOfPartitions);

                    // given  each partition distributed over multiple threads
                    final var allFound = Collections.synchronizedSet(new HashSet<Long>());
                    final var workerContexts =
                            TestUtils.createContexts(tx, factory.getCursor(kernel.cursors())::with, numberOfThreads);
                    final var race = new Race();
                    for (final var workerContext : workerContexts) {
                        race.addContestant(() -> {
                            final var executionContext = workerContext.getContext();
                            try (var entities = workerContext.getCursor()) {
                                final var found = new HashSet<Long>();
                                while (scan.reservePartition(entities, executionContext)) {
                                    while (entities.next()) {
                                        // when   inspecting the found entities
                                        // then   there should be no duplicates within the partition
                                        softly.assertThat(found.add(factory.getEntityReference(entities)))
                                                .as("no duplicate")
                                                .isTrue();
                                    }
                                }

                                // then   there should be no duplicates amongst any of the partitions
                                found.forEach(s -> softly.assertThat(allFound.add(s))
                                        .as("no duplicates")
                                        .isTrue());
                            } finally {
                                executionContext.complete();
                            }
                        });
                    }
                    race.goUnchecked();
                    workerContexts.forEach(WorkerContext::close);

                    // then   all the entities with matching the query should be found
                    if (!expectedMatches.equals(allFound)) {
                        // only use softly if we see that there's a mismatch because the call is absurdly ultra slow
                        softly.assertThat(allFound)
                                .as("only the expected data found matching %s", query)
                                .containsExactlyInAnyOrderElementsOf(expectedMatches);
                    }
                }
            }
        }

        protected IntStream rangeFromOneToMaxPartitions() {
            return IntStream.rangeClosed(1, maxNumberOfPartitions);
        }
    }

    private QUERY getFirstValidQuery() {
        return queries.valid().iterator().next().getKey();
    }

    protected String getTokenIndexName(EntityType entityType) {
        try (var tx = beginTx()) {
            final var indexes = tx.schemaRead().index(SchemaDescriptors.forAnyEntityTokens(entityType));
            assumeThat(indexes.hasNext())
                    .as("%s based token index exists", entityType)
                    .isTrue();
            final var index = indexes.next();
            assumeThat(indexes.hasNext())
                    .as("only one %s based token index exists", entityType)
                    .isFalse();
            return index.getName();
        } catch (Exception e) {
            throw new AssertionError(String.format("failed to get %s based token index", entityType), e);
        }
    }

    protected void createIndexes(Iterable<IndexPrototype> indexPrototypes) {
        try (var tx = beginTx()) {
            final var schemaWrite = tx.schemaWrite();
            for (final var indexPrototype : indexPrototypes) {
                schemaWrite.indexCreate(indexPrototype);
            }
            tx.commit();
        } catch (Exception e) {
            throw new AssertionError("failed to create indexes", e);
        }

        try (var tx = beginTx()) {
            new SchemaImpl(tx).awaitIndexesOnline(1, TimeUnit.HOURS);
        } catch (Exception e) {
            throw new AssertionError("failed waiting for indexes to come online", e);
        }
    }

    protected int calculateMaxNumberOfPartitions(Iterable<QUERY> queries) {
        try (var tx = beginTx()) {
            var maxNumberOfPartitions = 0;
            for (final var query : queries) {
                maxNumberOfPartitions = Math.max(
                        maxNumberOfPartitions,
                        factory.partitionedScan(tx, Integer.MAX_VALUE, query).getNumberOfPartitions());
            }
            return maxNumberOfPartitions;
        } catch (Exception e) {
            throw new AssertionError("failed to calculated max number of partitions", e);
        }
    }

    private static void createState(KernelTransaction tx) throws InvalidTransactionTypeKernelException {
        tx.dataWrite().nodeCreate();
    }

    protected record Queries<QUERY extends Query<?>>(EntityIdsMatchingQuery<QUERY> valid, Set<QUERY> invalid) {
        public Queries(EntityIdsMatchingQuery<QUERY> valid, Set<QUERY> invalid) {
            this.valid = valid;
            this.invalid = Collections.unmodifiableSet(invalid);
        }

        public Queries(EntityIdsMatchingQuery<QUERY> valid) {
            this(valid, Set.of());
        }
    }

    protected static final class EntityIdsMatchingQuery<QUERY extends Query<?>>
            implements Iterable<Map.Entry<QUERY, Set<Long>>> {
        private final Map<QUERY, Set<Long>> matches = new HashMap<>();

        static <QUERY extends Query<?>>
                Collector<QUERY, EntityIdsMatchingQuery<QUERY>, EntityIdsMatchingQuery<QUERY>> collector() {
            return Collector.of(
                    EntityIdsMatchingQuery<QUERY>::new,
                    EntityIdsMatchingQuery::getOrCreate,
                    EntityIdsMatchingQuery::addAll);
        }

        Set<Long> getOrCreate(QUERY query) {
            return matches.computeIfAbsent(query, q -> new HashSet<>());
        }

        Set<Long> addOrReplace(QUERY query, Set<Long> entityIds) {
            return matches.put(query, entityIds);
        }

        EntityIdsMatchingQuery<QUERY> addAll(EntityIdsMatchingQuery<QUERY> other) {
            matches.putAll(other.matches);
            return this;
        }

        Set<QUERY> queries() {
            return Collections.unmodifiableMap(matches).keySet();
        }

        @Override
        public Iterator<Map.Entry<QUERY, Set<Long>>> iterator() {
            return Collections.unmodifiableMap(matches).entrySet().iterator();
        }
    }

    protected record Range(long min, long max) {
        boolean contains(long value) {
            return min <= value && value < max;
        }

        long quantile(long n, long q) {
            assumeThat(n).as("given numbered quantile, is a valid quantile").isBetween(0L, q);
            return min + n * (max - min) / q;
        }

        long random(Random random) {
            return random.nextLong(min, max);
        }

        long randomBetweenQuantiles(Random random, long n, long m, long q) {
            return createSane(quantile(n, q), quantile(m, q)).random(random);
        }

        static Range createSane(long x, long y) {
            return x < y ? new Range(x, y) : new Range(y, x);
        }

        static Range union(Range lhs, Range rhs) {
            if (lhs == null) {
                return rhs;
            }
            if (rhs == null) {
                return lhs;
            }
            return new Range(Math.min(lhs.min, rhs.min), Math.max(lhs.max, rhs.max));
        }

        static boolean strictlyLessThan(Range lhs, Range rhs) {
            return lhs.min <= lhs.max && rhs.min <= rhs.max && lhs.max <= rhs.min;
        }
    }

    protected final <TOKEN> int createToken(Tokens.Suppliers.Supplier<TOKEN> token) {
        final int tokenId;
        try (var tx = beginTx()) {
            tokenId = token.getId(tx);
            tx.commit();
        } catch (KernelException e) {
            throw new AssertionError(String.format("failed to create %ss in database", token.name()), e);
        }
        return tokenId;
    }

    protected final <TOKEN> int[] createTokens(int numberOfTags, Tokens.Suppliers.Supplier<TOKEN> token) {
        final int[] tokenIds;
        try (var tx = beginTx()) {
            tokenIds = token.getIds(tx, numberOfTags);
            tx.commit();
        } catch (KernelException e) {
            throw new AssertionError(String.format("failed to create %ss in database", token.name()), e);
        }
        return tokenIds;
    }

    protected interface Query<QUERY> {
        String indexName();

        QUERY get();
    }

    interface TestSuite<QUERY extends Query<?>, SESSION, CURSOR extends Cursor> {
        PartitionedScanFactory<QUERY, SESSION, CURSOR> getFactory();
    }
}

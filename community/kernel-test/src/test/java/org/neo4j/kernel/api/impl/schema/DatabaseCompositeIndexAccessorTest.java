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
package org.neo4j.kernel.api.impl.schema;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.collection.PrimitiveLongCollections.toSet;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.SIMPLE_NAME_LOOKUP;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracker.NO_USAGE_TRACKER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.extension.Threading.waitingWhileIn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.assertj.core.api.Condition;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.common.EmptyDependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexQueryHelper;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.AbstractIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.kernel.impl.index.schema.RangeIndexProviderFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Threading;
import org.neo4j.test.extension.ThreadingExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.token.TokenHolders;

@EphemeralPageCacheExtension
@ExtendWith(ThreadingExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DatabaseCompositeIndexAccessorTest {
    private static final int PROP_ID1 = 1;
    private static final int PROP_ID2 = 2;
    private static final Config CONFIG = Config.defaults();
    private static final IndexSamplingConfig SAMPLING_CONFIG = new IndexSamplingConfig(CONFIG);
    private static final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Inject
    private Threading threading;

    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;

    private final long nodeId = 1;
    private final long nodeId2 = 2;
    private final Object[] values = {"value1", "values2"};
    private final Object[] values2 = {40, 42};
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;
    private static final IndexPrototype SCHEMA_INDEX_DESCRIPTOR =
            IndexPrototype.forSchema(SchemaDescriptors.forLabel(0, PROP_ID1, PROP_ID2));
    private static final IndexPrototype UNIQUE_SCHEMA_INDEX_DESCRIPTOR =
            IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(1, PROP_ID1, PROP_ID2));
    private final JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();

    private Iterable<IndexProvider> providers;

    @BeforeAll
    public void prepareProviders() throws IOException {
        dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        providers = getIndexProviders(pageCache, jobScheduler, fileSystem, testDirectory);
    }

    @AfterAll
    public void after() throws IOException {
        closeAll(dirFactory, jobScheduler);
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class CompositeTests {
        private List<IndexAccessor> indexAccessors() throws IOException {
            List<IndexAccessor> accessors = new ArrayList<>();
            for (IndexProvider p : providers) {
                accessors.add(indexAccessor(
                        p,
                        p.completeConfiguration(
                                SCHEMA_INDEX_DESCRIPTOR.withName("index_" + 0).materialise(0),
                                StorageEngineIndexingBehaviour.EMPTY)));
                accessors.add(indexAccessor(
                        p,
                        p.completeConfiguration(
                                UNIQUE_SCHEMA_INDEX_DESCRIPTOR
                                        .withName("constraint_" + 1)
                                        .materialise(1),
                                StorageEngineIndexingBehaviour.EMPTY)));
            }
            return accessors;
        }

        @ParameterizedTest
        @MethodSource("indexAccessors")
        void indexReaderShouldSupportScan(IndexAccessor accessor) throws Exception {
            // GIVEN
            try (accessor) {
                updateAndCommit(accessor, asList(add(nodeId, values), add(nodeId2, values2)));
                try (var reader = accessor.newValueReader(NO_USAGE_TRACKER)) {

                    // WHEN
                    Set<Long> results =
                            resultSet(reader, PropertyIndexQuery.exists(PROP_ID1), PropertyIndexQuery.exists(PROP_ID2));
                    Set<Long> results2 = resultSet(reader, exact(PROP_ID1, values[0]), exact(PROP_ID2, values[1]));

                    // THEN
                    assertEquals(asSet(nodeId, nodeId2), results);
                    assertEquals(asSet(nodeId), results2);
                }
            }
        }

        @ParameterizedTest
        @MethodSource("indexAccessors")
        void multipleIndexReadersFromDifferentPointsInTimeCanSeeDifferentResults(IndexAccessor accessor)
                throws Exception {
            // WHEN
            try (accessor) {
                updateAndCommit(accessor, singletonList(add(nodeId, values)));
                var firstReader = accessor.newValueReader(NO_USAGE_TRACKER);
                updateAndCommit(accessor, singletonList(add(nodeId2, values2)));
                var secondReader = accessor.newValueReader(NO_USAGE_TRACKER);

                // THEN
                assertEquals(
                        asSet(nodeId), resultSet(firstReader, exact(PROP_ID1, values[0]), exact(PROP_ID2, values[1])));
                assertThat(resultSet(firstReader, exact(PROP_ID1, values2[0]), exact(PROP_ID2, values2[1])))
                        .is(anyOf(
                                new Condition<>(s -> s.equals(asSet()), "empty set"),
                                new Condition<>(s -> s.equals(asSet(nodeId2)), "one element")));
                assertEquals(
                        asSet(nodeId), resultSet(secondReader, exact(PROP_ID1, values[0]), exact(PROP_ID2, values[1])));
                assertEquals(
                        asSet(nodeId2),
                        resultSet(secondReader, exact(PROP_ID1, values2[0]), exact(PROP_ID2, values2[1])));
                firstReader.close();
                secondReader.close();
            }
        }

        @ParameterizedTest
        @MethodSource("indexAccessors")
        void canAddNewData(IndexAccessor accessor) throws Exception {
            // WHEN
            try (accessor) {
                updateAndCommit(accessor, asList(add(nodeId, values), add(nodeId2, values2)));
                try (var reader = accessor.newValueReader(NO_USAGE_TRACKER)) {
                    assertEquals(
                            asSet(nodeId), resultSet(reader, exact(PROP_ID1, values[0]), exact(PROP_ID2, values[1])));
                }
            }
        }

        @ParameterizedTest
        @MethodSource("indexAccessors")
        void canChangeExistingData(IndexAccessor accessor) throws Exception {
            // GIVEN
            try (accessor) {
                updateAndCommit(accessor, singletonList(add(nodeId, values)));

                // WHEN
                updateAndCommit(accessor, singletonList(change(nodeId, values, values2)));
                try (var reader = accessor.newValueReader(NO_USAGE_TRACKER)) {
                    // THEN
                    assertEquals(
                            asSet(nodeId), resultSet(reader, exact(PROP_ID1, values2[0]), exact(PROP_ID2, values2[1])));
                    assertEquals(emptySet(), resultSet(reader, exact(PROP_ID1, values[0]), exact(PROP_ID2, values[1])));
                }
            }
        }

        @ParameterizedTest
        @MethodSource("indexAccessors")
        void canRemoveExistingData(IndexAccessor accessor) throws Exception {
            // GIVEN
            try (accessor) {
                updateAndCommit(accessor, asList(add(nodeId, values), add(nodeId2, values2)));

                // WHEN
                updateAndCommit(accessor, singletonList(remove(nodeId, values)));
                try (var reader = accessor.newValueReader(NO_USAGE_TRACKER)) {
                    // THEN
                    assertEquals(
                            asSet(nodeId2),
                            resultSet(reader, exact(PROP_ID1, values2[0]), exact(PROP_ID2, values2[1])));
                    assertEquals(asSet(), resultSet(reader, exact(PROP_ID1, values[0]), exact(PROP_ID2, values[1])));
                }
            }
        }

        @ParameterizedTest
        @MethodSource("indexAccessors")
        void shouldStopSamplingWhenIndexIsDropped(IndexAccessor accessor) throws Exception {
            // given
            try (accessor) {
                updateAndCommit(accessor, asList(add(nodeId, values), add(nodeId2, values2)));

                // when
                var indexReader =
                        accessor.newValueReader(NO_USAGE_TRACKER); // needs to be acquired before drop() is called
                IndexSampler indexSampler = indexReader.createSampler();

                AtomicBoolean droppedLatch = new AtomicBoolean();
                AtomicReference<Thread> dropper = new AtomicReference<>();
                Predicate<Thread> awaitCompletion = waitingWhileIn(TaskCoordinator.class, "awaitCompletion");
                Future<Void> drop = threading.execute(
                        nothing -> {
                            dropper.set(Thread.currentThread());
                            try {
                                accessor.drop();
                            } finally {
                                droppedLatch.set(true);
                            }
                            return null;
                        },
                        null);

                var e = assertThrows(IndexNotFoundKernelException.class, () -> {
                    try (var reader = indexReader /* do not inline! */;
                            IndexSampler sampler = indexSampler /* do not inline! */) {
                        while (!droppedLatch.get() && !awaitCompletion.test(dropper.get())) {
                            LockSupport.parkNanos(MILLISECONDS.toNanos(10));
                        }
                        sampler.sampleIndex(CursorContext.NULL_CONTEXT, new AtomicBoolean());
                    } finally {
                        drop.get();
                    }
                });
                assertThat(e).hasMessage("Index dropped while sampling.");
            }
        }
    }

    private static Iterable<IndexProvider> getIndexProviders(
            PageCache pageCache,
            JobScheduler jobScheduler,
            FileSystemAbstraction fileSystem,
            TestDirectory testDirectory) {
        Collection<AbstractIndexProviderFactory<?>> indexProviderFactories = List.of(new RangeIndexProviderFactory());
        var cacheTracer = NULL;
        return indexProviderFactories.stream()
                .map(f -> f.create(
                        pageCache,
                        fileSystem,
                        new SimpleLogService(logProvider),
                        new Monitors(),
                        CONFIG,
                        writable(),
                        HostedOnMode.SINGLE,
                        RecoveryCleanupWorkCollector.ignore(),
                        DatabaseLayout.ofFlat(testDirectory.homePath()),
                        new TokenHolders(null, null, null),
                        jobScheduler,
                        new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER),
                        cacheTracer,
                        EmptyDependencyResolver.EMPTY_RESOLVER))
                .collect(Collectors.toList());
    }

    private static IndexAccessor indexAccessor(IndexProvider provider, IndexDescriptor descriptor) throws IOException {
        IndexPopulator populator = provider.getPopulator(
                descriptor,
                SAMPLING_CONFIG,
                heapBufferFactory(1024),
                INSTANCE,
                SIMPLE_NAME_LOOKUP,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
        populator.create();
        populator.close(true, CursorContext.NULL_CONTEXT);

        return provider.getOnlineAccessor(
                descriptor,
                SAMPLING_CONFIG,
                SIMPLE_NAME_LOOKUP,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
    }

    private static Set<Long> resultSet(ValueIndexReader reader, PropertyIndexQuery... queries)
            throws IndexNotApplicableKernelException {
        try (NodeValueIterator results = new NodeValueIterator()) {
            reader.query(results, QueryContext.NULL_CONTEXT, unconstrained(), queries);
            return toSet(results);
        }
    }

    private static IndexEntryUpdate<?> add(long nodeId, Object... values) {
        return IndexQueryHelper.add(nodeId, SCHEMA_INDEX_DESCRIPTOR, values);
    }

    private static IndexEntryUpdate<?> remove(long nodeId, Object... values) {
        return IndexQueryHelper.remove(nodeId, SCHEMA_INDEX_DESCRIPTOR, values);
    }

    private static IndexEntryUpdate<?> change(long nodeId, Object[] valuesBefore, Object[] valuesAfter) {
        return IndexQueryHelper.change(nodeId, SCHEMA_INDEX_DESCRIPTOR, valuesBefore, valuesAfter);
    }

    private static void updateAndCommit(IndexAccessor accessor, List<IndexEntryUpdate<?>> nodePropertyUpdates)
            throws IndexEntryConflictException {
        try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
            for (IndexEntryUpdate<?> update : nodePropertyUpdates) {
                updater.process(update);
            }
        }
    }
}

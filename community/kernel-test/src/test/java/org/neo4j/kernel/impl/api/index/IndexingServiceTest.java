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
package org.neo4j.kernel.impl.api.index;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.neo4j.common.Subject.AUTH_DISABLED;
import static org.neo4j.common.Subject.SYSTEM;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.helpers.collection.Iterators.asCollection;
import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.internal.helpers.collection.Iterators.loop;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.api.TransactionVisibilityProvider.EMPTY_VISIBILITY_PROVIDER;
import static org.neo4j.kernel.impl.api.index.IndexSamplingMode.backgroundRebuildAll;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.RECOVERY;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.index.EntityRange;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictHandler;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.IndexUsageStats;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.DatabaseIndexStats;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.kernel.impl.index.schema.NodeIdsIndexReaderQueryAnswer;
import org.neo4j.kernel.impl.index.schema.PartitionedTokenScan;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.kernel.impl.scheduler.GroupedDaemonThreadFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.state.storeview.IndexStoreViewFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.schema.SimpleEntityTokenClient;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.test.Barrier;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.FakeClockJobScheduler;
import org.neo4j.test.InMemoryTokens;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.util.concurrent.BinaryLatch;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class IndexingServiceTest {
    private final LifeSupport life = new LifeSupport();
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    private final SchemaState schemaState = mock(SchemaState.class);
    private final int labelId = 7;
    private final int propertyKeyId = 15;
    private final int uniquePropertyKeyId = 15;
    private final IndexPrototype prototype = forSchema(forLabel(labelId, propertyKeyId))
            .withIndexProvider(PROVIDER_DESCRIPTOR)
            .withName("index");
    private final IndexDescriptor index = prototype.materialise(0);
    private final IndexPrototype uniqueIndex = uniqueForSchema(forLabel(labelId, uniquePropertyKeyId))
            .withIndexProvider(PROVIDER_DESCRIPTOR)
            .withName("constraint");
    private final IndexDescriptor tokenIndex = forSchema(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
            .withIndexProvider(PROVIDER_DESCRIPTOR)
            .withName("tokenIndex")
            .materialise(21);
    private final IndexPopulator populator = mock(IndexPopulator.class);
    private final IndexUpdater updater = mock(IndexUpdater.class);
    private final IndexProvider indexProvider =
            mock(IndexProvider.class, withSettings().name("testindexprovider"));
    private final IndexAccessor accessor = mock(IndexAccessor.class, RETURNS_MOCKS);
    private final IndexStoreView storeView = mock(IndexStoreView.class);
    private final IndexStoreViewFactory storeViewFactory = mock(IndexStoreViewFactory.class);
    private final InMemoryTokens nameLookup = new InMemoryTokens();
    private final AssertableLogProvider internalLogProvider = new AssertableLogProvider();
    private final IndexStatisticsStore indexStatisticsStore = mock(IndexStatisticsStore.class);
    private final JobScheduler scheduler = JobSchedulerFactory.createScheduler();
    private final StorageEngine storageEngine = mock(StorageEngine.class);
    private final FakeClock clock = Clocks.fakeClock();

    @BeforeEach
    void setUp() throws IndexNotFoundKernelException {
        when(populator.sample(any(CursorContext.class))).thenReturn(new IndexSample());
        when(indexStatisticsStore.indexSample(anyLong())).thenReturn(new IndexSample());
        when(indexStatisticsStore.storeFile()).thenReturn(Path.of("foo"));
        when(storeViewFactory.createTokenIndexStoreView(any())).thenReturn(storeView);
        ValueIndexReader indexReader = mock(ValueIndexReader.class);
        IndexSampler indexSampler = mock(IndexSampler.class);
        when(indexSampler.sampleIndex(any(), any())).thenReturn(new IndexSample());
        when(indexReader.createSampler()).thenReturn(indexSampler);
        when(accessor.newValueReader(any())).thenReturn(indexReader);
        when(storageEngine.getOpenOptions()).thenReturn(immutable.empty());
    }

    @AfterEach
    void tearDown() {
        life.shutdown();
    }

    @Test
    void noMessagesWhenThereIsNoIndexes() throws Throwable {
        IndexMapReference indexMapReference = new IndexMapReference();
        IndexingService indexingService = createIndexServiceWithCustomIndexMap(indexMapReference);
        indexingService.start();

        assertThat(internalLogProvider).doesNotHaveAnyLogs();
    }

    @Test
    void shouldBringIndexOnlineAndFlipOverToIndexAccessor() throws Exception {
        // given
        when(accessor.newUpdater(any(IndexUpdateMode.class), any(CursorContext.class), anyBoolean()))
                .thenReturn(updater);

        IndexingService indexingService = newIndexingServiceWithMockedDependencies(populator, accessor, withData());

        life.start();

        // when
        indexingService.createIndexes(AUTH_DISABLED, index);
        IndexProxy proxy = indexingService.getIndexProxy(index);

        waitForIndexesToComeOnline(indexingService, index);
        verify(populator, timeout(10000)).close(eq(true), any());

        try (IndexUpdater updater = proxy.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
            updater.process(add(10, "foo"));
        }

        // then
        assertEquals(ONLINE, proxy.getState());
        InOrder order = inOrder(populator, accessor, updater);
        order.verify(populator).create();
        order.verify(populator).close(eq(true), any());
        order.verify(accessor).newUpdater(eq(IndexUpdateMode.ONLINE_IDEMPOTENT), any(), anyBoolean());
        order.verify(updater).process(add(10, "foo"));
        order.verify(updater).close();
    }

    @Test
    void indexCreationShouldBeIdempotent() throws Exception {
        // given
        when(accessor.newUpdater(any(IndexUpdateMode.class), any(CursorContext.class), anyBoolean()))
                .thenReturn(updater);

        IndexingService indexingService = newIndexingServiceWithMockedDependencies(populator, accessor, withData());

        life.start();

        // when
        indexingService.createIndexes(AUTH_DISABLED, index);
        indexingService.createIndexes(AUTH_DISABLED, index);

        // We are asserting that the second call to createIndex does not throw an exception.
        waitForIndexesToComeOnline(indexingService, index);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldDeliverUpdatesThatOccurDuringPopulationToPopulator() throws Exception {
        // given
        when(populator.newPopulatingUpdater(any())).thenReturn(updater);

        CountDownLatch populationLatch = new CountDownLatch(1);

        Barrier.Control populationStartBarrier = new Barrier.Control();
        IndexMonitor monitor = new IndexMonitor.MonitorAdapter() {
            @Override
            public void indexPopulationScanStarting(IndexDescriptor[] indexDescriptors) {
                populationStartBarrier.reached();
            }

            @Override
            public void indexPopulationScanComplete() {
                try {
                    populationLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Index population monitor was interrupted", e);
                }
            }
        };
        IndexingService indexingService = newIndexingServiceWithMockedDependencies(
                populator, accessor, withData(addNodeUpdate(1, "value1")), monitor);

        life.start();

        // when

        indexingService.createIndexes(AUTH_DISABLED, index);
        IndexProxy proxy = indexingService.getIndexProxy(index);
        assertEquals(POPULATING, proxy.getState());
        populationStartBarrier.await();
        populationStartBarrier.release();

        IndexEntryUpdate<?> value2 = add(2, "value2");
        try (IndexUpdater updater = proxy.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
            updater.process(value2);
        }

        populationLatch.countDown();

        waitForIndexesToComeOnline(indexingService, index);
        verify(populator).close(eq(true), any());

        // then
        assertEquals(ONLINE, proxy.getState());
        InOrder order = inOrder(populator, accessor, updater);
        order.verify(populator).create();
        order.verify(populator).includeSample(add(1, "value1"));
        order.verify(populator, times(1)).add(any(Collection.class), any(CursorContext.class));
        order.verify(populator)
                .scanCompleted(
                        any(PhaseTracker.class),
                        any(IndexPopulator.PopulationWorkScheduler.class),
                        any(CursorContext.class));
        order.verify(populator).newPopulatingUpdater(any());
        order.verify(populator).includeSample(any());
        order.verify(updater).process(any());
        order.verify(updater).close();
        order.verify(populator).sample(any());
        order.verify(populator).close(eq(true), any());
        verifyNoMoreInteractions(updater);
        verifyNoMoreInteractions(populator);

        verifyNoInteractions(accessor);
    }

    @Test
    void shouldStillReportInternalIndexStateAsPopulatingWhenConstraintIndexIsDonePopulating() throws Exception {
        // given
        when(accessor.newUpdater(any(IndexUpdateMode.class), any(CursorContext.class), anyBoolean()))
                .thenReturn(updater);
        ValueIndexReader indexReader = mock(ValueIndexReader.class);
        when(accessor.newValueReader(any())).thenReturn(indexReader);
        doAnswer(new NodeIdsIndexReaderQueryAnswer(index)).when(indexReader).query(any(), any(), any(), any());

        IndexingService indexingService = newIndexingServiceWithMockedDependencies(populator, accessor, withData());

        life.start();

        // when
        IndexDescriptor index = constraintIndexRule(0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR);
        indexingService.createIndexes(AUTH_DISABLED, index);
        IndexProxy proxy = indexingService.getIndexProxy(index);

        // don't wait for index to come ONLINE here since we're testing that it doesn't
        verify(populator, timeout(20000)).close(eq(true), any());

        try (IndexUpdater updater = proxy.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
            updater.process(add(10, "foo"));
        }

        // then
        assertEquals(POPULATING, proxy.getState());
        InOrder order = inOrder(populator, accessor, updater);
        order.verify(populator).create();
        order.verify(populator).close(eq(true), any());
        order.verify(accessor).newUpdater(eq(IndexUpdateMode.ONLINE), any(), anyBoolean());
        order.verify(updater).process(add(10, "foo"));
        order.verify(updater).close();
    }

    @Test
    void shouldBringConstraintIndexOnlineWhenExplicitlyToldTo() throws Exception {
        // given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies(populator, accessor, withData());

        life.start();

        // when
        IndexDescriptor index = constraintIndexRule(0, labelId, propertyKeyId, PROVIDER_DESCRIPTOR);
        indexingService.createIndexes(AUTH_DISABLED, index);
        IndexProxy proxy = indexingService.getIndexProxy(index);

        indexingService.activateIndex(index);

        // then
        assertEquals(ONLINE, proxy.getState());
        InOrder order = inOrder(populator, accessor);
        order.verify(populator).create();
        order.verify(populator).close(eq(true), any(CursorContext.class));
    }

    @Test
    void shouldLogIndexStateOnInit() throws Exception {
        // given
        IndexProvider provider = mockIndexProviderWithAccessor(PROVIDER_DESCRIPTOR);
        IndexProviderMap providerMap = life.add(new MockIndexProviderMap(provider));

        IndexDescriptor onlineIndex = storeIndex(1, 1, 1, PROVIDER_DESCRIPTOR);
        IndexDescriptor populatingIndex = storeIndex(2, 1, 2, PROVIDER_DESCRIPTOR);
        IndexDescriptor failedIndex = storeIndex(3, 2, 2, PROVIDER_DESCRIPTOR);

        life.add(IndexingServiceFactory.createIndexingService(
                storageEngine,
                Config.defaults(),
                mock(JobScheduler.class),
                providerMap,
                mock(IndexStoreViewFactory.class),
                nameLookup,
                asList(onlineIndex, populatingIndex, failedIndex),
                internalLogProvider,
                IndexMonitor.NO_MONITOR,
                schemaState,
                indexStatisticsStore,
                new DatabaseIndexStats(),
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                writable(),
                Clocks.nanoClock(),
                mock(KernelVersionProvider.class),
                new DefaultFileSystemAbstraction(),
                EMPTY_VISIBILITY_PROVIDER));

        when(provider.getInitialState(eq(onlineIndex), any(), any())).thenReturn(ONLINE);
        when(provider.getInitialState(eq(populatingIndex), any(), any())).thenReturn(POPULATING);
        when(provider.getInitialState(eq(failedIndex), any(), any())).thenReturn(FAILED);

        nameLookup.label(1, "LabelOne");
        nameLookup.label(2, "LabelTwo");
        nameLookup.propertyKey(1, "propertyOne");
        nameLookup.propertyKey(2, "propertyTwo");

        // when
        life.init();

        // then
        assertThat(internalLogProvider)
                .forLevel(DEBUG)
                .containsMessages(
                        "IndexingService.init: index 1 on (:LabelOne {propertyOne}) is ONLINE",
                        "IndexingService.init: index 2 on (:LabelOne {propertyTwo}) is POPULATING",
                        "IndexingService.init: index 3 on (:LabelTwo {propertyTwo}) is FAILED");
    }

    @Test
    void shouldLogIndexStateOnStart() throws Throwable {
        // given
        IndexProvider provider = mockIndexProviderWithAccessor(PROVIDER_DESCRIPTOR);
        MockIndexProviderMap providerMap = new MockIndexProviderMap(provider);
        providerMap.init();

        IndexDescriptor onlineIndex = storeIndex(1, 1, 1, PROVIDER_DESCRIPTOR);
        IndexDescriptor populatingIndex = storeIndex(2, 1, 2, PROVIDER_DESCRIPTOR);
        IndexDescriptor failedIndex = storeIndex(3, 2, 2, PROVIDER_DESCRIPTOR);

        var indexingService = IndexingServiceFactory.createIndexingService(
                storageEngine,
                Config.defaults(),
                mock(JobScheduler.class),
                providerMap,
                storeViewFactory,
                nameLookup,
                asList(onlineIndex, populatingIndex, failedIndex),
                internalLogProvider,
                IndexMonitor.NO_MONITOR,
                schemaState,
                indexStatisticsStore,
                new DatabaseIndexStats(),
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                writable(),
                Clocks.nanoClock(),
                mock(KernelVersionProvider.class),
                new DefaultFileSystemAbstraction(),
                EMPTY_VISIBILITY_PROVIDER);

        when(provider.getInitialState(eq(onlineIndex), any(), any())).thenReturn(ONLINE);
        when(provider.getInitialState(eq(populatingIndex), any(), any())).thenReturn(POPULATING);
        when(provider.getInitialState(eq(failedIndex), any(), any())).thenReturn(FAILED);
        when(provider.getMinimalIndexAccessor(any(), anyBoolean())).thenReturn(mock(MinimalIndexAccessor.class));

        indexingService.init();

        nameLookup.label(1, "LabelOne");
        nameLookup.label(2, "LabelTwo");
        nameLookup.propertyKey(1, "propertyOne");
        nameLookup.propertyKey(2, "propertyTwo");
        when(indexStatisticsStore.indexSample(anyLong())).thenReturn(new IndexSample(100L, 32L, 32L));

        internalLogProvider.clear();

        // when
        indexingService.start();

        // then
        verify(provider).getPopulationFailure(eq(failedIndex), any(), any());
        assertThat(internalLogProvider)
                .forLevel(DEBUG)
                .containsMessages(
                        "IndexingService.start: index 1 on (:LabelOne {propertyOne}) is ONLINE",
                        "IndexingService.start: index 2 on (:LabelOne {propertyTwo}) is POPULATING",
                        "IndexingService.start: index 3 on (:LabelTwo {propertyTwo}) is FAILED");
    }

    @Test
    void shouldFailToStartIfMissingIndexProvider() throws Exception {
        // GIVEN an indexing service that has a schema index provider X
        String otherProviderKey = "something-completely-different";
        IndexProviderDescriptor otherDescriptor = new IndexProviderDescriptor(otherProviderKey, "no-version");
        IndexDescriptor rule = storeIndex(1, 2, 3, otherDescriptor);
        newIndexingServiceWithMockedDependencies(
                mock(IndexPopulator.class), mock(IndexAccessor.class), new DataUpdates(), rule);

        // WHEN trying to start up and initialize it with an index from provider Y
        assertThatThrownBy(life::init)
                .isInstanceOf(LifecycleException.class)
                .hasMessageContaining("lookup by descriptor failed");
    }

    @Test
    void shouldSnapshotOnlineAndFailedIndexes() throws Exception {
        // GIVEN
        int indexId1 = 1;
        int indexId2 = 2;
        int indexId3 = 3;
        IndexDescriptor rule1 = storeIndex(indexId1, 2, 3, PROVIDER_DESCRIPTOR);
        IndexDescriptor rule2 = storeIndex(indexId2, 4, 5, PROVIDER_DESCRIPTOR);
        IndexDescriptor rule3 = storeIndex(indexId3, 7, 9, PROVIDER_DESCRIPTOR);

        IndexAccessor indexAccessor = mock(IndexAccessor.class);
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                mock(IndexPopulator.class), indexAccessor, new DataUpdates(), rule1, rule2, rule3);
        Path theFile = Path.of("Blah");

        when(indexAccessor.snapshotFiles()).thenAnswer(newResourceIterator(theFile));
        when(indexProvider.getInitialState(eq(rule1), any(), any())).thenReturn(ONLINE);
        when(indexProvider.getInitialState(eq(rule2), any(), any())).thenReturn(ONLINE);
        when(indexProvider.getInitialState(eq(rule3), any(), any())).thenReturn(FAILED);
        when(indexStatisticsStore.indexSample(anyLong())).thenReturn(new IndexSample(100L, 32L, 32L));

        life.start();

        // WHEN
        ResourceIterator<Path> files = indexing.snapshotIndexFiles();

        // THEN
        // We get a snapshot per online / failed index
        assertThat(asCollection(files))
                .isEqualTo(asCollection(iterator(indexStatisticsStore.storeFile(), theFile, theFile, theFile)));
    }

    @Test
    void shouldNotSnapshotPopulatingIndexes() throws Exception {
        // GIVEN
        CountDownLatch populatorLatch = new CountDownLatch(1);
        IndexAccessor indexAccessor = mock(IndexAccessor.class);
        int indexId = 1;
        int indexId2 = 2;
        IndexDescriptor index1 = storeIndex(indexId, 2, 3, PROVIDER_DESCRIPTOR);
        IndexDescriptor index2 = storeIndex(indexId2, 4, 5, PROVIDER_DESCRIPTOR);
        IndexingService indexing =
                newIndexingServiceWithMockedDependencies(populator, indexAccessor, new DataUpdates(), index1, index2);
        Path theFile = Path.of("Blah");

        doAnswer(waitForLatch(populatorLatch)).when(populator).create();
        when(indexAccessor.snapshotFiles()).thenAnswer(newResourceIterator(theFile));
        when(indexProvider.getInitialState(eq(index1), any(), any())).thenReturn(POPULATING);
        when(indexProvider.getInitialState(eq(index2), any(), any())).thenReturn(ONLINE);
        when(indexStatisticsStore.indexSample(anyLong())).thenReturn(new IndexSample(100, 32, 32));
        life.start();

        // WHEN
        ResourceIterator<Path> files = indexing.snapshotIndexFiles();
        populatorLatch.countDown(); // only now, after the snapshot, is the population job allowed to finish
        waitForIndexesToComeOnline(indexing, index1, index2);

        // THEN
        // We get a snapshot from the online index, but no snapshot from the populating one
        assertThat(asCollection(files)).isEqualTo(asCollection(iterator(indexStatisticsStore.storeFile(), theFile)));
    }

    @Test
    void shouldIgnoreActivateCallDuringRecovery() throws Exception {
        // given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies(populator, accessor, withData());
        IndexDescriptor index = forSchema(forLabel(0, 0))
                .withIndexProvider(PROVIDER_DESCRIPTOR)
                .withName("index")
                .materialise(0);

        // when
        indexingService.activateIndex(index);

        // then no exception should be thrown.
    }

    @Test
    void shouldLogTriggerSamplingOnAllIndexes() throws Exception {
        // given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies(populator, accessor, withData());
        IndexSamplingMode mode = backgroundRebuildAll();

        // when
        indexingService.triggerIndexSampling(mode);

        // then
        assertThat(internalLogProvider)
                .forLevel(INFO)
                .containsMessages("Manual trigger for sampling all indexes [" + mode + "]");
    }

    @Test
    void shouldLogTriggerSamplingOnAnIndexes() throws Exception {
        // given
        long indexId = 0;
        IndexSamplingMode mode = backgroundRebuildAll();
        IndexPrototype prototype =
                forSchema(forLabel(0, 1)).withIndexProvider(PROVIDER_DESCRIPTOR).withName("index");
        IndexDescriptor index = prototype.materialise(indexId);
        when(accessor.newValueReader(any())).thenReturn(ValueIndexReader.EMPTY);
        IndexingService indexingService =
                newIndexingServiceWithMockedDependencies(populator, accessor, withData(), index);
        life.init();
        life.start();

        // when
        indexingService.triggerIndexSampling(index, mode);

        // then
        String userDescription = index.userDescription(nameLookup);
        assertThat(internalLogProvider)
                .forLevel(INFO)
                .containsMessages("Manual trigger for sampling index " + userDescription + " [" + mode + "]");
    }

    @Test
    void applicationOfIndexUpdatesShouldThrowIfServiceIsShutdown() throws IOException {
        // Given
        IndexingService indexingService = newIndexingServiceWithMockedDependencies(populator, accessor, withData());
        life.start();
        life.shutdown();

        var e = assertThrows(
                IllegalStateException.class,
                () -> indexingService.applyUpdates(asSet(add(1, "foo")), NULL_CONTEXT, false));
        assertThat(e.getMessage()).startsWith("Can't apply index updates");
    }

    @Test
    void applicationOfUpdatesShouldFlush() throws Exception {
        // Given
        when(accessor.newUpdater(any(IndexUpdateMode.class), any(CursorContext.class), anyBoolean()))
                .thenReturn(updater);
        IndexingService indexing = newIndexingServiceWithMockedDependencies(populator, accessor, withData());
        life.start();

        indexing.createIndexes(AUTH_DISABLED, index);
        waitForIndexesToComeOnline(indexing, index);
        verify(populator, timeout(10000)).close(eq(true), any());

        // When
        indexing.applyUpdates(asList(add(1, "foo"), add(2, "bar")), NULL_CONTEXT, false);

        // Then
        InOrder inOrder = inOrder(updater);
        inOrder.verify(updater).process(add(1, "foo"));
        inOrder.verify(updater).process(add(2, "bar"));
        inOrder.verify(updater).close();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void closingOfValidatedUpdatesShouldCloseUpdaters() throws Exception {
        // Given
        long indexId1 = 1;
        long indexId2 = 2;

        int labelId1 = 24;
        int labelId2 = 42;

        IndexDescriptor index1 = storeIndex(indexId1, labelId1, propertyKeyId, PROVIDER_DESCRIPTOR);
        IndexDescriptor index2 = storeIndex(indexId2, labelId2, propertyKeyId, PROVIDER_DESCRIPTOR);

        IndexingService indexing = newIndexingServiceWithMockedDependencies(populator, accessor, withData());

        IndexAccessor accessor1 = mock(IndexAccessor.class);
        IndexUpdater updater1 = mock(IndexUpdater.class);
        when(accessor1.newUpdater(any(IndexUpdateMode.class), any(CursorContext.class), anyBoolean()))
                .thenReturn(updater1);

        IndexAccessor accessor2 = mock(IndexAccessor.class);
        IndexUpdater updater2 = mock(IndexUpdater.class);
        when(accessor2.newUpdater(any(IndexUpdateMode.class), any(CursorContext.class), anyBoolean()))
                .thenReturn(updater2);

        when(indexProvider.getOnlineAccessor(
                        eq(index1), any(IndexSamplingConfig.class), any(TokenNameLookup.class), any(), any()))
                .thenReturn(accessor1);
        when(indexProvider.getOnlineAccessor(
                        eq(index2), any(IndexSamplingConfig.class), any(TokenNameLookup.class), any(), any()))
                .thenReturn(accessor2);

        life.start();

        indexing.createIndexes(AUTH_DISABLED, index1);
        indexing.createIndexes(AUTH_DISABLED, index2);

        waitForIndexesToComeOnline(indexing, index1, index2);

        verify(populator, timeout(10000).times(2)).close(eq(true), any());

        // When
        indexing.applyUpdates(asList(add(1, "foo", index1), add(2, "bar", index2)), NULL_CONTEXT, false);

        // Then
        verify(updater1).close();
        verify(updater2).close();
    }

    private static void waitForIndexesToComeOnline(IndexingService indexing, IndexDescriptor... index)
            throws IndexNotFoundKernelException {
        waitForIndexesToGetIntoState(indexing, ONLINE, index);
    }

    private static void waitForIndexesToGetIntoState(
            IndexingService indexing, InternalIndexState state, IndexDescriptor... indexes)
            throws IndexNotFoundKernelException {
        long end = currentTimeMillis() + SECONDS.toMillis(30);
        while (!allInState(indexing, state, indexes)) {
            if (currentTimeMillis() > end) {
                fail("Indexes couldn't come online");
            }
        }
    }

    private static boolean allInState(IndexingService indexing, InternalIndexState state, IndexDescriptor[] indexes)
            throws IndexNotFoundKernelException {
        for (IndexDescriptor index : indexes) {
            if (indexing.getIndexProxy(index).getState() != state) {
                return false;
            }
        }
        return true;
    }

    private Iterable<IndexEntryUpdate<IndexDescriptor>> nodeIdsAsIndexUpdates(long... nodeIds) {
        return () -> {
            List<IndexEntryUpdate<IndexDescriptor>> updates = new ArrayList<>();
            for (long nodeId : nodeIds) {
                updates.add(IndexEntryUpdate.add(nodeId, index, Values.of(1)));
            }
            return updates.iterator();
        };
    }

    /*
     * See comments in IndexingService#createIndex
     */
    @Test
    void shouldNotLoseIndexDescriptorDueToOtherSimilarIndexDuringRecovery() throws Exception {
        // GIVEN
        long nodeId = 0;
        long otherIndexId = 2;
        Update update = addNodeUpdate(nodeId, "value");
        when(indexStatisticsStore.indexSample(anyLong())).thenReturn(new IndexSample(100, 42, 42));
        // For some reason the usual accessor returned null from newUpdater, even when told to return the updater
        // so spying on a real object instead.
        IndexAccessor accessor = spy(new TrackingIndexAccessor());
        IndexingService indexing =
                newIndexingServiceWithMockedDependencies(populator, accessor, withData(update), index);
        when(indexProvider.getInitialState(eq(index), any(), any())).thenReturn(ONLINE);
        life.init();

        // WHEN dropping another index, which happens to have the same label/property... while recovering
        IndexDescriptor otherIndex = prototype.withName("index_" + otherIndexId).materialise(otherIndexId);
        indexing.createIndexes(AUTH_DISABLED, otherIndex);
        indexing.dropIndex(otherIndex);
        // and WHEN finally creating our index again (at a later point in recovery)
        indexing.createIndexes(AUTH_DISABLED, index);
        reset(accessor);
        indexing.applyUpdates(nodeIdsAsIndexUpdates(nodeId), NULL_CONTEXT, false);
        // and WHEN starting, i.e. completing recovery
        life.start();

        verify(accessor).newUpdater(eq(RECOVERY), any(CursorContext.class), anyBoolean());
    }

    @Test
    void shouldNotLoseIndexDescriptorDueToOtherVerySimilarIndexDuringRecovery() throws Exception {
        // GIVEN
        AtomicReference<BinaryLatch> populationStartLatch = latchedIndexPopulation();
        long nodeId = 0;
        Update update = addNodeUpdate(nodeId, "value");
        when(indexStatisticsStore.indexSample(anyLong())).thenReturn(new IndexSample(100, 42, 42));
        // For some reason the usual accessor returned null from newUpdater, even when told to return the updater
        // so spying on a real object instead.
        IndexAccessor accessor = spy(new TrackingIndexAccessor());
        IndexingService indexing =
                newIndexingServiceWithMockedDependencies(populator, accessor, withData(update), index);
        when(indexProvider.getInitialState(eq(index), any(), any())).thenReturn(ONLINE);
        life.init();
        populationStartLatch.getAndSet(new BinaryLatch()).release();

        // WHEN dropping another index, which happens to be identical to the existing one except for different index
        // config... while recovering
        IndexConfig indexConfig = index.getIndexConfig().withIfAbsent("a", Values.booleanValue(true));
        IndexDescriptor otherIndex = index.withIndexConfig(indexConfig);
        indexing.createIndexes(AUTH_DISABLED, otherIndex);
        indexing.dropIndex(otherIndex);
        // and WHEN finally creating our index again (at a later point in recovery)
        indexing.createIndexes(AUTH_DISABLED, index);
        reset(accessor);
        indexing.applyUpdates(nodeIdsAsIndexUpdates(nodeId), NULL_CONTEXT, false);
        // and WHEN starting, i.e. completing recovery
        life.start();

        IndexProxy indexProxy = indexing.getIndexProxy(index);
        try {
            assertNull(indexProxy.getDescriptor().getIndexConfig().get("a"));
            assertThat(indexProxy.getState())
                    .isEqualTo(POPULATING); // The existing online index got nuked during recovery.
        } finally {
            populationStartLatch.get().release();
        }
    }

    @Test
    void shouldWaitForRecoveredUniquenessConstraintIndexesToBeFullyPopulated() throws Exception {
        // I.e. when a uniqueness constraint is created, but database crashes before that schema record
        // ends up in the store, so that next start have no choice but to rebuild it.

        // GIVEN
        DoubleLatch latch = new DoubleLatch();
        ControlledIndexPopulator populator = new ControlledIndexPopulator(latch);
        AtomicReference<IndexDescriptor> indexRef = new AtomicReference<>();
        IndexMonitor monitor = new IndexMonitor.MonitorAdapter() {
            @Override
            public void awaitingPopulationOfRecoveredIndex(IndexDescriptor descriptor) {
                // When we see that we start to await the index to populate, notify the slow-as-heck
                // populator that it can actually go and complete its job.
                indexRef.set(descriptor);
                latch.startAndWaitForAllToStart();
            }
        };
        // leaving out the IndexRule here will have the index being populated from scratch
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                populator, accessor, withData(addNodeUpdate(0, "value", 1)), monitor);

        // WHEN initializing, i.e. preparing for recovery
        life.init();
        // simulating an index being created as part of applying recovered transactions
        long fakeOwningConstraintRuleId = 1;
        indexing.createIndexes(
                AUTH_DISABLED,
                constraintIndexRule(2, labelId, propertyKeyId, PROVIDER_DESCRIPTOR, fakeOwningConstraintRuleId));
        // and then starting, i.e. considering recovery completed
        life.start();

        // THEN afterwards the index should be ONLINE
        assertEquals(2, indexRef.get().getId());
        assertEquals(
                InternalIndexState.ONLINE,
                indexing.getIndexProxy(indexRef.get()).getState());
    }

    @Test
    void shouldCreateMultipleIndexesInOneCall() throws Exception {
        // GIVEN
        IndexMonitor monitor = IndexMonitor.NO_MONITOR;
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                populator, accessor, withData(addNodeUpdate(0, "value", 1)), monitor);
        life.start();

        // WHEN
        IndexDescriptor index1 = storeIndex(0, 0, 0, PROVIDER_DESCRIPTOR);
        IndexDescriptor index2 = storeIndex(1, 0, 1, PROVIDER_DESCRIPTOR);
        IndexDescriptor index3 = storeIndex(2, 1, 0, PROVIDER_DESCRIPTOR);
        indexing.createIndexes(AUTH_DISABLED, index1, index2, index3);

        // THEN
        IndexPrototype prototype = forSchema(forLabel(0, 0)).withIndexProvider(PROVIDER_DESCRIPTOR);
        verify(indexProvider)
                .getPopulator(
                        eq(prototype.withName("index_0").materialise(0)),
                        any(IndexSamplingConfig.class),
                        any(),
                        any(),
                        any(TokenNameLookup.class),
                        any(),
                        any());
        verify(indexProvider)
                .getPopulator(
                        eq(prototype
                                .withSchemaDescriptor(forLabel(0, 1))
                                .withName("index_1")
                                .materialise(1)),
                        any(IndexSamplingConfig.class),
                        any(),
                        any(),
                        any(TokenNameLookup.class),
                        any(),
                        any());
        verify(indexProvider)
                .getPopulator(
                        eq(prototype
                                .withSchemaDescriptor(forLabel(1, 0))
                                .withName("index_2")
                                .materialise(2)),
                        any(IndexSamplingConfig.class),
                        any(),
                        any(),
                        any(TokenNameLookup.class),
                        any(),
                        any());

        waitForIndexesToComeOnline(indexing, index1, index2, index3);
    }

    @Test
    void shouldStoreIndexFailureWhenFailingToCreateOnlineAccessorAfterPopulating() throws Exception {
        // given
        IndexingService indexing = newIndexingServiceWithMockedDependencies(populator, accessor, withData());

        IOException exception = new IOException("Expected failure");
        nameLookup.label(labelId, "TheLabel");
        nameLookup.propertyKey(propertyKeyId, "propertyKey");

        when(indexProvider.getOnlineAccessor(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenThrow(exception);

        life.start();
        ArgumentCaptor<Boolean> closeArgs = ArgumentCaptor.forClass(Boolean.class);

        // when
        indexing.createIndexes(AUTH_DISABLED, index);
        waitForIndexesToGetIntoState(indexing, FAILED, index);
        verify(populator, timeout(10000).times(2)).close(closeArgs.capture(), any());

        // then
        assertEquals(FAILED, indexing.getIndexProxy(index).getState());
        assertEquals(asList(true, false), closeArgs.getAllValues());
        assertThat(storedFailure()).contains(format("java.io.IOException: Expected failure%n\tat "));
        assertThat(internalLogProvider)
                .forClass(IndexPopulationJob.class)
                .forLevel(ERROR)
                .assertExceptionForLogMessage(
                        "Failed to populate index: [Index( id=0, name='index', type='RANGE', schema=(:TheLabel {propertyKey}), "
                                + "indexProvider='quantum-dex-25.0' )]")
                .hasRootCause(exception);
        assertThat(internalLogProvider)
                .forClass(IndexPopulationJob.class)
                .forLevel(INFO)
                .doesNotContainMessageWithArguments(
                        "Index population completed. Index is now online: [%s]",
                        "Index( id=0, name='index', type='RANGE', schema=(:TheLabel {propertyKey}), indexProvider='quantum-dex-25.0' )");
    }

    @Test
    void shouldStoreIndexFailureWhenFailingToCreateOnlineAccessorAfterRecoveringPopulatingIndex() throws Exception {
        // given
        IndexingService indexing = newIndexingServiceWithMockedDependencies(populator, accessor, withData(), index);

        IOException exception = new IOException("Expected failure");
        nameLookup.label(labelId, "TheLabel");
        nameLookup.propertyKey(propertyKeyId, "propertyKey");

        when(indexProvider.getInitialState(eq(index), any(), any())).thenReturn(POPULATING);
        when(indexProvider.getOnlineAccessor(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenThrow(exception);
        when(indexProvider.getMinimalIndexAccessor(index, true)).thenReturn(mock(MinimalIndexAccessor.class));

        life.start();
        ArgumentCaptor<Boolean> closeArgs = ArgumentCaptor.forClass(Boolean.class);

        // when
        waitForIndexesToGetIntoState(indexing, FAILED, index);
        verify(populator, timeout(10000).times(2)).close(closeArgs.capture(), any());

        // then
        assertEquals(FAILED, indexing.getIndexProxy(index).getState());
        assertEquals(asList(true, false), closeArgs.getAllValues());
        assertThat(storedFailure()).contains(format("java.io.IOException: Expected failure%n\tat "));
        assertThat(internalLogProvider)
                .forClass(IndexPopulationJob.class)
                .forLevel(ERROR)
                .assertExceptionForLogMessage("Failed to populate index: [Index( id=0, name='index', type='RANGE', "
                        + "schema=(:TheLabel {propertyKey}), indexProvider='quantum-dex-25.0' )]")
                .hasRootCause(exception);
        assertThat(internalLogProvider)
                .forClass(IndexPopulationJob.class)
                .forLevel(INFO)
                .doesNotContainMessageWithArguments(
                        "Index population completed. Index is now online: [%s]",
                        "Index( id=0, name='index', type='RANGE', schema=(:TheLabel {propertyKey}), indexProvider='quantum-dex-25.0' )");
    }

    @Test
    void constraintIndexesWithoutConstraintsMustGetPopulatingProxies() throws Exception {
        // given
        AtomicReference<BinaryLatch> populationStartLatch = latchedIndexPopulation();
        try {
            long indexId = 1;
            IndexDescriptor index = uniqueIndex.materialise(indexId); // Note the lack of an "owned constraint id".
            IndexingService indexing = newIndexingServiceWithMockedDependencies(populator, accessor, withData(), index);
            when(indexProvider.getInitialState(eq(index), any(), any())).thenReturn(POPULATING);

            // when
            life.start();

            // then
            assertEquals(POPULATING, indexing.getIndexProxy(index).getState());
        } finally {
            populationStartLatch.get().release();
        }
    }

    @Test
    void shouldReportCauseOfPopulationFailureIfPopulationFailsDuringRecovery() throws Exception {
        // given
        long indexId = 1;
        long constraintId = 2;
        IndexDescriptor indexRule = uniqueIndex.materialise(indexId).withOwningConstraintId(constraintId);
        Barrier.Control barrier = new Barrier.Control();
        CountDownLatch exceptionBarrier = new CountDownLatch(1);
        IndexingService indexing = newIndexingServiceWithMockedDependencies(
                populator,
                accessor,
                withData(),
                new IndexMonitor.MonitorAdapter() {
                    @Override
                    public void awaitingPopulationOfRecoveredIndex(IndexDescriptor descriptor) {
                        barrier.reached();
                    }
                },
                indexRule);
        when(indexProvider.getInitialState(eq(indexRule), any(), any())).thenReturn(POPULATING);

        life.init();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> {
                try {
                    life.start();
                } finally {
                    exceptionBarrier.countDown();
                }
            });

            // Thread is just about to start checking index status. We flip to failed proxy to indicate population
            // failure during recovery.
            barrier.await();
            // Wait for the index to come online, otherwise we'll race the failed flip below with its flip and sometimes
            // the POPULATING -> ONLINE
            // flip will win and make the index NOT fail and therefor hanging this test awaiting on the exceptionBarrier
            // below
            waitForIndexesToComeOnline(indexing, indexRule);
            IndexProxy indexProxy = indexing.getIndexProxy(indexRule);
            assertThat(indexProxy).isInstanceOf(ContractCheckingIndexProxy.class);
            ContractCheckingIndexProxy contractCheckingIndexProxy = (ContractCheckingIndexProxy) indexProxy;
            IndexProxy delegate = contractCheckingIndexProxy.getDelegate();
            assertThat(delegate).isInstanceOf(FlippableIndexProxy.class);
            FlippableIndexProxy flippableIndexProxy = (FlippableIndexProxy) delegate;
            Exception expectedCause = new Exception("index was failed on purpose");
            IndexPopulationFailure indexFailure = IndexPopulationFailure.failure(expectedCause);

            flippableIndexProxy.flipTo(new FailedIndexProxy(
                    new ValueIndexProxyStrategy(indexRule, mock(IndexStatisticsStore.class), nameLookup),
                    mock(IndexPopulator.class),
                    indexFailure,
                    internalLogProvider));
            barrier.release();
            exceptionBarrier.await();

            assertThat(internalLogProvider)
                    .containsMessages(expectedCause.getMessage())
                    .containsMessages(format("Index %s entered %s state ", indexRule, FAILED));
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void shouldLogIndexStateOutliersOnInit() throws Exception {
        // given
        IndexProvider provider = mockIndexProviderWithAccessor(PROVIDER_DESCRIPTOR);
        IndexProviderMap providerMap = life.add(new MockIndexProviderMap(provider));

        List<IndexDescriptor> indexes = new ArrayList<>();
        int nextIndexId = 1;
        IndexDescriptor populatingIndex = storeIndex(nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR);
        when(provider.getInitialState(eq(populatingIndex), any(), any())).thenReturn(POPULATING);
        indexes.add(populatingIndex);
        IndexDescriptor failedIndex = storeIndex(nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR);
        when(provider.getInitialState(eq(failedIndex), any(), any())).thenReturn(FAILED);
        indexes.add(failedIndex);
        for (int i = 0; i < 10; i++) {
            IndexDescriptor indexRule = storeIndex(nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR);
            when(provider.getInitialState(eq(indexRule), any(), any())).thenReturn(ONLINE);
            indexes.add(indexRule);
        }
        for (int i = 0; i < nextIndexId; i++) {
            nameLookup.label(i, "Label" + i);
        }

        life.add(IndexingServiceFactory.createIndexingService(
                storageEngine,
                Config.defaults(),
                mock(JobScheduler.class),
                providerMap,
                mock(IndexStoreViewFactory.class),
                nameLookup,
                indexes,
                internalLogProvider,
                IndexMonitor.NO_MONITOR,
                schemaState,
                indexStatisticsStore,
                new DatabaseIndexStats(),
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                writable(),
                Clocks.nanoClock(),
                mock(KernelVersionProvider.class),
                new DefaultFileSystemAbstraction(),
                EMPTY_VISIBILITY_PROVIDER));

        nameLookup.propertyKey(1, "prop");

        // when
        life.init();

        // then
        assertThat(internalLogProvider)
                .forLevel(INFO)
                .containsMessages(
                        "IndexingService.init: index 1 on (:Label1 {prop}) is POPULATING",
                        "IndexingService.init: index 2 on (:Label2 {prop}) is FAILED",
                        "IndexingService.init: indexes not specifically mentioned above are ONLINE")
                .doesNotContainMessage("IndexingService.init: index 3 on :Label3(prop) is ONLINE");
    }

    @Test
    void shouldLogIndexStateOutliersOnStart() throws Throwable {
        // given
        IndexProvider provider = mockIndexProviderWithAccessor(PROVIDER_DESCRIPTOR);
        MockIndexProviderMap providerMap = new MockIndexProviderMap(provider);
        providerMap.init();

        List<IndexDescriptor> indexes = new ArrayList<>();
        int nextIndexId = 1;
        IndexDescriptor populatingIndex = storeIndex(nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR);
        when(provider.getInitialState(eq(populatingIndex), any(), any())).thenReturn(POPULATING);
        indexes.add(populatingIndex);
        IndexDescriptor failedIndex = storeIndex(nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR);
        when(provider.getInitialState(eq(failedIndex), any(), any())).thenReturn(FAILED);
        indexes.add(failedIndex);
        for (int i = 0; i < 10; i++) {
            IndexDescriptor indexRule = storeIndex(nextIndexId, nextIndexId++, 1, PROVIDER_DESCRIPTOR);
            when(provider.getInitialState(eq(indexRule), any(), any())).thenReturn(ONLINE);
            indexes.add(indexRule);
        }
        when(provider.getMinimalIndexAccessor(any(), anyBoolean())).thenReturn(mock(MinimalIndexAccessor.class));
        for (int i = 0; i < nextIndexId; i++) {
            nameLookup.label(i, "Label" + i);
        }

        IndexingService indexingService = IndexingServiceFactory.createIndexingService(
                storageEngine,
                Config.defaults(),
                mock(JobScheduler.class),
                providerMap,
                storeViewFactory,
                nameLookup,
                indexes,
                internalLogProvider,
                IndexMonitor.NO_MONITOR,
                schemaState,
                indexStatisticsStore,
                new DatabaseIndexStats(),
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                writable(),
                Clocks.nanoClock(),
                mock(KernelVersionProvider.class),
                new DefaultFileSystemAbstraction(),
                EMPTY_VISIBILITY_PROVIDER);
        when(indexStatisticsStore.indexSample(anyLong())).thenReturn(new IndexSample(100, 32, 32));
        nameLookup.propertyKey(1, "prop");

        // when
        indexingService.init();
        internalLogProvider.clear();
        indexingService.start();

        // then
        assertThat(internalLogProvider)
                .forLevel(INFO)
                .containsMessages(
                        "IndexingService.start: index 1 on (:Label1 {prop}) is POPULATING",
                        "IndexingService.start: index 2 on (:Label2 {prop}) is FAILED",
                        "IndexingService.start: indexes not specifically mentioned above are ONLINE")
                .doesNotContainMessage("IndexingService.start: index 3 on :Label3(prop) is ONLINE");
    }

    @Test
    void flushAllIndexesWhileSomeOfThemDropped() throws IOException {
        IndexMapReference indexMapReference = new IndexMapReference();
        IndexProxy validIndex1 = createIndexProxyMock(1);
        IndexProxy validIndex2 = createIndexProxyMock(2);
        IndexProxy deletedIndexProxy = createIndexProxyMock(3);
        IndexProxy validIndex3 = createIndexProxyMock(4);
        IndexProxy validIndex4 = createIndexProxyMock(5);
        indexMapReference.modify(indexMap -> {
            indexMap.putIndexProxy(validIndex1);
            indexMap.putIndexProxy(validIndex2);
            indexMap.putIndexProxy(deletedIndexProxy);
            indexMap.putIndexProxy(validIndex3);
            indexMap.putIndexProxy(validIndex4);
            return indexMap;
        });

        doAnswer(invocation -> {
                    indexMapReference.modify(indexMap -> {
                        indexMap.removeIndexProxy(3);
                        return indexMap;
                    });
                    throw new RuntimeException("Index deleted.");
                })
                .when(deletedIndexProxy)
                .force(any(), any(CursorContext.class));

        IndexingService indexingService = createIndexServiceWithCustomIndexMap(indexMapReference);

        indexingService.checkpoint(DatabaseFlushEvent.NULL, NULL_CONTEXT);
        verify(validIndex1).force(FileFlushEvent.NULL, NULL_CONTEXT);
        verify(validIndex2).force(FileFlushEvent.NULL, NULL_CONTEXT);
        verify(validIndex3).force(FileFlushEvent.NULL, NULL_CONTEXT);
        verify(validIndex4).force(FileFlushEvent.NULL, NULL_CONTEXT);
    }

    @Test
    void failForceAllWhenOneOfTheIndexesFailToForce() throws IOException {
        IndexMapReference indexMapReference = new IndexMapReference();
        IndexProxy strangeIndexProxy = createIndexProxyMock(1);
        doThrow(new UncheckedIOException(new IOException("Can't force")))
                .when(strangeIndexProxy)
                .force(any(), any(CursorContext.class));
        indexMapReference.modify(indexMap -> {
            IndexProxy validIndex = createIndexProxyMock(0);
            indexMap.putIndexProxy(validIndex);
            indexMap.putIndexProxy(validIndex);
            indexMap.putIndexProxy(strangeIndexProxy);
            indexMap.putIndexProxy(validIndex);
            indexMap.putIndexProxy(validIndex);
            return indexMap;
        });

        IndexingService indexingService = createIndexServiceWithCustomIndexMap(indexMapReference);

        var e = assertThrows(
                UnderlyingStorageException.class,
                () -> indexingService.checkpoint(DatabaseFlushEvent.NULL, NULL_CONTEXT));
        assertThat(e.getMessage()).startsWith("Unable to force");
    }

    @Test
    void shouldRefreshIndexesOnStart() throws Exception {
        // given
        newIndexingServiceWithMockedDependencies(populator, accessor, withData(), index);

        IndexAccessor accessor = mock(IndexAccessor.class);
        IndexUpdater updater = mock(IndexUpdater.class);
        when(accessor.newValueReader(any())).thenReturn(ValueIndexReader.EMPTY);
        when(accessor.newUpdater(any(IndexUpdateMode.class), any(CursorContext.class), anyBoolean()))
                .thenReturn(updater);
        when(indexProvider.getOnlineAccessor(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenReturn(accessor);

        life.init();

        verify(accessor, never()).refresh();

        life.start();

        // Then
        verify(accessor).refresh();
    }

    @Test
    void shouldNotHaveToWaitForOrphanedUniquenessIndexInRecovery() throws Exception {
        // given that we have a uniqueness index that needs to be recovered and that doesn't have a constraint attached
        // to it
        IndexDescriptor descriptor = uniqueIndex.materialise(10);
        Iterable<IndexDescriptor> schemaRules = Collections.singletonList(descriptor);
        IndexProvider indexProvider = mock(IndexProvider.class);
        when(indexProvider.getInitialState(any(), any(), any())).thenReturn(POPULATING);
        IndexProviderMap indexProviderMap = mock(IndexProviderMap.class);
        when(indexProviderMap.lookup(anyString())).thenReturn(indexProvider);
        when(indexProviderMap.lookup(any(IndexProviderDescriptor.class))).thenReturn(indexProvider);
        when(indexProviderMap.getDefaultProvider()).thenReturn(indexProvider);
        NullLogProvider logProvider = NullLogProvider.getInstance();
        IndexMapReference indexMapReference = new IndexMapReference();
        IndexProxyCreator indexProxyCreator = mock(IndexProxyCreator.class);
        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexProxy.getDescriptor()).thenReturn(descriptor);
        // Eventually return ONLINE so that this test won't hang if the product code changes in this regard.
        // This test should still fail below when verifying interactions with the proxy and monitor tho.
        when(indexProxy.getState()).thenReturn(POPULATING, POPULATING, POPULATING, POPULATING, ONLINE);
        when(indexProxyCreator.createRecoveringIndexProxy(any())).thenReturn(indexProxy);
        when(indexProxyCreator.createFailedIndexProxy(any(), any())).thenReturn(indexProxy);
        when(indexProxyCreator.createPopulatingIndexProxy(any(), any(), any())).thenReturn(indexProxy);
        JobScheduler scheduler = mock(JobScheduler.class);
        IndexSamplingController samplingController = mock(IndexSamplingController.class);
        IndexMonitor monitor = mock(IndexMonitor.class);
        IndexStoreViewFactory storeViewFactory = mock(IndexStoreViewFactory.class);
        when(storeViewFactory.createTokenIndexStoreView(any())).thenReturn(storeView);
        IndexingService indexingService = new IndexingService(
                storageEngine,
                indexProxyCreator,
                indexProviderMap,
                indexMapReference,
                storeViewFactory,
                schemaRules,
                samplingController,
                nameLookup,
                scheduler,
                null,
                logProvider,
                monitor,
                mock(IndexStatisticsStore.class),
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                writable(),
                Config.defaults(),
                mock(KernelVersionProvider.class),
                new DefaultFileSystemAbstraction(),
                EMPTY_VISIBILITY_PROVIDER);
        // and where index population starts
        indexingService.init();

        // when starting the indexing service
        indexingService.start();

        // then it should be able to start without awaiting the completion of the population of the index
        verify(indexProxy, never()).awaitStoreScanCompleted(anyLong(), any());
        verify(monitor, never()).awaitingPopulationOfRecoveredIndex(any());
    }

    @Test
    void shouldIncrementIndexUpdatesAfterStartingExistingOnlineIndexProxy() throws Exception {
        // given
        long indexId = 10;
        IndexDescriptor indexDescriptor = uniqueIndex.materialise(indexId);
        IndexingService indexingService =
                newIndexingServiceWithMockedDependencies(populator, accessor, withData(), indexDescriptor);
        life.start();

        // when
        IndexProxy proxy = indexingService.getIndexProxy(indexDescriptor);
        proxy.awaitStoreScanCompleted(1, HOURS);
        proxy.activate();
        try (IndexUpdater updater = proxy.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
            updater.process(IndexEntryUpdate.add(123, indexDescriptor, stringValue("some value")));
        }

        // then
        verify(indexStatisticsStore).incrementIndexUpdates(indexId, 1L);
    }

    @Test
    void shouldDropAndCreateIndexWithSameIdDuringRecovery() throws IOException {
        // given
        long indexId = 10;
        IndexDescriptor indexDescriptor = uniqueIndex.materialise(indexId);
        IndexingService indexingService =
                newIndexingServiceWithMockedDependencies(populator, accessor, withData(), indexDescriptor);
        life.init();

        // when
        indexingService.dropIndex(indexDescriptor);
        indexingService.createIndexes(AUTH_DISABLED, indexDescriptor);
        life.start();

        // then drop call two times: one from the explicit call by this test and the other from start()
        verify(accessor, times(2)).drop();
    }

    @Test
    void shouldHandleCreatingBothNodeBasedRelationshipLookupAndValueIndexesInSameCall() throws IOException {
        // given
        when(accessor.newUpdater(any(IndexUpdateMode.class), any(CursorContext.class), anyBoolean()))
                .thenReturn(updater);
        when(storageEngine.indexingBehaviour()).thenReturn(new NodeIdsForRelationshipsBehaviour());
        Set<IndexDescriptor> populationJobDescriptors = new CopyOnWriteArraySet<>();
        var indexingMonitor = new IndexMonitor.MonitorAdapter() {
            @Override
            public void indexPopulationScanStarting(IndexDescriptor[] descriptors) {
                assertThat(descriptors.length).isEqualTo(1);
                populationJobDescriptors.add(descriptors[0]);
            }
        };
        var indexingService =
                newIndexingServiceWithMockedDependencies(populator, accessor, withData(), indexingMonitor);
        life.start();

        // when
        var valueIndex = IndexPrototype.forSchema(SchemaDescriptors.forRelType(0, 0))
                .withName("rel value")
                .withIndexProvider(PROVIDER_DESCRIPTOR)
                .materialise(1);
        var lookupIndex = IndexPrototype.forSchema(ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR)
                .withName("rli")
                .withIndexProvider(PROVIDER_DESCRIPTOR)
                .withIndexType(IndexType.LOOKUP)
                .materialise(2);
        indexingService.createIndexes(SYSTEM, valueIndex, lookupIndex);

        // then
        await().atMost(10, SECONDS).until(() -> populationJobDescriptors.size() == 2);
        assertThat(populationJobDescriptors).isEqualTo(Set.of(valueIndex, lookupIndex));
    }

    /**
     * This was an issue where {@link StorageEngineIndexingBehaviour#useNodeIdsInRelationshipTokenIndex()} is {@code true},
     * a single transaction creating both the relationship type LOOKUP index and a relationship value index.
     * In a clustered scenario on an empty database and where the LOOKUP index ended up being created first,
     * the value index would be run on the applier thread (due to db being empty) and try to acquire a shared lock
     * on the lookup index, which would be exclusively locked by the transaction creating these indexes
     * (remember, in clustering it's a dedicated, i.e. different thread applying transactions).
     * <p>
     * There's a lot of ways to solve this conundrum (because there are so many things that must align), but the
     * solution of choice is to order the index population "groups" such that if there's both relationship lookup
     * index and relationship value indexes, and they're in different population groups, then the lookup index
     * will start its population after the value index.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHandleCreatingBothNodeBasedRelationshipLookupAndValueIndexesInSameCallOrderedOnEmptyDb(
            boolean lookupIndexFirst) throws IOException {
        // given
        when(accessor.newUpdater(any(IndexUpdateMode.class), any(CursorContext.class), anyBoolean()))
                .thenReturn(updater);
        when(storageEngine.indexingBehaviour()).thenReturn(new NodeIdsForRelationshipsBehaviour());
        when(storeView.isEmpty(any())).thenReturn(true);
        when(storeView.visitRelationships(any(), any(), any(), any(), anyBoolean(), anyBoolean(), any(), any()))
                .thenReturn(IndexStoreView.EMPTY_SCAN);
        List<IndexDescriptor> populationJobDescriptors = new CopyOnWriteArrayList<>();
        var threadExecutingThisTest = Thread.currentThread();
        var indexingMonitor = new IndexMonitor.MonitorAdapter() {
            @Override
            public void indexPopulationScanStarting(IndexDescriptor[] descriptors) {
                // A little sanity check that populations are started/run on the same thread
                assertThat(Thread.currentThread()).isEqualTo(threadExecutingThisTest);

                assertThat(descriptors.length).isEqualTo(1);
                populationJobDescriptors.add(descriptors[0]);
            }
        };
        var indexingService =
                newIndexingServiceWithMockedDependencies(populator, accessor, withData(), indexingMonitor);
        life.start();

        // when
        var valueIndex = IndexPrototype.forSchema(SchemaDescriptors.forRelType(0, 0))
                .withName("rel value")
                .withIndexProvider(PROVIDER_DESCRIPTOR)
                .materialise(1);
        var lookupIndex = IndexPrototype.forSchema(SchemaDescriptors.forAnyEntityTokens(EntityType.RELATIONSHIP))
                .withName("rli")
                .withIndexProvider(PROVIDER_DESCRIPTOR)
                .withIndexType(IndexType.LOOKUP)
                .materialise(2);
        var indexesToCreate = lookupIndexFirst
                ? new IndexDescriptor[] {lookupIndex, valueIndex}
                : new IndexDescriptor[] {valueIndex, lookupIndex};
        indexingService.createIndexes(SYSTEM, indexesToCreate);

        // then
        assertThat(populationJobDescriptors).isEqualTo(List.of(valueIndex, lookupIndex));
    }

    /*
     * This scenario is a semi-hypothetical scenario and yet believed to have been observed at least once in the wild:
     *
     * - an index (with internal ID X) is created.
     * - another member in the cluster starts a store copy and copies the index but not yet the schema store.
     * - index is deleted and another index (which happens to get the same internal ID X) is created as a
     *   composite index.
     * - the store copy copies the schema store, the rest and completes.
     * - now on that member the schema store says that index X is a composite index whereas the file on disk is a
     *   single-property index.
     * - replaying the transactions that happened during copy would have dropped that index and then creating the new
     *   one, so semantically it would have been fine in the end.
     *
     * The problem was that index X had status ONLINE in the schema store and so IndexingService#init went ahead
     * and just opened it, not knowing that it would open an index not matching the layout that schema store said.
     * There was already a catch for IOException, but not for other unchecked exceptions. The fix was to also
     * catch RuntimeException there.
     */
    @Test
    void shouldHandleOpeningMismatchingIndex() throws Exception {
        // given
        when(indexProvider.getInitialState(any(IndexDescriptor.class), any(CursorContext.class), any()))
                .thenReturn(ONLINE);
        when(indexProvider.getProviderDescriptor()).thenReturn(PROVIDER_DESCRIPTOR);
        when(indexProvider.getPopulator(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(),
                        any(),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenReturn(populator);
        withData().getsProcessedByStoreScanFrom(storeView);
        when(indexProvider.getOnlineAccessor(
                        eq(index), any(IndexSamplingConfig.class), any(TokenNameLookup.class), any(), any()))
                .thenThrow(new IllegalStateException("Something unexpectedly wrong with the index here"));
        when(indexProvider.getMinimalIndexAccessor(index, true)).thenReturn(mock(MinimalIndexAccessor.class));
        when(indexProvider.storeMigrationParticipant(
                        any(FileSystemAbstraction.class), any(PageCache.class), any(), any(), any()))
                .thenReturn(StoreMigrationParticipant.NOT_PARTICIPATING);
        var providerMap = life.add(new MockIndexProviderMap(indexProvider));
        var populationStarted = new AtomicBoolean();
        var populationCompleted = new AtomicBoolean();
        var initialState = new MutableObject<InternalIndexState>();
        var monitor = new IndexMonitor.MonitorAdapter() {
            @Override
            public void initialState(String databaseName, IndexDescriptor descriptor, InternalIndexState state) {
                if (descriptor.equals(index)) {
                    initialState.setValue(state);
                }
            }

            @Override
            public void indexPopulationScanStarting(IndexDescriptor[] indexDescriptors) {
                for (var descriptor : indexDescriptors) {
                    assertThat(descriptor).isEqualTo(index);
                    populationStarted.set(true);
                }
            }

            @Override
            public void populationCompleteOn(IndexDescriptor descriptor) {
                assertThat(descriptor).isEqualTo(index);
                populationCompleted.set(true);
            }
        };
        var indexingService = life.add(IndexingServiceFactory.createIndexingService(
                storageEngine,
                Config.defaults(),
                life.add(scheduler),
                providerMap,
                storeViewFactory,
                nameLookup,
                loop(iterator(index)),
                internalLogProvider,
                monitor,
                schemaState,
                indexStatisticsStore,
                new DatabaseIndexStats(),
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                writable(),
                Clocks.nanoClock(),
                mock(KernelVersionProvider.class),
                new DefaultFileSystemAbstraction(),
                EMPTY_VISIBILITY_PROVIDER));

        // when
        life.init();
        assertThat(populationStarted.get()).isFalse();
        assertThat(populationCompleted.get()).isFalse();
        assertThat(initialState.getValue()).isEqualTo(ONLINE);

        // and when
        when(indexProvider.getOnlineAccessor(
                        eq(index), any(IndexSamplingConfig.class), any(TokenNameLookup.class), any(), any()))
                .thenReturn(accessor);
        when(indexProvider.getMinimalIndexAccessor(index, true)).thenReturn(accessor);
        life.start();
        indexingService.getIndexProxy(index).awaitStoreScanCompleted(1, MINUTES);

        // then
        assertThat(populationStarted.get()).isTrue();
        assertThat(populationCompleted.get()).isTrue();
    }

    @Test
    void shouldGetUsageStatisticsFromOnlineValueIndexReader() throws Exception {
        // given
        when(accessor.newValueReader(any()))
                .thenAnswer(invocationOnMock -> new UsageReportingIndexReader(invocationOnMock.getArgument(0)));
        var indexingService = newIndexingServiceWithMockedDependencies(populator, accessor, withData());
        life.start();
        clock.forward(10, SECONDS);
        var creationTimeMillis = clock.millis();
        indexingService.createIndexes(AUTH_DISABLED, index);
        waitForIndexesToComeOnline(indexingService, index);

        // when
        var proxy = indexingService.getIndexProxy(index);
        clock.forward(1, SECONDS);
        var readerTimeMillis = clock.millis();
        try (var reader = proxy.newValueReader()) {
            reader.query(
                    new SimpleEntityValueClient(),
                    QueryContext.NULL_CONTEXT,
                    IndexQueryConstraints.unconstrained(),
                    PropertyIndexQuery.allEntries());
        }
        indexingService.reportUsageStatistics();
        var statsCaptor = ArgumentCaptor.forClass(IndexUsageStats.class);
        verify(indexStatisticsStore).addUsageStats(eq(index.getId()), statsCaptor.capture());
        assertThat(statsCaptor.getValue().trackedSince()).isEqualTo(creationTimeMillis);
        assertThat(statsCaptor.getValue().readCount()).isEqualTo(1);
        assertThat(statsCaptor.getValue().lastRead()).isEqualTo(readerTimeMillis);
    }

    @Test
    void shouldGetUsageStatisticsFromOnlineTokenIndexReader() throws Exception {
        // given
        when(accessor.newTokenReader(any()))
                .thenAnswer(invocationOnMock -> new UsageReportingIndexReader(invocationOnMock.getArgument(0)));
        var indexingService = newIndexingServiceWithMockedDependencies(populator, accessor, withData());
        life.start();
        clock.forward(10, SECONDS);
        var creationTimeMillis = clock.millis();
        indexingService.createIndexes(AUTH_DISABLED, tokenIndex);
        waitForIndexesToComeOnline(indexingService, tokenIndex);

        // when
        var proxy = indexingService.getIndexProxy(tokenIndex);
        clock.forward(1, SECONDS);
        var readerTimeMillis = clock.millis();
        try (var reader = proxy.newTokenReader()) {
            reader.query(
                    new SimpleEntityTokenClient(),
                    IndexQueryConstraints.unconstrained(),
                    new TokenPredicate(0),
                    NULL_CONTEXT);
        }
        indexingService.reportUsageStatistics();
        var statsCaptor = ArgumentCaptor.forClass(IndexUsageStats.class);
        verify(indexStatisticsStore).addUsageStats(eq(tokenIndex.getId()), statsCaptor.capture());
        assertThat(statsCaptor.getValue().trackedSince()).isEqualTo(creationTimeMillis);
        assertThat(statsCaptor.getValue().readCount()).isEqualTo(1);
        assertThat(statsCaptor.getValue().lastRead()).isEqualTo(readerTimeMillis);
    }

    @Test
    void shouldScheduleAndRunIndexUsageReportingInTheBackground() throws Exception {
        // given
        var fakeClockScheduler = new FakeClockJobScheduler();
        var indexingService = newIndexingServiceWithMockedDependencies(
                populator, accessor, withData(), IndexMonitor.NO_MONITOR, fakeClockScheduler, life);
        life.start();
        indexingService.createIndexes(AUTH_DISABLED, index, tokenIndex);
        waitForIndexesToComeOnline(indexingService, tokenIndex);
        verify(indexStatisticsStore, times(0)).addUsageStats(eq(index.getId()), any());
        verify(indexStatisticsStore, times(0)).addUsageStats(eq(tokenIndex.getId()), any());

        // when
        fakeClockScheduler.forward(IndexingService.USAGE_REPORT_FREQUENCY_SECONDS + 1, SECONDS);

        // then
        verify(indexStatisticsStore, times(1)).addUsageStats(eq(index.getId()), any());
        verify(indexStatisticsStore, times(1)).addUsageStats(eq(tokenIndex.getId()), any());
    }

    @Test
    void shouldStopBackgroundSampling() throws Exception {
        assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            var localLife = new LifeSupport();
            var indexingService = newIndexingServiceWithMockedDependencies(
                    populator, accessor, withData(), IndexMonitor.NO_MONITOR, localLife.add(scheduler), localLife);
            localLife.start();
            indexingService.createIndexes(AUTH_DISABLED, index);
            waitForIndexesToComeOnline(indexingService, index);

            IndexSampler neverEndingSampler = (cursorContext, stopped) -> {
                while (!stopped.get()) {
                    Thread.yield();
                }
                return new IndexSample();
            };
            ValueIndexReader indexReader = mock(ValueIndexReader.class);
            when(indexReader.createSampler()).thenReturn(neverEndingSampler);
            when(accessor.newValueReader(any())).thenReturn(indexReader);

            indexingService.triggerIndexSampling(backgroundRebuildAll());
            // shouldn't hang
            localLife.stop();
        });
    }

    private AtomicReference<BinaryLatch> latchedIndexPopulation() {
        AtomicReference<BinaryLatch> populationStartLatch = new AtomicReference<>(new BinaryLatch());
        scheduler.setThreadFactory(
                Group.INDEX_POPULATION, (group, parent) -> new GroupedDaemonThreadFactory(group, parent) {
                    @Override
                    public Thread newThread(Runnable job) {
                        return super.newThread(() -> {
                            populationStartLatch.get().await();
                            job.run();
                        });
                    }
                });
        return populationStartLatch;
    }

    private static IndexProxy createIndexProxyMock(long indexId) {
        IndexProxy proxy = mock(IndexProxy.class);
        IndexDescriptor descriptor = storeIndex(indexId, 1, 2, PROVIDER_DESCRIPTOR);
        when(proxy.getDescriptor()).thenReturn(descriptor);
        return proxy;
    }

    private String storedFailure() {
        ArgumentCaptor<String> reason = ArgumentCaptor.forClass(String.class);
        verify(populator).markAsFailed(reason.capture());
        return reason.getValue();
    }

    private static class ControlledIndexPopulator extends IndexPopulator.Adapter {
        private final DoubleLatch latch;

        ControlledIndexPopulator(DoubleLatch latch) {
            this.latch = latch;
        }

        @Override
        public void scanCompleted(
                PhaseTracker phaseTracker,
                PopulationWorkScheduler jobScheduler,
                IndexEntryConflictHandler conflictHandler,
                CursorContext cursorContext) {
            latch.waitForAllToStart();
        }

        @Override
        public void close(boolean populationCompletedSuccessfully, CursorContext cursorContext) {
            latch.finish();
        }
    }

    private static Answer<Void> waitForLatch(final CountDownLatch latch) {
        return invocationOnMock -> {
            latch.await();
            return null;
        };
    }

    private static Answer<ResourceIterator<Path>> newResourceIterator(final Path theFile) {
        return invocationOnMock -> asResourceIterator(iterator(theFile));
    }

    private Update addNodeUpdate(long nodeId, Object propertyValue) {
        return addNodeUpdate(nodeId, propertyValue, labelId);
    }

    private Update addNodeUpdate(long nodeId, Object propertyValue, int labelId) {
        return new Update(nodeId, new int[] {labelId}, prototype.schema().getPropertyId(), Values.of(propertyValue));
    }

    private IndexEntryUpdate<IndexDescriptor> add(long nodeId, Object propertyValue) {
        return IndexEntryUpdate.add(nodeId, index, Values.of(propertyValue));
    }

    private static IndexEntryUpdate<IndexDescriptor> add(long nodeId, Object propertyValue, IndexDescriptor index) {
        return IndexEntryUpdate.add(nodeId, index, Values.of(propertyValue));
    }

    private IndexingService newIndexingServiceWithMockedDependencies(
            IndexPopulator populator, IndexAccessor accessor, DataUpdates data, IndexDescriptor... rules)
            throws IOException {
        return newIndexingServiceWithMockedDependencies(populator, accessor, data, IndexMonitor.NO_MONITOR, rules);
    }

    private IndexingService newIndexingServiceWithMockedDependencies(
            IndexPopulator populator,
            IndexAccessor accessor,
            DataUpdates data,
            IndexMonitor monitor,
            IndexDescriptor... rules)
            throws IOException {
        return newIndexingServiceWithMockedDependencies(
                populator, accessor, data, monitor, life.add(scheduler), life, rules);
    }

    private static final IndexCapability MOCK_INDEX_CAPABILITY = mock(IndexCapability.class);

    private IndexingService newIndexingServiceWithMockedDependencies(
            IndexPopulator populator,
            IndexAccessor accessor,
            DataUpdates data,
            IndexMonitor monitor,
            JobScheduler scheduler,
            LifeSupport providedLife,
            IndexDescriptor... rules)
            throws IOException {
        when(indexProvider.getInitialState(any(IndexDescriptor.class), any(CursorContext.class), any()))
                .thenReturn(ONLINE);
        when(indexProvider.getProviderDescriptor()).thenReturn(PROVIDER_DESCRIPTOR);
        when(indexProvider.getPopulator(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(),
                        any(),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenReturn(populator);
        data.getsProcessedByStoreScanFrom(storeView);
        when(indexProvider.getOnlineAccessor(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenReturn(accessor);
        when(indexProvider.getMinimalIndexAccessor(any(), anyBoolean())).thenReturn(accessor);
        when(indexProvider.storeMigrationParticipant(
                        any(FileSystemAbstraction.class), any(PageCache.class), any(), any(), any()))
                .thenReturn(StoreMigrationParticipant.NOT_PARTICIPATING);
        when(indexProvider.completeConfiguration(any(IndexDescriptor.class), any()))
                .then(invocation -> {
                    final var descriptor = invocation.getArgument(0, IndexDescriptor.class);
                    return descriptor.getCapability().equals(IndexCapability.NO_CAPABILITY)
                            ? descriptor.withIndexCapability(MOCK_INDEX_CAPABILITY)
                            : descriptor;
                });

        MockIndexProviderMap providerMap = providedLife.add(new MockIndexProviderMap(indexProvider));
        var config = Config.defaults();
        var kernelVersionProvider = mock(KernelVersionProvider.class);
        when(kernelVersionProvider.kernelVersion()).thenReturn(KernelVersion.getLatestVersion(config));

        return providedLife.add(IndexingServiceFactory.createIndexingService(
                storageEngine,
                config,
                scheduler,
                providerMap,
                storeViewFactory,
                nameLookup,
                loop(iterator(rules)),
                internalLogProvider,
                monitor,
                schemaState,
                indexStatisticsStore,
                new DatabaseIndexStats(),
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                writable(),
                clock,
                kernelVersionProvider,
                new DefaultFileSystemAbstraction(),
                EMPTY_VISIBILITY_PROVIDER));
    }

    private static DataUpdates withData(Update... updates) {
        return new DataUpdates(Arrays.asList(updates));
    }

    private static class DataUpdates implements Answer<StoreScan> {
        private final List<Update> updates;

        DataUpdates() {
            this.updates = List.of();
        }

        DataUpdates(List<Update> updates) {
            this.updates = updates;
        }

        void getsProcessedByStoreScanFrom(IndexStoreView mock) {
            when(mock.visitNodes(
                            any(int[].class),
                            any(PropertySelection.class),
                            any(PropertyScanConsumer.class),
                            isNull(),
                            anyBoolean(),
                            anyBoolean(),
                            any(CursorContextFactory.class),
                            any()))
                    .thenAnswer(this);
            when(mock.visitRelationships(
                            any(int[].class),
                            any(PropertySelection.class),
                            any(PropertyScanConsumer.class),
                            any(TokenScanConsumer.class),
                            anyBoolean(),
                            anyBoolean(),
                            any(CursorContextFactory.class),
                            any(MemoryTracker.class)))
                    .thenAnswer(this);
        }

        @Override
        public StoreScan answer(InvocationOnMock invocation) {
            final PropertyScanConsumer consumer = invocation.getArgument(2);
            return new StoreScan() {
                private volatile boolean stop;

                @Override
                public void run(ExternalUpdatesCheck externalUpdatesCheck) {
                    if (stop || updates.isEmpty()) {
                        return;
                    }

                    var batch = consumer.newBatch();
                    updates.forEach(update ->
                            batch.addRecord(update.id, update.labels, Map.of(update.propertyId, update.propertyValue)));
                    batch.process();
                }

                @Override
                public void stop() {
                    stop = true;
                }

                @Override
                public PopulationProgress getProgress() {
                    return PopulationProgress.single(42, 100);
                }
            };
        }

        @Override
        public String toString() {
            return updates.toString();
        }
    }

    private static class TrackingIndexAccessor extends IndexAccessor.Adapter {
        private final IndexUpdater updater = mock(IndexUpdater.class);

        @Override
        public void drop() {}

        @Override
        public IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
            return updater;
        }

        @Override
        public ValueIndexReader newValueReader(IndexUsageTracking usageTracker) {
            throw new UnsupportedOperationException("Not required");
        }

        @Override
        public BoundedIterable<Long> newAllEntriesValueReader(
                long fromIdInclusive, long toIdExclusive, CursorContext cursorContext) {
            throw new UnsupportedOperationException("Not required");
        }

        @Override
        public ResourceIterator<Path> snapshotFiles() {
            throw new UnsupportedOperationException("Not required");
        }
    }

    private static IndexDescriptor storeIndex(
            long ruleId, int labelId, int propertyKeyId, IndexProviderDescriptor providerDescriptor) {
        return forSchema(forLabel(labelId, propertyKeyId))
                .withIndexProvider(providerDescriptor)
                .withName("index_" + ruleId)
                .materialise(ruleId);
    }

    private static IndexDescriptor constraintIndexRule(
            long ruleId, int labelId, int propertyKeyId, IndexProviderDescriptor providerDescriptor) {
        return uniqueForSchema(forLabel(labelId, propertyKeyId))
                .withIndexProvider(providerDescriptor)
                .withName("constraint_" + ruleId)
                .materialise(ruleId);
    }

    private static IndexDescriptor constraintIndexRule(
            long ruleId,
            int labelId,
            int propertyKeyId,
            IndexProviderDescriptor providerDescriptor,
            long constraintId) {
        return uniqueForSchema(forLabel(labelId, propertyKeyId))
                .withIndexProvider(providerDescriptor)
                .withName("constraint_" + ruleId)
                .materialise(ruleId)
                .withOwningConstraintId(constraintId);
    }

    private IndexingService createIndexServiceWithCustomIndexMap(IndexMapReference indexMapReference) {
        return new IndexingService(
                storageEngine,
                mock(IndexProxyCreator.class),
                mock(IndexProviderMap.class),
                indexMapReference,
                mock(IndexStoreViewFactory.class),
                Collections.emptyList(),
                mock(IndexSamplingController.class),
                nameLookup,
                mock(JobScheduler.class),
                mock(SchemaState.class),
                internalLogProvider,
                IndexMonitor.NO_MONITOR,
                indexStatisticsStore,
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                writable(),
                Config.defaults(),
                mock(KernelVersionProvider.class),
                new DefaultFileSystemAbstraction(),
                EMPTY_VISIBILITY_PROVIDER);
    }

    private static IndexProvider mockIndexProviderWithAccessor(IndexProviderDescriptor descriptor) throws IOException {
        IndexProvider provider = mockIndexProvider(descriptor);
        IndexAccessor indexAccessor = mock(IndexAccessor.class);
        when(provider.getOnlineAccessor(
                        any(IndexDescriptor.class),
                        any(IndexSamplingConfig.class),
                        any(TokenNameLookup.class),
                        any(),
                        any()))
                .thenReturn(indexAccessor);
        return provider;
    }

    private static IndexProvider mockIndexProvider(IndexProviderDescriptor descriptor) {
        IndexProvider provider = mock(IndexProvider.class);
        when(provider.getProviderDescriptor()).thenReturn(descriptor);
        return provider;
    }

    private static class Update {
        private final long id;
        private final int[] labels;
        private final int propertyId;
        private final Value propertyValue;

        private Update(long id, int[] labels, int propertyId, Value propertyValue) {
            this.id = id;
            this.labels = labels;
            this.propertyId = propertyId;
            this.propertyValue = propertyValue;
        }
    }

    private static class UsageReportingIndexReader implements ValueIndexReader, TokenIndexReader {
        private final IndexUsageTracking tracker;

        UsageReportingIndexReader(IndexUsageTracking tracker) {
            this.tracker = tracker;
        }

        @Override
        public long countIndexedEntities(
                long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexSampler createSampler() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void query(
                IndexProgressor.EntityValueClient client,
                QueryContext context,
                IndexQueryConstraints constraints,
                PropertyIndexQuery... query) {
            tracker.queried();
        }

        @Override
        public PartitionedValueSeek valueSeek(
                int desiredNumberOfPartitions, QueryContext queryContext, PropertyIndexQuery... query) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}

        @Override
        public void query(
                IndexProgressor.EntityTokenClient client,
                IndexQueryConstraints constraints,
                TokenPredicate query,
                CursorContext cursorContext) {
            tracker.queried();
        }

        @Override
        public void query(
                IndexProgressor.EntityTokenClient client,
                IndexQueryConstraints constraints,
                TokenPredicate query,
                EntityRange range,
                CursorContext cursorContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PartitionedTokenScan entityTokenScan(
                int desiredNumberOfPartitions, CursorContext context, TokenPredicate query) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PartitionedTokenScan entityTokenScan(PartitionedTokenScan leadingPartition, TokenPredicate query) {
            throw new UnsupportedOperationException();
        }
    }

    private static class NodeIdsForRelationshipsBehaviour implements StorageEngineIndexingBehaviour {
        @Override
        public boolean useNodeIdsInRelationshipTokenIndex() {
            return true;
        }

        @Override
        public boolean requireCoordinationLocks() {
            return false;
        }

        @Override
        public int nodesPerPage() {
            return 0;
        }

        @Override
        public int relationshipsPerPage() {
            return 0;
        }
    }
}

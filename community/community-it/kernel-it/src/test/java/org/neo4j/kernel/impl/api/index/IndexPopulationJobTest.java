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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.common.Subject.AUTH_DISABLED;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.MapUtil.genericMap;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.IndexMonitor.NO_MONITOR;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.SIMPLE_NAME_LOOKUP;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.test.PageCacheTracerAssertions.assertThatTracing;
import static org.neo4j.test.PageCacheTracerAssertions.pins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.transaction.state.storeview.IndexStoreViewFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@ImpermanentDbmsExtension
class IndexPopulationJobTest {
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private GraphDatabaseAPI db;

    private static final Label FIRST = Label.label("FIRST");
    private static final Label SECOND = Label.label("SECOND");
    private static final String name = "name";
    private static final String age = "age";
    private static final RelationshipType likes = RelationshipType.withName("likes");
    private static final RelationshipType knows = RelationshipType.withName("knows");
    private final TokenNameLookup tokenNameLookup = SIMPLE_NAME_LOOKUP;

    private Kernel kernel;
    private TokenNameLookup tokens;
    private TokenHolders tokenHolders;
    private IndexStoreView indexStoreView;
    private DatabaseSchemaState stateHolder;
    private int labelId;
    private IndexStatisticsStore indexStatisticsStore;
    private JobScheduler jobScheduler;
    private StorageEngine storageEngine;

    @BeforeEach
    void before() throws Exception {
        kernel = db.getDependencyResolver().resolveDependency(Kernel.class);
        tokens = db.getDependencyResolver().resolveDependency(TokenNameLookup.class);
        tokenHolders = db.getDependencyResolver().resolveDependency(TokenHolders.class);
        stateHolder = new DatabaseSchemaState(NullLogProvider.getInstance());
        IndexingService indexingService = db.getDependencyResolver().resolveDependency(IndexingService.class);
        indexStoreView = db.getDependencyResolver()
                .resolveDependency(IndexStoreViewFactory.class)
                .createTokenIndexStoreView(indexingService::getIndexProxy);
        indexStatisticsStore = db.getDependencyResolver().resolveDependency(IndexStatisticsStore.class);
        jobScheduler = db.getDependencyResolver().resolveDependency(JobScheduler.class);
        storageEngine = db.getDependencyResolver().resolveDependency(StorageEngine.class);

        try (KernelTransaction tx = kernel.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED)) {
            labelId = tx.tokenWrite().labelGetOrCreateForName(FIRST.name());
            tx.tokenWrite().labelGetOrCreateForName(SECOND.name());
            tx.commit();
        }
    }

    @Test
    void shouldPopulateIndexWithOneNode() throws Exception {
        // GIVEN
        String value = "Taylor";
        long nodeId = createNode(map(name, value), FIRST);
        IndexPopulator actualPopulator = indexPopulator(false);
        TrackingIndexPopulator populator = new TrackingIndexPopulator(actualPopulator);
        int label = tokenHolders.labelTokens().getIdByName(FIRST.name());
        int prop = tokenHolders.propertyKeyTokens().getIdByName(name);
        LabelSchemaDescriptor descriptor = SchemaDescriptors.forLabel(label, prop);
        IndexPopulationJob job = newIndexPopulationJob(
                populator, new FlippableIndexProxy(), EntityType.NODE, IndexPrototype.forSchema(descriptor));

        // WHEN
        job.run();

        // THEN
        IndexEntryUpdate<?> update = IndexEntryUpdate.add(nodeId, () -> descriptor, Values.of(value));

        assertTrue(populator.created);
        assertEquals(Collections.singletonList(update), populator.includedSamples);
        assertEquals(1, populator.adds.size());
        assertTrue(populator.resultSampled);
        assertTrue(populator.closeCall);
    }

    @Test
    void tracePageCacheAccessIndexWithOneNodePopulation() throws KernelException {
        var value = "value";
        long nodeId = createNode(map(name, value), FIRST);
        IndexPopulator actualPopulator = indexPopulator(false);
        TrackingIndexPopulator populator = new TrackingIndexPopulator(actualPopulator);
        int label = tokenHolders.labelTokens().getIdByName(FIRST.name());
        int prop = tokenHolders.propertyKeyTokens().getIdByName(name);
        LabelSchemaDescriptor descriptor = SchemaDescriptors.forLabel(label, prop);
        var pageCacheTracer = new DefaultPageCacheTracer();
        CursorContextFactory contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        IndexPopulationJob job = newIndexPopulationJob(
                populator,
                new FlippableIndexProxy(),
                EntityType.NODE,
                IndexPrototype.forSchema(descriptor),
                contextFactory);

        job.run();

        IndexEntryUpdate<?> update = IndexEntryUpdate.add(nodeId, () -> descriptor, Values.of(value));

        assertTrue(populator.created);
        assertEquals(Collections.singletonList(update), populator.includedSamples);
        assertEquals(1, populator.adds.size());
        assertTrue(populator.resultSampled);
        assertTrue(populator.closeCall);

        assertThatTracing(db)
                .record(pins(12).faults(2))
                .block(pins(11).faults(2))
                .matches(pageCacheTracer);
    }

    @Test
    void shouldPopulateIndexWithOneRelationship() {
        // GIVEN
        String value = "Taylor";
        long nodeId = createNode(map(name, value), FIRST);
        long relationship = createRelationship(map(name, age), likes, nodeId, nodeId);
        int relType = tokenHolders.relationshipTypeTokens().getIdByName(likes.name());
        int propertyId = tokenHolders.propertyKeyTokens().getIdByName(name);
        IndexPrototype descriptor = IndexPrototype.forSchema(SchemaDescriptors.forRelType(relType, propertyId));
        IndexPopulator actualPopulator = indexPopulator(descriptor);
        TrackingIndexPopulator populator = new TrackingIndexPopulator(actualPopulator);
        IndexPopulationJob job =
                newIndexPopulationJob(populator, new FlippableIndexProxy(), EntityType.RELATIONSHIP, descriptor);

        // WHEN
        job.run();

        // THEN
        IndexEntryUpdate<?> update = IndexEntryUpdate.add(relationship, descriptor, Values.of(age));

        assertTrue(populator.created);
        assertEquals(Collections.singletonList(update), populator.includedSamples);
        assertEquals(1, populator.adds.size());
        assertTrue(populator.resultSampled);
        assertTrue(populator.closeCall);
    }

    @Test
    void tracePageCacheAccessIndexWithOneRelationship() {
        String value = "value";
        long nodeId = createNode(map(name, value), FIRST);
        long relationship = createRelationship(map(name, age), likes, nodeId, nodeId);
        int rel = tokenHolders.relationshipTypeTokens().getIdByName(likes.name());
        int prop = tokenHolders.propertyKeyTokens().getIdByName(name);
        IndexPrototype descriptor = IndexPrototype.forSchema(SchemaDescriptors.forRelType(rel, prop));
        IndexPopulator actualPopulator = indexPopulator(descriptor);
        TrackingIndexPopulator populator = new TrackingIndexPopulator(actualPopulator);
        var pageCacheTracer = new DefaultPageCacheTracer();
        CursorContextFactory contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        IndexPopulationJob job = newIndexPopulationJob(
                populator, new FlippableIndexProxy(), EntityType.RELATIONSHIP, descriptor, contextFactory);

        job.run();

        IndexEntryUpdate<?> update = IndexEntryUpdate.add(relationship, descriptor, Values.of(age));

        assertTrue(populator.created);
        assertEquals(Collections.singletonList(update), populator.includedSamples);
        assertEquals(1, populator.adds.size());
        assertTrue(populator.resultSampled);
        assertTrue(populator.closeCall);

        assertThatTracing(db)
                .record(pins(12).faults(2))
                .block(pins(11).faults(2))
                .matches(pageCacheTracer);
    }

    @Test
    void shouldFlushSchemaStateAfterPopulation() throws Exception {
        // GIVEN
        String value = "Taylor";
        createNode(map(name, value), FIRST);
        stateHolder.put("key", "original_value");
        IndexPopulator populator = indexPopulator(false);
        IndexPopulationJob job = newIndexPopulationJob(
                populator, new FlippableIndexProxy(), EntityType.NODE, indexPrototype(FIRST, name, false));

        // WHEN
        job.run();

        // THEN
        String result = stateHolder.get("key");
        assertNull(result);
    }

    @Test
    void shouldPopulateIndexWithASmallDataset() throws Exception {
        // GIVEN
        String value = "Mattias";
        long node1 = createNode(map(name, value), FIRST);
        createNode(map(name, value), SECOND);
        createNode(map(age, 31), FIRST);
        long node4 = createNode(map(age, 35, name, value), FIRST);
        IndexPopulator actualPopulator = indexPopulator(false);
        TrackingIndexPopulator populator = new TrackingIndexPopulator(actualPopulator);
        int label = tokenHolders.labelTokens().getIdByName(FIRST.name());
        int prop = tokenHolders.propertyKeyTokens().getIdByName(name);
        LabelSchemaDescriptor descriptor = SchemaDescriptors.forLabel(label, prop);
        IndexPopulationJob job = newIndexPopulationJob(
                populator, new FlippableIndexProxy(), EntityType.NODE, IndexPrototype.forSchema(descriptor));

        // WHEN
        job.run();

        // THEN
        IndexEntryUpdate<?> update1 = add(node1, () -> descriptor, Values.of(value));
        IndexEntryUpdate<?> update2 = add(node4, () -> descriptor, Values.of(value));

        assertTrue(populator.created);
        assertEquals(Arrays.asList(update1, update2), populator.includedSamples);
        assertEquals(1, populator.adds.size());
        assertTrue(populator.resultSampled);
        assertTrue(populator.closeCall);
    }

    @Test
    void shouldPopulateRelationshipIndexWithASmallDataset() {
        // GIVEN
        String value = "Philip J.Fry";
        long node1 = createNode(map(name, value), FIRST);
        long node2 = createNode(map(name, value), SECOND);
        long node3 = createNode(map(age, 31), FIRST);
        long node4 = createNode(map(age, 35, name, value), FIRST);

        long rel1 = createRelationship(map(name, value), likes, node1, node3);
        createRelationship(map(name, value), knows, node3, node1);
        createRelationship(map(age, 31), likes, node2, node1);
        long rel4 = createRelationship(map(age, 35, name, value), likes, node4, node4);

        int rel = tokenHolders.relationshipTypeTokens().getIdByName(likes.name());
        int prop = tokenHolders.propertyKeyTokens().getIdByName(name);
        IndexPrototype descriptor = IndexPrototype.forSchema(SchemaDescriptors.forRelType(rel, prop));
        IndexPopulator actualPopulator = indexPopulator(descriptor);
        TrackingIndexPopulator populator = new TrackingIndexPopulator(actualPopulator);
        IndexPopulationJob job =
                newIndexPopulationJob(populator, new FlippableIndexProxy(), EntityType.RELATIONSHIP, descriptor);

        // WHEN
        job.run();

        // THEN
        IndexEntryUpdate<?> update1 = add(rel1, descriptor, Values.of(value));
        IndexEntryUpdate<?> update2 = add(rel4, descriptor, Values.of(value));

        assertTrue(populator.created);
        assertEquals(Arrays.asList(update1, update2), populator.includedSamples);
        assertEquals(1, populator.adds.size());
        assertTrue(populator.resultSampled);
        assertTrue(populator.closeCall);
    }

    @Test
    void shouldIndexConcurrentUpdatesWhilePopulating() throws Exception {
        // GIVEN
        Object value1 = "Mattias";
        Object value2 = "Jacob";
        Object value3 = "Stefan";
        Object changedValue = "changed";
        long node1 = createNode(map(name, value1), FIRST);
        long node2 = createNode(map(name, value2), FIRST);
        long node3 = createNode(map(name, value3), FIRST);
        @SuppressWarnings("UnnecessaryLocalVariable")
        long changeNode = node1;
        int propertyKeyId = getPropertyKeyForName(name);
        NodeChangingWriter populator = new NodeChangingWriter(changeNode, propertyKeyId, value1, changedValue, labelId);
        IndexPopulationJob job = newIndexPopulationJob(
                populator, new FlippableIndexProxy(), EntityType.NODE, indexPrototype(FIRST, name, false));
        populator.setJob(job);

        // WHEN
        job.run();

        // THEN
        Set<Pair<Long, Object>> expected = asSet(
                Pair.of(node1, value1), Pair.of(node2, value2), Pair.of(node3, value3), Pair.of(node1, changedValue));
        assertEquals(expected, populator.added);
    }

    @Test
    void shouldRemoveViaConcurrentIndexUpdatesWhilePopulating() throws Exception {
        // GIVEN
        String value1 = "Mattias";
        String value2 = "Jacob";
        String value3 = "Stefan";
        long node1 = createNode(map(name, value1), FIRST);
        long node2 = createNode(map(name, value2), FIRST);
        long node3 = createNode(map(name, value3), FIRST);
        int propertyKeyId = getPropertyKeyForName(name);
        NodeDeletingWriter populator = new NodeDeletingWriter(node2, propertyKeyId, value2, labelId);
        IndexPopulationJob job = newIndexPopulationJob(
                populator, new FlippableIndexProxy(), EntityType.NODE, indexPrototype(FIRST, name, false));
        populator.setJob(job);

        // WHEN
        job.run();

        // THEN
        Map<Long, Object> expectedAdded = genericMap(node1, value1, node2, value2, node3, value3);
        assertEquals(expectedAdded, populator.added);
        Map<Long, Object> expectedRemoved = genericMap(node2, value2);
        assertEquals(expectedRemoved, populator.removed);
    }

    @Test
    void shouldTransitionToFailedStateIfPopulationJobCrashes() throws Exception {
        // GIVEN
        IndexPopulator failingPopulator = mock(IndexPopulator.class);
        doThrow(new RuntimeException("BORK BORK"))
                .when(failingPopulator)
                .add(any(Collection.class), any(CursorContext.class));

        FlippableIndexProxy index = new FlippableIndexProxy();

        createNode(map(name, "Taylor"), FIRST);
        IndexPopulationJob job =
                newIndexPopulationJob(failingPopulator, index, EntityType.NODE, indexPrototype(FIRST, name, false));

        // WHEN
        job.run();

        // THEN
        assertThat(index.getState()).isEqualTo(InternalIndexState.FAILED);
    }

    @Test
    void shouldBeAbleToStopPopulationJob() throws Exception {
        // GIVEN
        createNode(map(name, "Mattias"), FIRST);
        IndexPopulator populator = mock(IndexPopulator.class);
        FlippableIndexProxy index = mock(FlippableIndexProxy.class);
        IndexStoreView storeView = mock(IndexStoreView.class);
        ControlledStoreScan storeScan = new ControlledStoreScan();
        when(storeView.visitNodes(
                        any(int[].class),
                        any(PropertySelection.class),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        anyBoolean(),
                        anyBoolean(),
                        any(),
                        any()))
                .thenReturn(storeScan);

        final IndexPopulationJob job = newIndexPopulationJob(
                populator,
                index,
                storeView,
                NullLogProvider.getInstance(),
                EntityType.NODE,
                indexPrototype(FIRST, name, false));
        JobHandle<?> jobHandle = mock(JobHandle.class);
        job.setHandle(jobHandle);

        Future<Void> runFuture;
        try (OtherThreadExecutor populationJobRunner = new OtherThreadExecutor("Population job test runner")) {
            runFuture = populationJobRunner.executeDontWait(() -> {
                job.run();
                return null;
            });

            storeScan.latch.waitForAllToStart();
            job.stop();
            job.awaitCompletion(0, TimeUnit.SECONDS);
            storeScan.latch.waitForAllToFinish();

            // WHEN
            runFuture.get();
        }

        // THEN
        verify(populator).close(eq(false), any());
        verify(index, never()).flip(any());
        verify(jobHandle).cancel();
    }

    @Test
    void shouldLogJobProgress() throws Exception {
        // Given
        createNode(map(name, "irrelephant"), FIRST);
        AssertableLogProvider logProvider = new AssertableLogProvider();
        FlippableIndexProxy index = mock(FlippableIndexProxy.class);
        when(index.getState()).thenReturn(InternalIndexState.ONLINE);
        IndexPopulator populator = indexPopulator(false);
        try {
            IndexPopulationJob job = newIndexPopulationJob(
                    populator, index, indexStoreView, logProvider, EntityType.NODE, indexPrototype(FIRST, name, false));

            // When
            job.run();

            // Then
            var populationLog = assertThat(logProvider).forClass(IndexPopulationJob.class);
            populationLog
                    .forLevel(INFO)
                    .containsMessages("Index population started: [%s]", "type='RANGE', schema=(:FIRST {name})");
            populationLog.forLevel(DEBUG).containsMessages("TIME/PHASE Final: SCAN[");
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }
    }

    @Test
    void logConstraintJobProgress() throws Exception {
        // Given
        createNode(map(name, "irrelephant"), FIRST);
        AssertableLogProvider logProvider = new AssertableLogProvider();
        FlippableIndexProxy index = mock(FlippableIndexProxy.class);
        when(index.getState()).thenReturn(InternalIndexState.POPULATING);
        IndexPopulator populator = indexPopulator(false);
        try {
            IndexPopulationJob job = newIndexPopulationJob(
                    populator, index, indexStoreView, logProvider, EntityType.NODE, indexPrototype(FIRST, name, true));

            // When
            job.run();

            // Then
            var populationLog = assertThat(logProvider).forClass(IndexPopulationJob.class);
            populationLog
                    .forLevel(INFO)
                    .containsMessageWithArgumentsContaining(
                            "Index population started: [%s]", "type='RANGE', schema=(:FIRST {name})");
            populationLog.forLevel(DEBUG).containsMessages("TIME/PHASE Final: SCAN[");
        } finally {
            populator.close(true, CursorContext.NULL_CONTEXT);
        }
    }

    @Test
    void shouldLogJobFailure() throws Exception {
        // Given
        createNode(map(name, "irrelephant"), FIRST);
        AssertableLogProvider logProvider = new AssertableLogProvider();
        FlippableIndexProxy index = mock(FlippableIndexProxy.class);
        IndexPopulator populator = spy(indexPopulator(false));
        IndexPopulationJob job = newIndexPopulationJob(
                populator, index, indexStoreView, logProvider, EntityType.NODE, indexPrototype(FIRST, name, false));

        Throwable failure = new IllegalStateException("not successful");
        doThrow(failure).when(populator).create();

        // When
        job.run();

        // Then
        assertThat(logProvider)
                .forClass(IndexPopulationJob.class)
                .forLevel(ERROR)
                .containsMessageWithException("Failed to populate index: [Index(", failure)
                .containsMessageWithException("type='RANGE', schema=(:FIRST {name})", failure);
    }

    @Test
    void shouldFlipToFailedUsingFailedIndexProxyFactory() throws Exception {
        // Given
        FlippableIndexProxy proxy = spy(new FlippableIndexProxy());
        IndexPopulator populator = spy(indexPopulator(false));
        IndexPopulationJob job = newIndexPopulationJob(
                populator,
                proxy,
                indexStoreView,
                NullLogProvider.getInstance(),
                EntityType.NODE,
                indexPrototype(FIRST, name, false),
                CONTEXT_FACTORY);

        IllegalStateException failure = new IllegalStateException("not successful");
        doThrow(failure).when(populator).close(eq(true), any());

        // When
        job.run();

        // Then
        verify(populator).close(eq(true), any());
        verify(proxy).flipTo(any(FailedIndexProxy.class));
    }

    @Test
    void shouldCloseAndFailOnFailure() throws Exception {
        createNode(map(name, "irrelephant"), FIRST);
        InternalLogProvider logProvider = NullLogProvider.getInstance();
        FlippableIndexProxy index = mock(FlippableIndexProxy.class);
        IndexPopulator populator = spy(indexPopulator(false));
        IndexPopulationJob job = newIndexPopulationJob(
                populator, index, indexStoreView, logProvider, EntityType.NODE, indexPrototype(FIRST, name, false));

        String failureMessage = "not successful";
        IllegalStateException failure = new IllegalStateException(failureMessage);
        doThrow(failure).when(populator).create();

        // When
        job.run();

        // Then
        verify(populator).markAsFailed(contains(failureMessage));
    }

    @Test
    void shouldCloseMultiPopulatorOnSuccessfulPopulation() {
        // given
        NullLogProvider logProvider = NullLogProvider.getInstance();
        TrackingMultipleIndexPopulator populator = new TrackingMultipleIndexPopulator(
                IndexStoreView.EMPTY,
                logProvider,
                EntityType.NODE,
                new DatabaseSchemaState(logProvider),
                jobScheduler,
                tokens);
        IndexPopulationJob populationJob = new IndexPopulationJob(
                populator,
                NO_MONITOR,
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                AUTH_DISABLED,
                EntityType.NODE,
                Config.defaults());

        // when
        populationJob.run();

        // then
        assertTrue(populator.closed);
    }

    @Test
    void shouldCloseMultiPopulatorOnFailedPopulation() {
        // given
        NullLogProvider logProvider = NullLogProvider.getInstance();
        IndexStoreView failingStoreView = new IndexStoreView.Adaptor() {
            @Override
            public StoreScan visitNodes(
                    int[] labelIds,
                    PropertySelection propertySelection,
                    PropertyScanConsumer propertyScanConsumer,
                    TokenScanConsumer labelScanConsumer,
                    boolean forceStoreScan,
                    boolean parallelWrite,
                    CursorContextFactory contextFactory,
                    MemoryTracker memoryTracker) {
                return new StoreScan() {
                    @Override
                    public void run(ExternalUpdatesCheck externalUpdatesCheck) {
                        throw new RuntimeException("Just failing");
                    }

                    @Override
                    public void stop() {}

                    @Override
                    public PopulationProgress getProgress() {
                        return null;
                    }
                };
            }
        };
        TrackingMultipleIndexPopulator populator = new TrackingMultipleIndexPopulator(
                failingStoreView,
                logProvider,
                EntityType.NODE,
                new DatabaseSchemaState(logProvider),
                jobScheduler,
                tokens);
        IndexPopulationJob populationJob = new IndexPopulationJob(
                populator,
                NO_MONITOR,
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                AUTH_DISABLED,
                EntityType.NODE,
                Config.defaults());

        // when
        populationJob.run();

        // then
        assertTrue(populator.closed);
    }

    private static class ControlledStoreScan implements StoreScan {
        private final DoubleLatch latch = new DoubleLatch();

        @Override
        public void run(ExternalUpdatesCheck externalUpdatesCheck) {
            latch.startAndWaitForAllToStartAndFinish();
        }

        @Override
        public void stop() {
            latch.finish();
        }

        @Override
        public PopulationProgress getProgress() {
            return PopulationProgress.single(42, 100);
        }
    }

    private static class NodeChangingWriter extends IndexPopulator.Adapter {
        private final Set<Pair<Long, Object>> added = new HashSet<>();
        private IndexPopulationJob job;
        private final long nodeToChange;
        private final Value newValue;
        private final Value previousValue;
        private final LabelSchemaDescriptor index;

        NodeChangingWriter(long nodeToChange, int propertyKeyId, Object previousValue, Object newValue, int label) {
            this.nodeToChange = nodeToChange;
            this.previousValue = Values.of(previousValue);
            this.newValue = Values.of(newValue);
            this.index = SchemaDescriptors.forLabel(label, propertyKeyId);
        }

        @Override
        public void add(Collection<? extends IndexEntryUpdate<?>> updates, CursorContext cursorContext) {
            for (IndexEntryUpdate<?> update : updates) {
                add((ValueIndexEntryUpdate<?>) update);
            }
        }

        void add(ValueIndexEntryUpdate<?> update) {
            if (update.getEntityId() == 2) {
                job.update(IndexEntryUpdate.change(nodeToChange, () -> index, previousValue, newValue));
            }
            added.add(Pair.of(update.getEntityId(), update.values()[0].asObjectCopy()));
        }

        @Override
        public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
            return new IndexUpdater() {
                @Override
                public void process(IndexEntryUpdate<?> update) {
                    ValueIndexEntryUpdate<?> valueUpdate = asValueUpdate(update);
                    switch (valueUpdate.updateMode()) {
                        case ADDED:
                        case CHANGED:
                            added.add(Pair.of(valueUpdate.getEntityId(), valueUpdate.values()[0].asObjectCopy()));
                            break;
                        default:
                            throw new IllegalArgumentException(
                                    valueUpdate.updateMode().name());
                    }
                }

                @Override
                public void close() {}
            };
        }

        void setJob(IndexPopulationJob job) {
            this.job = job;
        }
    }

    private static class NodeDeletingWriter extends IndexPopulator.Adapter {
        private final Map<Long, Object> added = new HashMap<>();
        private final Map<Long, Object> removed = new HashMap<>();
        private final long nodeToDelete;
        private IndexPopulationJob job;
        private final Value valueToDelete;
        private final LabelSchemaDescriptor index;

        NodeDeletingWriter(long nodeToDelete, int propertyKeyId, Object valueToDelete, int label) {
            this.nodeToDelete = nodeToDelete;
            this.valueToDelete = Values.of(valueToDelete);
            this.index = SchemaDescriptors.forLabel(label, propertyKeyId);
        }

        void setJob(IndexPopulationJob job) {
            this.job = job;
        }

        @Override
        public void add(Collection<? extends IndexEntryUpdate<?>> updates, CursorContext cursorContext) {
            for (IndexEntryUpdate<?> update : updates) {
                add((ValueIndexEntryUpdate<?>) update);
            }
        }

        void add(ValueIndexEntryUpdate<?> update) {
            if (update.getEntityId() == 2) {
                job.update(IndexEntryUpdate.remove(nodeToDelete, () -> index, valueToDelete));
            }
            added.put(update.getEntityId(), update.values()[0].asObjectCopy());
        }

        @Override
        public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
            return new IndexUpdater() {
                @Override
                public void process(IndexEntryUpdate<?> update) {
                    ValueIndexEntryUpdate<?> valueUpdate = asValueUpdate(update);
                    switch (valueUpdate.updateMode()) {
                        case ADDED:
                        case CHANGED:
                            added.put(valueUpdate.getEntityId(), valueUpdate.values()[0].asObjectCopy());
                            break;
                        case REMOVED:
                            removed.put(
                                    valueUpdate.getEntityId(),
                                    valueUpdate.values()[0].asObjectCopy()); // on remove, value is the before value
                            break;
                        default:
                            throw new IllegalArgumentException(
                                    valueUpdate.updateMode().name());
                    }
                }

                @Override
                public void close() {}
            };
        }
    }

    private IndexPopulator indexPopulator(boolean constraint) throws KernelException {
        IndexPrototype prototype = indexPrototype(FIRST, name, constraint);
        return indexPopulator(prototype);
    }

    private IndexPopulator indexPopulator(IndexPrototype prototype) {
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig(Config.defaults());
        IndexProvider indexProvider = db.getDependencyResolver()
                .resolveDependency(IndexProviderMap.class)
                .getDefaultProvider();
        IndexDescriptor indexDescriptor = prototype.withName("index_21").materialise(21);
        indexDescriptor = indexProvider.completeConfiguration(indexDescriptor, storageEngine.indexingBehaviour());
        return indexProvider.getPopulator(
                indexDescriptor,
                samplingConfig,
                heapBufferFactory(1024),
                INSTANCE,
                tokenNameLookup,
                storageEngine.getOpenOptions(),
                storageEngine.indexingBehaviour());
    }

    private IndexPopulationJob newIndexPopulationJob(
            IndexPopulator populator, FlippableIndexProxy flipper, EntityType type, IndexPrototype prototype) {
        return newIndexPopulationJob(
                populator, flipper, indexStoreView, NullLogProvider.getInstance(), type, prototype, CONTEXT_FACTORY);
    }

    private IndexPopulationJob newIndexPopulationJob(
            IndexPopulator populator,
            FlippableIndexProxy flipper,
            EntityType type,
            IndexPrototype prototype,
            CursorContextFactory contextFactory) {
        return newIndexPopulationJob(
                populator, flipper, indexStoreView, NullLogProvider.getInstance(), type, prototype, contextFactory);
    }

    private IndexPopulationJob newIndexPopulationJob(
            IndexPopulator populator,
            FlippableIndexProxy flipper,
            IndexStoreView storeView,
            InternalLogProvider logProvider,
            EntityType type,
            IndexPrototype prototype) {
        return newIndexPopulationJob(populator, flipper, storeView, logProvider, type, prototype, CONTEXT_FACTORY);
    }

    private IndexPopulationJob newIndexPopulationJob(
            IndexPopulator populator,
            FlippableIndexProxy flipper,
            IndexStoreView storeView,
            InternalLogProvider logProvider,
            EntityType type,
            IndexPrototype prototype,
            CursorContextFactory contextFactory) {
        long indexId = 0;
        flipper.setFlipTarget(mock(IndexProxyFactory.class));

        MultipleIndexPopulator multiPopulator = new MultipleIndexPopulator(
                storeView,
                logProvider,
                type,
                stateHolder,
                jobScheduler,
                tokens,
                contextFactory,
                INSTANCE,
                "",
                AUTH_DISABLED,
                Config.defaults());
        IndexPopulationJob job = new IndexPopulationJob(
                multiPopulator,
                NO_MONITOR,
                contextFactory,
                INSTANCE,
                "",
                AUTH_DISABLED,
                EntityType.NODE,
                Config.defaults());
        IndexDescriptor descriptor = prototype.withName("index_" + indexId).materialise(indexId);
        IndexProxyStrategy indexProxyStrategy = new ValueIndexProxyStrategy(descriptor, indexStatisticsStore, tokens);
        job.addPopulator(populator, indexProxyStrategy, flipper);
        return job;
    }

    private IndexPrototype indexPrototype(Label label, String propertyKey, boolean constraint) throws KernelException {
        try (KernelTransaction tx = kernel.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED)) {
            int labelId = tx.tokenWrite().labelGetOrCreateForName(label.name());
            int propertyKeyId = tx.tokenWrite().propertyKeyGetOrCreateForName(propertyKey);
            SchemaDescriptor schema = SchemaDescriptors.forLabel(labelId, propertyKeyId);
            IndexPrototype descriptor = constraint
                    ? IndexPrototype.uniqueForSchema(schema, PROVIDER_DESCRIPTOR)
                    : IndexPrototype.forSchema(schema, PROVIDER_DESCRIPTOR);
            tx.commit();
            return descriptor;
        }
    }

    private long createNode(Map<String, Object> properties, Label... labels) {
        try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
            Node node = tx.createNode(labels);
            for (Map.Entry<String, Object> property : properties.entrySet()) {
                node.setProperty(property.getKey(), property.getValue());
            }
            tx.commit();
            return node.getId();
        }
    }

    private long createRelationship(
            Map<String, Object> properties, RelationshipType relType, long fromNode, long toNode) {
        try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
            Node node1 = tx.getNodeById(fromNode);
            Node node2 = tx.getNodeById(toNode);
            Relationship relationship = node1.createRelationshipTo(node2, relType);
            for (Map.Entry<String, Object> property : properties.entrySet()) {
                relationship.setProperty(property.getKey(), property.getValue());
            }
            tx.commit();
            return relationship.getId();
        }
    }

    private int getPropertyKeyForName(String name) throws TransactionFailureException {
        try (KernelTransaction tx = kernel.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED)) {
            int result = tx.tokenRead().propertyKey(name);
            tx.commit();
            return result;
        }
    }

    private static class TrackingMultipleIndexPopulator extends MultipleIndexPopulator {
        private volatile boolean closed;

        TrackingMultipleIndexPopulator(
                IndexStoreView storeView,
                InternalLogProvider logProvider,
                EntityType type,
                SchemaState schemaState,
                JobScheduler jobScheduler,
                TokenNameLookup tokens) {
            super(
                    storeView,
                    logProvider,
                    type,
                    schemaState,
                    jobScheduler,
                    tokens,
                    CONTEXT_FACTORY,
                    INSTANCE,
                    "",
                    AUTH_DISABLED,
                    Config.defaults());
        }

        @Override
        public void close() {
            closed = true;
            super.close();
        }
    }

    private static class TrackingIndexPopulator extends IndexPopulator.Delegating {
        private volatile boolean created;
        private final List<Collection<? extends IndexEntryUpdate<?>>> adds = new ArrayList<>();
        private volatile Boolean closeCall;
        private final List<IndexEntryUpdate<?>> includedSamples = new ArrayList<>();
        private volatile boolean resultSampled;

        TrackingIndexPopulator(IndexPopulator delegate) {
            super(delegate);
        }

        @Override
        public void create() throws IOException {
            created = true;
            super.create();
        }

        @Override
        public void add(Collection<? extends IndexEntryUpdate<?>> updates, CursorContext cursorContext)
                throws IndexEntryConflictException {
            adds.add(updates);
            super.add(updates, cursorContext);
        }

        @Override
        public void close(boolean populationCompletedSuccessfully, CursorContext cursorContext) {
            closeCall = populationCompletedSuccessfully;
            super.close(populationCompletedSuccessfully, cursorContext);
        }

        @Override
        public void includeSample(IndexEntryUpdate<?> update) {
            includedSamples.add(update);
            super.includeSample(update);
        }

        @Override
        public IndexSample sample(CursorContext cursorContext) {
            resultSampled = true;
            return super.sample(cursorContext);
        }
    }
}

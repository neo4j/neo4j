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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.common.Subject.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.Values.intValue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator.IndexPopulation;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.JobSchedulerExtension;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.test.InMemoryTokens;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@ExtendWith({RandomExtension.class, JobSchedulerExtension.class})
class MultipleIndexPopulatorTest {
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    @Inject
    private RandomSupport random;

    @Inject
    private JobScheduler jobScheduler;

    private final SchemaDescriptorSupplier index1 = () -> SchemaDescriptors.forLabel(1, 1);
    private IndexStoreView indexStoreView;
    private SchemaState schemaState;
    private MultipleIndexPopulator multipleIndexPopulator;
    private IndexStatisticsStore indexStatisticsStore;
    private InMemoryTokens tokens;
    private StoreScan actualStoreScan;

    @BeforeEach
    void before() {
        indexStatisticsStore = mock(IndexStatisticsStore.class);
        indexStoreView = mock(IndexStoreView.class);
        // The returned StoreScan instance returned from this MultipleIndexPopulator is wrapped in a LoggingStoreScan
        // so this inner/actual StoreScan instance is accessible as a field for verification
        actualStoreScan = mock(StoreScan.class);
        when(indexStoreView.visitNodes(any(), any(), any(), any(), anyBoolean(), anyBoolean(), any(), any()))
                .thenReturn(actualStoreScan);
        schemaState = mock(SchemaState.class);
        tokens = new InMemoryTokens();
        multipleIndexPopulator = new MultipleIndexPopulator(
                indexStoreView,
                NullLogProvider.getInstance(),
                EntityType.NODE,
                schemaState,
                jobScheduler,
                tokens,
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                AUTH_DISABLED,
                Config.defaults());
    }

    @Test
    void disconnectedPopulationNotAbleToCreateNewIndex() throws Exception {
        IndexPopulator populator = createIndexPopulator();
        IndexPopulation indexPopulation = addPopulator(populator, 1);

        indexPopulation.disconnectAndStop(NULL_CONTEXT);

        multipleIndexPopulator.create(NULL_CONTEXT);

        verify(populator, never()).create();
    }

    @Test
    void disconnectedPopulationNotAbleToFlip() throws Exception {
        IndexPopulator populator = createIndexPopulator();
        IndexPopulation indexPopulation = addPopulator(populator, 1);

        indexPopulation.disconnectAndStop(NULL_CONTEXT);

        indexPopulation.flip(NULL_CONTEXT);

        verify(indexPopulation.populator, never()).sample(NULL_CONTEXT);
    }

    @Test
    void flippedPopulationIsNotCloseable() throws Exception {
        IndexPopulator populator = createIndexPopulator();
        IndexPopulation indexPopulation = addPopulator(populator, 1);

        indexPopulation.flip(NULL_CONTEXT);

        indexPopulation.disconnectAndStop(NULL_CONTEXT);

        verify(indexPopulation.populator, never()).close(false, NULL_CONTEXT);
    }

    @Test
    void disconnectAndDropShouldCallDropOnPopulator() throws Exception {
        IndexPopulator populator = createIndexPopulator();
        IndexPopulation indexPopulation = addPopulator(populator, 1);

        indexPopulation.disconnectAndDrop();

        verify(populator, never()).close(false, NULL_CONTEXT);
        verify(populator).drop();
    }

    @Test
    void testMultiplePopulatorsCreation() throws Exception {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();
        addPopulator(indexPopulator1, 1);
        addPopulator(indexPopulator2, 2);

        multipleIndexPopulator.create(NULL_CONTEXT);

        verify(indexPopulator1).create();
        verify(indexPopulator2).create();
    }

    @Test
    void testMultiplePopulatorCreationFailure() throws Exception {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();
        IndexPopulator indexPopulator3 = createIndexPopulator();

        doThrow(getPopulatorException()).when(indexPopulator1).create();
        doThrow(getPopulatorException()).when(indexPopulator3).create();

        addPopulator(indexPopulator1, 1);
        addPopulator(indexPopulator2, 2);
        addPopulator(indexPopulator3, 3);

        multipleIndexPopulator.create(NULL_CONTEXT);

        checkPopulatorFailure(indexPopulator1);
        checkPopulatorFailure(indexPopulator3);

        verify(indexPopulator2).create();
    }

    @Test
    void testHasPopulators() throws Exception {
        assertFalse(multipleIndexPopulator.hasPopulators());

        addPopulator(createIndexPopulator(), 42);

        assertTrue(multipleIndexPopulator.hasPopulators());
    }

    @Test
    void stoppingSinglePopulatorDoNotStopAnyOther() throws Exception {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();

        IndexPopulation populationToCancel = addPopulator(indexPopulator1, 1);
        IndexPopulation populationToKeepActive = addPopulator(indexPopulator2, 2);

        multipleIndexPopulator.create(NULL_CONTEXT);

        multipleIndexPopulator.stop(populationToCancel, NULL_CONTEXT);

        multipleIndexPopulator.createStoreScan(CONTEXT_FACTORY);

        assertTrue(multipleIndexPopulator.hasPopulators());

        multipleIndexPopulator.flipAfterStoreScan(NULL_CONTEXT);

        verify(populationToKeepActive.flipper).flip(any(Callable.class), any(FailedIndexProxyFactory.class));
    }

    @Test
    void stoppedPopulatorDoNotFlipWhenPopulationCompleted() throws Exception {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();

        IndexPopulation populationToCancel = addPopulator(indexPopulator1, 1);
        addPopulator(indexPopulator2, 2);

        multipleIndexPopulator.create(NULL_CONTEXT);

        multipleIndexPopulator.stop(populationToCancel, NULL_CONTEXT);

        multipleIndexPopulator.createStoreScan(CONTEXT_FACTORY);

        assertTrue(multipleIndexPopulator.hasPopulators());

        multipleIndexPopulator.flipAfterStoreScan(NULL_CONTEXT);

        verify(populationToCancel.flipper, never()).flip(any(Callable.class), any(FailedIndexProxyFactory.class));
    }

    @Test
    void testIndexAllNodes() throws Exception {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();

        addPopulator(indexPopulator1, 1);
        addPopulator(indexPopulator2, 2);

        multipleIndexPopulator.create(NULL_CONTEXT);
        multipleIndexPopulator.createStoreScan(CONTEXT_FACTORY);

        verify(indexStoreView)
                .visitNodes(
                        any(int[].class),
                        any(PropertySelection.class),
                        any(PropertyScanConsumer.class),
                        isNull(),
                        anyBoolean(),
                        anyBoolean(),
                        any(CursorContextFactory.class),
                        any());
    }

    @Test
    void testCancelPopulator() throws Exception {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();

        addPopulator(indexPopulator1, 1);
        addPopulator(indexPopulator2, 2);

        multipleIndexPopulator.cancel(getPopulatorException(), NULL_CONTEXT);

        checkPopulatorFailure(indexPopulator1);
        checkPopulatorFailure(indexPopulator2);
    }

    @Test
    void testCancelByPopulation() throws Exception {
        IndexPopulator populator1 = createIndexPopulator();
        IndexPopulator populator2 = createIndexPopulator();

        addPopulator(populator1, 1);
        IndexPopulation population2 = addPopulator(populator2, 2);

        multipleIndexPopulator.cancel(population2, getPopulatorException(), NULL_CONTEXT);

        verify(populator1, never()).markAsFailed(anyString());
        checkPopulatorFailure(populator2);
    }

    @Test
    void testCancelByPopulationRemovesPopulator() throws Exception {
        IndexPopulator populator1 = createIndexPopulator();
        IndexPopulator populator2 = createIndexPopulator();

        IndexPopulation population1 = addPopulator(populator1, 1);
        IndexPopulation population2 = addPopulator(populator2, 2);

        multipleIndexPopulator.cancel(population1, getPopulatorException(), NULL_CONTEXT);
        multipleIndexPopulator.cancel(population2, getPopulatorException(), NULL_CONTEXT);

        checkPopulatorFailure(populator1);
        checkPopulatorFailure(populator2);
        assertFalse(multipleIndexPopulator.hasPopulators());
    }

    @Test
    void testCancelByNonExistingPopulation() throws Exception {
        IndexPopulation nonExistingPopulation = mock(IndexPopulation.class);
        IndexPopulator populator = createIndexPopulator();

        addPopulator(populator, 1);

        multipleIndexPopulator.cancel(nonExistingPopulation, getPopulatorException(), NULL_CONTEXT);

        verify(populator, never()).markAsFailed(anyString());
    }

    @Test
    void testFlipAfterStoreScan() throws Exception {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();

        FlippableIndexProxy flipper1 = addPopulator(indexPopulator1, 1).flipper;
        FlippableIndexProxy flipper2 = addPopulator(indexPopulator2, 2).flipper;

        multipleIndexPopulator.flipAfterStoreScan(NULL_CONTEXT);

        verify(flipper1).flip(any(Callable.class), any(FailedIndexProxyFactory.class));
        verify(flipper2).flip(any(Callable.class), any(FailedIndexProxyFactory.class));
    }

    @Test
    void populationsRemovedDuringFlip() throws Exception {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();

        addPopulator(indexPopulator1, 1);
        addPopulator(indexPopulator2, 2);

        assertTrue(multipleIndexPopulator.hasPopulators());

        multipleIndexPopulator.flipAfterStoreScan(NULL_CONTEXT);

        assertFalse(multipleIndexPopulator.hasPopulators());
    }

    @Test
    void testStopPopulation() throws Exception {
        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();

        addPopulator(indexPopulator1, 1);
        addPopulator(indexPopulator2, 2);

        multipleIndexPopulator.stop(NULL_CONTEXT);

        verify(indexStatisticsStore, times(2)).setSampleStats(anyLong(), eq(new IndexSample(0, 0, 0)));
        verify(indexPopulator1).close(false, NULL_CONTEXT);
        verify(indexPopulator2).close(false, NULL_CONTEXT);
    }

    @Test
    void testIndexFlip() {
        IndexProxyFactory indexProxyFactory = mock(IndexProxyFactory.class);
        FailedIndexProxyFactory failedIndexProxyFactory = mock(FailedIndexProxyFactory.class);
        FlippableIndexProxy flipper = new FlippableIndexProxy();
        flipper.setFlipTarget(indexProxyFactory);

        IndexPopulator indexPopulator1 = createIndexPopulator();
        IndexPopulator indexPopulator2 = createIndexPopulator();
        addPopulator(indexPopulator1, 1, flipper, failedIndexProxyFactory);
        addPopulator(indexPopulator2, 2, flipper, failedIndexProxyFactory);

        when(indexPopulator1.sample(any(CursorContext.class))).thenThrow(getSampleError());

        multipleIndexPopulator.createStoreScan(CONTEXT_FACTORY);
        multipleIndexPopulator.flipAfterStoreScan(NULL_CONTEXT);

        verify(indexPopulator1).close(false, NULL_CONTEXT);
        verify(failedIndexProxyFactory).create(any(RuntimeException.class));

        verify(indexPopulator2).close(true, NULL_CONTEXT);
        verify(indexPopulator2).sample(NULL_CONTEXT);
        verify(indexStatisticsStore).setSampleStats(anyLong(), any());
        verify(schemaState).clear();
    }

    @Test
    void testMultiplePopulatorUpdater() throws Exception {
        IndexUpdater indexUpdater1 = mock(IndexUpdater.class);
        IndexPopulator indexPopulator1 = createIndexPopulator(indexUpdater1);
        IndexPopulator indexPopulator2 = createIndexPopulator();

        addPopulator(indexPopulator1, 1);
        addPopulator(indexPopulator2, 2);

        doThrow(getPopulatorException()).when(indexPopulator2).newPopulatingUpdater(any());

        IndexUpdater multipleIndexUpdater = multipleIndexPopulator.newPopulatingUpdater(NULL_CONTEXT);
        IndexEntryUpdate<?> propertyUpdate = createIndexEntryUpdate(index1);
        multipleIndexUpdater.process(propertyUpdate);

        checkPopulatorFailure(indexPopulator2);
        verify(indexUpdater1).process(propertyUpdate);
    }

    @Test
    void testNonApplicableUpdaterDoNotUpdatePopulator() throws Exception {
        IndexUpdater indexUpdater1 = mock(IndexUpdater.class);
        IndexPopulator indexPopulator1 = createIndexPopulator(indexUpdater1);

        addPopulator(indexPopulator1, 2);

        IndexUpdater multipleIndexUpdater = multipleIndexPopulator.newPopulatingUpdater(NULL_CONTEXT);

        IndexEntryUpdate<?> propertyUpdate = createIndexEntryUpdate(index1);
        multipleIndexUpdater.process(propertyUpdate);

        verifyNoInteractions(indexUpdater1);
    }

    @Test
    void testPropertyUpdateFailure() throws Exception {
        IndexEntryUpdate<?> propertyUpdate = createIndexEntryUpdate(index1);
        IndexUpdater indexUpdater1 = mock(IndexUpdater.class);
        IndexPopulator indexPopulator1 = createIndexPopulator(indexUpdater1);

        addPopulator(indexPopulator1, 1);

        doThrow(getPopulatorException()).when(indexUpdater1).process(propertyUpdate);

        IndexUpdater multipleIndexUpdater = multipleIndexPopulator.newPopulatingUpdater(NULL_CONTEXT);

        multipleIndexUpdater.process(propertyUpdate);

        verify(indexUpdater1).close();
        checkPopulatorFailure(indexPopulator1);
    }

    @Test
    void testMultiplePropertyUpdateFailures() throws Exception {
        IndexEntryUpdate<?> update1 = add(1, index1, "foo");
        IndexEntryUpdate<?> update2 = add(2, index1, "bar");
        IndexUpdater updater = mock(IndexUpdater.class);
        IndexPopulator populator = createIndexPopulator(updater);

        addPopulator(populator, 1);

        doThrow(getPopulatorException()).when(updater).process(any(IndexEntryUpdate.class));

        IndexUpdater multipleIndexUpdater = multipleIndexPopulator.newPopulatingUpdater(NULL_CONTEXT);

        multipleIndexUpdater.process(update1);
        multipleIndexUpdater.process(update2);

        verify(updater).process(update1);
        verify(updater, never()).process(update2);
        verify(updater).close();
        checkPopulatorFailure(populator);
    }

    @Test
    void shouldIncludeIndexSampleUpdatesInStatsOnFlip() {
        IndexProxyFactory indexProxyFactory = mock(IndexProxyFactory.class);
        FailedIndexProxyFactory failedIndexProxyFactory = mock(FailedIndexProxyFactory.class);
        FlippableIndexProxy flipper = new FlippableIndexProxy();
        flipper.setFlipTarget(indexProxyFactory);

        IndexPopulator indexPopulator = createIndexPopulator();
        addPopulator(indexPopulator, 1, flipper, failedIndexProxyFactory);

        int indexSize = 100;
        int uniqueValues = 110;
        int sampleSize = 120;
        int updates = 130;
        IndexSample sample = new IndexSample(indexSize, uniqueValues, sampleSize, updates);
        when(indexPopulator.sample(any(CursorContext.class))).thenReturn(sample);

        multipleIndexPopulator.createStoreScan(CONTEXT_FACTORY);
        multipleIndexPopulator.flipAfterStoreScan(NULL_CONTEXT);

        verify(indexPopulator).close(true, NULL_CONTEXT);

        verify(indexStatisticsStore).setSampleStats(1, sample);
        verify(schemaState).clear();
    }

    @Test
    void shouldApplyConcurrentUpdatesEarlierWhenHittingMaxBatchByteSize() {
        // given
        createIndexPopulator();
        multipleIndexPopulator.create(NULL_CONTEXT);
        String largeString = random.nextAlphaNumericString(100_000, 100_000);
        int roughlyNumUpdates = (int) (multipleIndexPopulator.batchMaxByteSizeScan / HeapEstimator.sizeOf(largeString));
        Value largeStringValue = Values.stringValue(largeString);
        IndexDescriptor indexDescriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(0, 1))
                .withName("name")
                .materialise(99);
        multipleIndexPopulator.createStoreScan(CONTEXT_FACTORY);
        boolean full = false;

        // when
        for (int i = 0; !full && i < roughlyNumUpdates * 2; i++) {
            IndexEntryUpdate<IndexDescriptor> largeUpdate = IndexEntryUpdate.add(i, indexDescriptor, largeStringValue);
            multipleIndexPopulator.queueConcurrentUpdate(largeUpdate);
            full = multipleIndexPopulator.needToApplyExternalUpdates();
            if (full) {
                multipleIndexPopulator.applyExternalUpdates(Long.MAX_VALUE);
            }
        }

        // then
        assertThat(full).isTrue();
    }

    @Test
    void updateForHigherNodeIgnoredWhenUsingFullNodeStoreScan() throws Exception {
        // given
        createIndexPopulator();
        multipleIndexPopulator.create(NULL_CONTEXT);
        IndexUpdater updater = mock(IndexUpdater.class);
        IndexPopulator populator = createIndexPopulator(updater);
        IndexUpdater indexUpdater = mock(IndexUpdater.class);
        var schema = SchemaDescriptors.forLabel(1, 1);
        addPopulator(populator, 1);

        // when external updates comes in
        var lowUpdate = IndexEntryUpdate.add(10, () -> schema, intValue(99));
        var highUpdate = IndexEntryUpdate.add(20, () -> schema, intValue(101));
        multipleIndexPopulator.queueConcurrentUpdate(lowUpdate);
        multipleIndexPopulator.queueConcurrentUpdate(highUpdate);

        // and we ask to apply them, given an entity in between the two
        multipleIndexPopulator.applyExternalUpdates(15);

        // then only the lower one should be applied, the higher one ignored
        verify(populator, times(1)).newPopulatingUpdater(any());
        verify(updater).process(lowUpdate);
        verify(updater, never()).process(highUpdate);

        verify(indexUpdater, never()).process(any(IndexEntryUpdate.class));
    }

    @Test
    void shouldStopStoreScanWhenLastPopulatorGetsDropped() throws Exception {
        shouldStopStoreScanWhenNoMorePopulatorsLeft(
                population -> multipleIndexPopulator.dropIndexPopulation(population));
    }

    @Test
    void shouldStopStoreScanWhenLastPopulatorGetsStopped() throws Exception {
        shouldStopStoreScanWhenNoMorePopulatorsLeft(
                population -> multipleIndexPopulator.stop(population, NULL_CONTEXT));
    }

    private void shouldStopStoreScanWhenNoMorePopulatorsLeft(Consumer<IndexPopulation> cancelAction) throws Exception {
        // given
        IndexPopulator populator1 = createIndexPopulator();
        IndexPopulator populator2 = createIndexPopulator();
        IndexPopulation population1 = addPopulator(populator1, 1);
        IndexPopulation population2 = addPopulator(populator2, 2);
        multipleIndexPopulator.create(NULL_CONTEXT);

        // when
        StoreScan storeScan = multipleIndexPopulator.createStoreScan(CONTEXT_FACTORY);
        storeScan.run(StoreScan.NO_EXTERNAL_UPDATES);
        cancelAction.accept(population1);
        verify(actualStoreScan, never()).stop();

        // then
        cancelAction.accept(population2);
        verify(actualStoreScan).stop();

        // and when
        multipleIndexPopulator.close();

        // then
        verify(actualStoreScan).close();
    }

    private static IndexEntryUpdate<?> createIndexEntryUpdate(SchemaDescriptorSupplier schemaDescriptor) {
        return add(1, schemaDescriptor, "theValue");
    }

    private static RuntimeException getSampleError() {
        return new RuntimeException("sample error");
    }

    private static IndexPopulator createIndexPopulator(IndexUpdater indexUpdater) {
        IndexPopulator indexPopulator = createIndexPopulator();
        when(indexPopulator.newPopulatingUpdater(any())).thenReturn(indexUpdater);
        return indexPopulator;
    }

    private static IndexPopulator createIndexPopulator() {
        IndexPopulator populator = mock(IndexPopulator.class);
        when(populator.sample(any(CursorContext.class))).thenReturn(new IndexSample());
        return populator;
    }

    private static UncheckedIOException getPopulatorException() {
        return new UncheckedIOException(new IOException("something went wrong"));
    }

    private static void checkPopulatorFailure(IndexPopulator populator) {
        verify(populator).markAsFailed(contains("something went wrong"));
        verify(populator).close(false, NULL_CONTEXT);
    }

    private IndexPopulation addPopulator(
            IndexPopulator indexPopulator,
            int id,
            FlippableIndexProxy flippableIndexProxy,
            FailedIndexProxyFactory failedIndexProxyFactory) {
        return addPopulator(multipleIndexPopulator, indexPopulator, id, flippableIndexProxy, failedIndexProxyFactory);
    }

    private IndexPopulation addPopulator(
            MultipleIndexPopulator multipleIndexPopulator,
            IndexPopulator indexPopulator,
            int id,
            FlippableIndexProxy flippableIndexProxy,
            FailedIndexProxyFactory failedIndexProxyFactory) {
        IndexDescriptor descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(id, id))
                .withName("index_" + id)
                .materialise(id);
        return addPopulator(
                multipleIndexPopulator, descriptor, indexPopulator, flippableIndexProxy, failedIndexProxyFactory);
    }

    private IndexPopulation addPopulator(
            MultipleIndexPopulator multipleIndexPopulator,
            IndexDescriptor descriptor,
            IndexPopulator indexPopulator,
            FlippableIndexProxy flippableIndexProxy,
            FailedIndexProxyFactory failedIndexProxyFactory) {
        IndexProxyStrategy indexProxyStrategy = new ValueIndexProxyStrategy(descriptor, indexStatisticsStore, tokens);
        return multipleIndexPopulator.addPopulator(
                indexPopulator, indexProxyStrategy, flippableIndexProxy, failedIndexProxyFactory);
    }

    private IndexPopulation addPopulator(IndexPopulator indexPopulator, int id) throws Exception {
        FlippableIndexProxy indexProxy = mock(FlippableIndexProxy.class);
        when(indexProxy.getState()).thenReturn(InternalIndexState.ONLINE);
        doAnswer(invocation -> {
                    Callable argument = invocation.getArgument(0);
                    return argument.call();
                })
                .when(indexProxy)
                .flip(any(Callable.class), any(FailedIndexProxyFactory.class));
        return addPopulator(indexPopulator, id, indexProxy, mock(FailedIndexProxyFactory.class));
    }
}

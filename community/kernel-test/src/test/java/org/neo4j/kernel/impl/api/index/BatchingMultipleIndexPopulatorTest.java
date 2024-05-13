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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.common.Subject.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;
import static org.neo4j.kernel.impl.api.index.StoreScan.NO_EXTERNAL_UPDATES;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.transaction.state.storeview.FullScanStoreView;
import org.neo4j.lock.LockService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.JobSchedulerExtension;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.InMemoryTokens;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.scheduler.CallingThreadJobScheduler;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@ExtendWith(JobSchedulerExtension.class)
public class BatchingMultipleIndexPopulatorTest {
    private static final int propertyId = 1;
    private static final int labelId = 1;
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    @Inject
    private JobScheduler jobScheduler;

    private final IndexDescriptor index1 = TestIndexDescriptorFactory.forLabel(1, 1);
    private final IndexDescriptor index42 = TestIndexDescriptorFactory.forLabel(42, 42);
    private final InMemoryTokens tokens = new InMemoryTokens();

    @Test
    void populateFromQueueDoesNothingIfThresholdNotReached() throws Exception {
        MultipleIndexPopulator batchingPopulator = new MultipleIndexPopulator(
                mock(IndexStoreView.class),
                NullLogProvider.getInstance(),
                EntityType.NODE,
                mock(SchemaState.class),
                new CallingThreadJobScheduler(),
                tokens,
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                AUTH_DISABLED,
                Config.defaults(GraphDatabaseInternalSettings.index_population_queue_threshold, 5));

        IndexPopulator populator = addPopulator(batchingPopulator, index1);
        IndexUpdater updater = mock(IndexUpdater.class);
        when(populator.newPopulatingUpdater(any())).thenReturn(updater);

        IndexEntryUpdate<?> update1 = add(1, index1, "foo");
        IndexEntryUpdate<?> update2 = add(2, index1, "bar");
        batchingPopulator.queueConcurrentUpdate(update1);
        batchingPopulator.queueConcurrentUpdate(update2);

        assertThat(batchingPopulator.needToApplyExternalUpdates()).isFalse();

        verify(updater, never()).process(any(ValueIndexEntryUpdate.class));
        verify(populator, never()).newPopulatingUpdater(any());
    }

    @Test
    void populateFromQueuePopulatesWhenThresholdReached() throws Exception {
        var storageEngine = mock(StorageEngine.class);
        var reader = mock(StorageReader.class);
        when(storageEngine.newReader()).thenReturn(reader);
        var indexingBehaviour = mock(StorageEngineIndexingBehaviour.class);
        when(storageEngine.indexingBehaviour()).thenReturn(indexingBehaviour);
        when(storageEngine.createStorageCursors(any())).thenReturn(StoreCursors.NULL);
        when(storageEngine.getOpenOptions()).thenReturn(Sets.immutable.empty());
        FullScanStoreView storeView =
                new FullScanStoreView(LockService.NO_LOCK_SERVICE, storageEngine, Config.defaults(), jobScheduler);
        MultipleIndexPopulator batchingPopulator = new MultipleIndexPopulator(
                storeView,
                NullLogProvider.getInstance(),
                EntityType.NODE,
                mock(SchemaState.class),
                new CallingThreadJobScheduler(),
                tokens,
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                AUTH_DISABLED,
                Config.defaults(GraphDatabaseInternalSettings.index_population_queue_threshold, 2));

        IndexPopulator populator1 = addPopulator(batchingPopulator, index1);
        IndexUpdater updater1 = mock(IndexUpdater.class);
        when(populator1.newPopulatingUpdater(any())).thenReturn(updater1);

        IndexPopulator populator2 = addPopulator(batchingPopulator, index42);
        IndexUpdater updater2 = mock(IndexUpdater.class);
        when(populator2.newPopulatingUpdater(any())).thenReturn(updater2);

        batchingPopulator.createStoreScan(CONTEXT_FACTORY);
        IndexEntryUpdate<?> update1 = add(1, index1, "foo");
        IndexEntryUpdate<?> update2 = add(2, index42, "bar");
        IndexEntryUpdate<?> update3 = add(3, index1, "baz");
        batchingPopulator.queueConcurrentUpdate(update1);
        batchingPopulator.queueConcurrentUpdate(update2);
        batchingPopulator.queueConcurrentUpdate(update3);

        batchingPopulator.applyExternalUpdates(42);

        verify(updater1).process(update1);
        verify(updater1).process(update3);
        verify(updater2).process(update2);
    }

    @Test
    void pendingBatchesFlushedAfterStoreScan() throws Exception {
        Update update1 = nodeUpdate(1, propertyId, "foo", labelId);
        Update update2 = nodeUpdate(2, propertyId, "bar", labelId);
        Update update3 = nodeUpdate(3, propertyId, "baz", labelId);
        Update update42 = nodeUpdate(4, 42, "42", 42);
        IndexStoreView storeView = newStoreView(update1, update2, update3, update42);

        MultipleIndexPopulator batchingPopulator = new MultipleIndexPopulator(
                storeView,
                NullLogProvider.getInstance(),
                EntityType.NODE,
                mock(SchemaState.class),
                new CallingThreadJobScheduler(),
                tokens,
                CONTEXT_FACTORY,
                INSTANCE,
                "",
                AUTH_DISABLED,
                Config.defaults());

        IndexPopulator populator1 = addPopulator(batchingPopulator, index1);
        IndexPopulator populator42 = addPopulator(batchingPopulator, index42);

        batchingPopulator.createStoreScan(CONTEXT_FACTORY).run(NO_EXTERNAL_UPDATES);

        verify(populator1).add(eq(forUpdates(index1, update1, update2, update3)), any());
        verify(populator42).add(eq(forUpdates(index42, update42)), any());
    }

    @Test
    void populatorMarkedAsFailed() throws Exception {
        Update update1 = nodeUpdate(1, propertyId, "aaa", labelId);
        Update update2 = nodeUpdate(1, propertyId, "bbb", labelId);
        IndexStoreView storeView = newStoreView(update1, update2);

        RuntimeException batchFlushError = new RuntimeException("Batch failed");

        IndexPopulator populator;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ThreadPoolJobScheduler jobScheduler = new ThreadPoolJobScheduler(executor);
        try {
            MultipleIndexPopulator batchingPopulator = new MultipleIndexPopulator(
                    storeView,
                    NullLogProvider.getInstance(),
                    EntityType.NODE,
                    mock(SchemaState.class),
                    jobScheduler,
                    tokens,
                    CONTEXT_FACTORY,
                    INSTANCE,
                    "",
                    AUTH_DISABLED,
                    Config.defaults(GraphDatabaseInternalSettings.index_population_batch_max_byte_size, 1L));

            populator = addPopulator(batchingPopulator, index1);
            List<IndexEntryUpdate<IndexDescriptor>> expected = forUpdates(index1, update1, update2);
            doThrow(batchFlushError).when(populator).add(eq(expected), any());

            batchingPopulator.createStoreScan(CONTEXT_FACTORY).run(NO_EXTERNAL_UPDATES);
        } finally {
            jobScheduler.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }

        verify(populator).markAsFailed(failure(batchFlushError).asString());
    }

    private static List<IndexEntryUpdate<IndexDescriptor>> forUpdates(IndexDescriptor index, Update... updates) {
        var entityUpdates = Arrays.stream(updates)
                .map(update -> EntityUpdates.forEntity(update.id, true)
                        .withTokens(update.labels)
                        .added(update.propertyId, update.propertyValue)
                        .build())
                .collect(Collectors.toList());
        return Iterables.asList(Iterables.concat(
                Iterables.map(update -> update.valueUpdatesForIndexKeys(Iterables.asIterable(index)), entityUpdates)));
    }

    private static Update nodeUpdate(int nodeId, int propertyId, String propertyValue, int... labelIds) {
        return new Update(nodeId, labelIds, propertyId, Values.stringValue(propertyValue));
    }

    private IndexPopulator addPopulator(MultipleIndexPopulator batchingPopulator, IndexDescriptor descriptor) {
        IndexPopulator populator = mock(IndexPopulator.class);

        IndexProxyFactory indexProxyFactory = mock(IndexProxyFactory.class);
        FlippableIndexProxy flipper = new FlippableIndexProxy();
        flipper.setFlipTarget(indexProxyFactory);

        IndexProxyStrategy indexProxyStrategy =
                new ValueIndexProxyStrategy(descriptor, mock(IndexStatisticsStore.class), tokens);
        batchingPopulator.addPopulator(populator, indexProxyStrategy, flipper);

        return populator;
    }

    private static IndexStoreView newStoreView(Update... updates) {
        IndexStoreView storeView = mock(IndexStoreView.class);
        when(storeView.visitNodes(any(), any(), any(), any(), anyBoolean(), anyBoolean(), any(), any()))
                .thenAnswer(invocation -> {
                    PropertyScanConsumer consumerArg = invocation.getArgument(2);
                    return new IndexEntryUpdateScan(updates, consumerArg);
                });
        return storeView;
    }

    private static class IndexEntryUpdateScan implements StoreScan {
        final Update[] updates;
        final PropertyScanConsumer consumer;

        boolean stop;

        IndexEntryUpdateScan(Update[] updates, PropertyScanConsumer consumer) {
            this.updates = updates;
            this.consumer = consumer;
        }

        @Override
        public void run(ExternalUpdatesCheck externalUpdatesCheck) {
            if (stop) {
                return;
            }
            var batch = consumer.newBatch();
            Arrays.stream(updates)
                    .forEach(update ->
                            batch.addRecord(update.id, update.labels, Map.of(update.propertyId, update.propertyValue)));
            batch.process();
        }

        @Override
        public void stop() {
            stop = true;
        }

        @Override
        public PopulationProgress getProgress() {
            return PopulationProgress.NONE;
        }
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
}

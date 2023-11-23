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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.common.Subject.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.api.index.StoreScan.NO_EXTERNAL_UPDATES;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.schema.TokenIndexProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.JobSchedulerExtension;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.InMemoryTokens;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Values;

@ExtendWith(JobSchedulerExtension.class)
class TokenIndexPopulationTest {
    private final IndexStoreView storeView = mock(IndexStoreView.class);
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    private final IndexDescriptor valueIndex = TestIndexDescriptorFactory.forLabel(1, 1);
    private final IndexPopulator valueIndexPopulator = mock(IndexPopulator.class);

    private IndexDescriptor tokenIndex;
    private final IndexPopulator tokenIndexPopulator = mock(IndexPopulator.class);

    private final ArgumentCaptor<Collection<? extends IndexEntryUpdate<?>>> indexUpdates =
            ArgumentCaptor.forClass(Collection.class);

    private MultipleIndexPopulator multipleIndexPopulator;

    @Inject
    private JobScheduler jobScheduler;

    @BeforeEach
    void beforeEach() {
        tokenIndex = IndexPrototype.forSchema(
                        SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR, TokenIndexProvider.DESCRIPTOR)
                .withName("label_index")
                .withIndexType(IndexType.LOOKUP)
                .materialise(123);

        multipleIndexPopulator = new MultipleIndexPopulator(
                storeView,
                NullLogProvider.getInstance(),
                EntityType.NODE,
                mock(SchemaState.class),
                jobScheduler,
                new InMemoryTokens(),
                CONTEXT_FACTORY,
                EmptyMemoryTracker.INSTANCE,
                "",
                AUTH_DISABLED,
                Config.defaults());
    }

    @Test
    void testBasicTokenIndexPopulation() throws Exception {
        addIndexPopulator(tokenIndexPopulator, tokenIndex);

        mockTokenStore(batch -> {
            batch.addRecord(1, new int[] {123});
            batch.addRecord(2, new int[] {123, 111});
            batch.addRecord(3, new int[] {111});
        });

        multipleIndexPopulator.create(CursorContext.NULL_CONTEXT);
        multipleIndexPopulator.createStoreScan(CONTEXT_FACTORY).run(NO_EXTERNAL_UPDATES);

        verify(tokenIndexPopulator).add(indexUpdates.capture(), any());

        var indexUpdateBatches = indexUpdates.getAllValues();
        assertEquals(1, indexUpdateBatches.size());
        Set<? extends IndexEntryUpdate<?>> indexEntryUpdates = new HashSet<>(indexUpdateBatches.get(0));
        Set<? extends IndexEntryUpdate<?>> expectedUpdates = Set.of(
                IndexEntryUpdate.change(1, tokenIndex, new int[] {}, new int[] {123}),
                IndexEntryUpdate.change(2, tokenIndex, new int[] {}, new int[] {123, 111}),
                IndexEntryUpdate.change(3, tokenIndex, new int[] {}, new int[] {111}));

        assertEquals(expectedUpdates, indexEntryUpdates);
    }

    /*
     * Generally EntityUpdates, if generated by the store view, provide superset
     * of the information provided by EntityTokenUpdates.
     * This test tests that token index population reacts only to EntityTokenUpdates,
     * so one token change does not potentially result in two events in token index populator.
     */
    @Test
    void tokenIndexPopulationShouldIgnoreEntityUpdates() throws Exception {
        addIndexPopulator(tokenIndexPopulator, tokenIndex);
        addIndexPopulator(valueIndexPopulator, valueIndex);

        // of course a real store would also generate
        // TokenIndexEntryUpdate for ID  1 and tokens long[]{1}
        // in this situation, but we want to test that the token index population
        // is driven only by TokenIndexEntryUpdates and ignores EntityUpdates
        mockPropertyStore(batch -> batch.addRecord(1, new int[] {1}, Map.of(1, Values.stringValue("Hello"))));

        multipleIndexPopulator.create(CursorContext.NULL_CONTEXT);
        multipleIndexPopulator.createStoreScan(CONTEXT_FACTORY).run(NO_EXTERNAL_UPDATES);

        verify(tokenIndexPopulator, never()).add(indexUpdates.capture(), any());
        verify(valueIndexPopulator).add(any(), any());
    }

    @Test
    void shouldNotPassConsumerForValueIndexUpdatesToStoreWhenNoValueIndexPopulating() {
        addIndexPopulator(tokenIndexPopulator, tokenIndex);

        mockTokenStore(batch -> batch.addRecord(1, new int[] {123}));

        multipleIndexPopulator.create(CursorContext.NULL_CONTEXT);
        multipleIndexPopulator.createStoreScan(CONTEXT_FACTORY).run(NO_EXTERNAL_UPDATES);

        verify(storeView).visitNodes(any(), any(), isNull(), any(), anyBoolean(), anyBoolean(), any(), any());
    }

    @Test
    void shouldNotPassConsumerForTokenIndexUpdatesToStoreWhenNoTokenIndexPopulating() {
        addIndexPopulator(valueIndexPopulator, valueIndex);

        mockPropertyStore(batch -> batch.addRecord(1, new int[] {1}, Map.of(1, Values.stringValue("Hello"))));

        multipleIndexPopulator.create(CursorContext.NULL_CONTEXT);
        multipleIndexPopulator.createStoreScan(CONTEXT_FACTORY).run(NO_EXTERNAL_UPDATES);

        verify(storeView).visitNodes(any(), any(), any(), isNull(), anyBoolean(), anyBoolean(), any(), any());
    }

    private void mockPropertyStore(Consumer<PropertyScanConsumer.Batch> updates) {
        when(storeView.visitNodes(any(), any(), any(), any(), anyBoolean(), anyBoolean(), any(), any()))
                .thenAnswer(invocation -> {
                    PropertyScanConsumer consumerArg = invocation.getArgument(2);
                    return new IndexEntryUpdateScan(() -> {
                        if (consumerArg != null) {
                            var batch = consumerArg.newBatch();
                            updates.accept(batch);
                            batch.process();
                        }
                    });
                });
    }

    private void mockTokenStore(Consumer<TokenScanConsumer.Batch> updates) {
        when(storeView.visitNodes(any(), any(), any(), any(), anyBoolean(), anyBoolean(), any(), any()))
                .thenAnswer(invocation -> {
                    TokenScanConsumer consumerArg = invocation.getArgument(3);
                    return new IndexEntryUpdateScan(() -> {
                        if (consumerArg != null) {
                            var batch = consumerArg.newBatch();
                            updates.accept(batch);
                            batch.process();
                        }
                    });
                });
    }

    private void addIndexPopulator(IndexPopulator populator, IndexDescriptor descriptor) {
        IndexProxyStrategy indexProxyStrategy;
        if (descriptor.getIndexType() == IndexType.LOOKUP) {
            indexProxyStrategy = new TokenIndexProxyStrategy(descriptor, new InMemoryTokens());
        } else {
            indexProxyStrategy = new ValueIndexProxyStrategy(
                    TestIndexDescriptorFactory.forLabel(1, 1), mock(IndexStatisticsStore.class), new InMemoryTokens());
        }

        multipleIndexPopulator.addPopulator(
                populator, indexProxyStrategy, mock(FlippableIndexProxy.class), mock(FailedIndexProxyFactory.class));
    }

    private static class IndexEntryUpdateScan implements StoreScan {
        final Runnable action;

        IndexEntryUpdateScan(Runnable action) {
            this.action = action;
        }

        @Override
        public void run(ExternalUpdatesCheck externalUpdatesCheck) {
            action.run();
        }

        @Override
        public void stop() {}

        @Override
        public PopulationProgress getProgress() {
            return PopulationProgress.NONE;
        }
    }
}

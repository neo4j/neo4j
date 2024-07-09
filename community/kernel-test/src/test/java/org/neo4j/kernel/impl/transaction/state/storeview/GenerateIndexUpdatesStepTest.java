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
package org.neo4j.kernel.impl.transaction.state.storeview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.batchimport.api.Configuration.DEFAULT;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.SimpleStageControl;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.state.storeview.GenerateIndexUpdatesStep.GeneratedIndexUpdates;
import org.neo4j.lock.Lock;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.storable.Value;

class GenerateIndexUpdatesStepTest {
    private static final LongFunction<Lock> NO_LOCKING = id -> null;

    private static final int LABEL = 1;
    private static final int OTHER_LABEL = 2;
    private static final String KEY = "key";
    private static final String OTHER_KEY = "other_key";
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldSendSingleBatchIfBelowMaxSizeThreshold(boolean alsoWrite) throws Exception {
        // given
        StubStorageCursors data = someUniformData(10);
        TestPropertyScanConsumer scanConsumer = new TestPropertyScanConsumer();
        GenerateIndexUpdatesStep<StorageNodeCursor> step = new GenerateIndexUpdatesStep(
                new SimpleStageControl(),
                DEFAULT,
                data,
                any -> StoreCursors.NULL,
                PropertySelection.ALL_PROPERTIES,
                new NodeCursorBehaviour(data),
                new int[] {LABEL},
                scanConsumer,
                null,
                NO_LOCKING,
                1,
                mebiBytes(1),
                alsoWrite,
                CONTEXT_FACTORY,
                INSTANCE);

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process(allNodeIds(data), sender, NULL_CONTEXT);

        // then
        if (alsoWrite) {
            assertThat(sender.batches).isEmpty();
            assertThat(scanConsumer.batches.size()).isEqualTo(1);
            assertThat(scanConsumer.batches.get(0).size()).isEqualTo(10);
        } else {
            assertThat(sender.batches.size()).isEqualTo(1);
            assertThat(scanConsumer.batches).isEmpty();
        }
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldSendBatchesOverMaxByteSizeThreshold(boolean alsoWrite) throws Exception {
        // given
        StubStorageCursors data = someUniformData(10);
        TestPropertyScanConsumer scanConsumer = new TestPropertyScanConsumer();
        GenerateIndexUpdatesStep<StorageNodeCursor> step = new GenerateIndexUpdatesStep<>(
                new SimpleStageControl(),
                DEFAULT,
                data,
                any -> StoreCursors.NULL,
                PropertySelection.ALL_PROPERTIES,
                new NodeCursorBehaviour(data),
                new int[] {LABEL},
                scanConsumer,
                null,
                NO_LOCKING,
                1,
                100,
                alsoWrite,
                CONTEXT_FACTORY,
                INSTANCE);

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process(allNodeIds(data), sender, NULL_CONTEXT);

        // then
        if (alsoWrite) {
            assertThat(scanConsumer.batches.size()).isGreaterThan(1);
            assertThat(sender.batches).isEmpty();
        } else {
            assertThat(scanConsumer.batches).isEmpty();
            assertThat(sender.batches.size()).isGreaterThan(1);
        }
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldGenerateEntityPropertyUpdates(boolean alsoWrite) throws Exception {
        // given
        StubStorageCursors data = someUniformData(10);
        TestPropertyScanConsumer scanConsumer = new TestPropertyScanConsumer();
        GenerateIndexUpdatesStep<StorageNodeCursor> step = new GenerateIndexUpdatesStep<>(
                new SimpleStageControl(),
                DEFAULT,
                data,
                any -> StoreCursors.NULL,
                PropertySelection.ALL_PROPERTIES,
                new NodeCursorBehaviour(data),
                new int[] {LABEL},
                scanConsumer,
                null,
                NO_LOCKING,
                1,
                mebiBytes(1),
                alsoWrite,
                CONTEXT_FACTORY,
                INSTANCE);
        Set<TestPropertyScanConsumer.Record> expectedUpdates = new HashSet<>();
        try (StorageNodeCursor cursor = data.allocateNodeCursor(NULL_CONTEXT, StoreCursors.NULL);
                StoragePropertyCursor propertyCursor =
                        data.allocatePropertyCursor(NULL_CONTEXT, StoreCursors.NULL, INSTANCE)) {
            cursor.scan();
            while (cursor.next()) {
                cursor.properties(propertyCursor, PropertySelection.ALL_PROPERTIES);
                Map<Integer, Value> properties = new HashMap<>();
                while (propertyCursor.next()) {
                    properties.put(propertyCursor.propertyKey(), propertyCursor.propertyValue());
                }
                expectedUpdates.add(
                        new TestPropertyScanConsumer.Record(cursor.entityReference(), cursor.labels(), properties));
            }
        }

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process(allNodeIds(data), sender, NULL_CONTEXT);

        // then
        if (alsoWrite) {
            for (TestPropertyScanConsumer.Record update : scanConsumer.batches.get(0)) {
                assertThat(expectedUpdates.remove(update)).isTrue();
            }
        } else {
            GeneratedIndexUpdates updates = sender.batches.get(0);
            updates.completeBatch();
            for (TestPropertyScanConsumer.Record update : scanConsumer.batches.get(0)) {
                assertThat(expectedUpdates.remove(update)).isTrue();
            }
        }
        assertThat(expectedUpdates).isEmpty();
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldGenerateEntityTokenUpdates(boolean alsoWrite) throws Exception {
        // given
        StubStorageCursors data = someUniformData(10);
        TestTokenScanConsumer scanConsumer = new TestTokenScanConsumer();
        GenerateIndexUpdatesStep<StorageNodeCursor> step = new GenerateIndexUpdatesStep<>(
                new SimpleStageControl(),
                DEFAULT,
                data,
                any -> StoreCursors.NULL,
                PropertySelection.ALL_PROPERTIES,
                new NodeCursorBehaviour(data),
                new int[] {LABEL},
                null,
                scanConsumer,
                NO_LOCKING,
                1,
                mebiBytes(1),
                alsoWrite,
                CONTEXT_FACTORY,
                INSTANCE);
        Set<TestTokenScanConsumer.Record> expectedUpdates = new HashSet<>();
        try (StorageNodeCursor cursor = data.allocateNodeCursor(NULL_CONTEXT, StoreCursors.NULL)) {
            cursor.scan();
            while (cursor.next()) {
                expectedUpdates.add(new TestTokenScanConsumer.Record(cursor.entityReference(), cursor.labels()));
            }
        }

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process(allNodeIds(data), sender, NULL_CONTEXT);

        // then
        if (alsoWrite) {
            for (TestTokenScanConsumer.Record tokenUpdate : scanConsumer.batches.get(0)) {
                assertThat(expectedUpdates.remove(tokenUpdate)).isTrue();
            }
        } else {
            GeneratedIndexUpdates updates = sender.batches.get(0);
            updates.completeBatch();
            for (TestTokenScanConsumer.Record tokenUpdate : scanConsumer.batches.get(0)) {
                assertThat(expectedUpdates.remove(tokenUpdate)).isTrue();
            }
        }
        assertThat(expectedUpdates).isEmpty();
    }

    @Test
    void shouldGenerateEntityPropertyUpdatesForRelevantEntityTokens() throws Exception {
        // given
        StubStorageCursors data = new StubStorageCursors();
        int numNodes = 10;
        MutableLongSet relevantNodeIds = LongSets.mutable.empty();
        for (int i = 0; i < numNodes; i++) {
            int labelId = i % 2 == 0 ? LABEL : OTHER_LABEL;
            data.withNode(i).labels(labelId).properties(KEY, stringValue("name_" + i));
            if (labelId == LABEL) {
                relevantNodeIds.add(i);
            }
        }
        TestPropertyScanConsumer scanConsumer = new TestPropertyScanConsumer();
        GenerateIndexUpdatesStep<StorageNodeCursor> step = new GenerateIndexUpdatesStep<>(
                new SimpleStageControl(),
                DEFAULT,
                data,
                any -> StoreCursors.NULL,
                PropertySelection.ALL_PROPERTIES,
                new NodeCursorBehaviour(data),
                new int[] {LABEL},
                scanConsumer,
                null,
                NO_LOCKING,
                1,
                mebiBytes(1),
                false,
                CONTEXT_FACTORY,
                INSTANCE);

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process(allNodeIds(data), sender, NULL_CONTEXT);

        // then
        GeneratedIndexUpdates updates = sender.batches.get(0);
        updates.completeBatch();
        for (TestPropertyScanConsumer.Record update : scanConsumer.batches.get(0)) {
            assertThat(relevantNodeIds.remove(update.entityId())).isTrue();
        }
        assertThat(relevantNodeIds.isEmpty()).isTrue();
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldGenerateEntityPropertyUpdatesForRelevantPropertyTokens(boolean alsoWrite) throws Exception {
        // given
        StubStorageCursors data = new StubStorageCursors();
        int numNodes = 10;
        MutableLongSet relevantNodeIds = LongSets.mutable.empty();
        for (int i = 0; i < numNodes; i++) {
            StubStorageCursors.NodeData node = data.withNode(i).labels(LABEL);
            Map<String, Value> properties = new HashMap<>();
            properties.put(KEY, stringValue("name_" + i));
            if (i % 2 == 0) {
                properties.put(OTHER_KEY, intValue(i));
                relevantNodeIds.add(i);
            }
            node.properties(properties);
        }
        int otherKeyId = data.propertyKeyTokenHolder().getIdByName(OTHER_KEY);
        TestPropertyScanConsumer scanConsumer = new TestPropertyScanConsumer();
        GenerateIndexUpdatesStep<StorageNodeCursor> step = new GenerateIndexUpdatesStep(
                new SimpleStageControl(),
                DEFAULT,
                data,
                any -> StoreCursors.NULL,
                PropertySelection.selection(otherKeyId),
                new NodeCursorBehaviour(data),
                new int[] {LABEL},
                scanConsumer,
                null,
                NO_LOCKING,
                1,
                mebiBytes(1),
                alsoWrite,
                CONTEXT_FACTORY,
                INSTANCE);

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process(allNodeIds(data), sender, NULL_CONTEXT);

        // then
        if (alsoWrite) {
            for (TestPropertyScanConsumer.Record update : scanConsumer.batches.get(0)) {
                assertThat(relevantNodeIds.remove(update.entityId())).isTrue();
            }
        } else {
            GeneratedIndexUpdates updates = sender.batches.get(0);
            updates.completeBatch();
            for (TestPropertyScanConsumer.Record update : scanConsumer.batches.get(0)) {
                assertThat(relevantNodeIds.remove(update.entityId())).isTrue();
            }
        }
        assertThat(relevantNodeIds.isEmpty()).isTrue();
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldGenerateBothEntityTokenAndPropertyUpdates(boolean alsoWrite) throws Exception {
        // given
        int numNodes = 10;
        StubStorageCursors data = someUniformData(numNodes);
        TestPropertyScanConsumer propertyScanConsumer = new TestPropertyScanConsumer();
        TestTokenScanConsumer tokenScanConsumer = new TestTokenScanConsumer();
        GenerateIndexUpdatesStep<StorageNodeCursor> step = new GenerateIndexUpdatesStep<>(
                new SimpleStageControl(),
                DEFAULT,
                data,
                any -> StoreCursors.NULL,
                PropertySelection.ALL_PROPERTIES,
                new NodeCursorBehaviour(data),
                new int[] {LABEL},
                propertyScanConsumer,
                tokenScanConsumer,
                NO_LOCKING,
                1,
                mebiBytes(1),
                alsoWrite,
                CONTEXT_FACTORY,
                INSTANCE);

        // when
        CapturingBatchSender<GeneratedIndexUpdates> sender = new CapturingBatchSender<>();
        step.process(allNodeIds(data), sender, NULL_CONTEXT);

        // then
        if (alsoWrite) {
            assertThat(propertyScanConsumer.batches.size()).isEqualTo(1);
            assertThat(propertyScanConsumer.batches.get(0).size()).isEqualTo(numNodes);
            assertThat(tokenScanConsumer.batches.size()).isEqualTo(1);
            assertThat(tokenScanConsumer.batches.get(0).size()).isEqualTo(numNodes);
        } else {
            GeneratedIndexUpdates updates = sender.batches.get(0);
            updates.completeBatch();
            assertThat(propertyScanConsumer.batches.size()).isEqualTo(1);
            assertThat(propertyScanConsumer.batches.get(0).size()).isEqualTo(numNodes);
            assertThat(tokenScanConsumer.batches.size()).isEqualTo(1);
            assertThat(tokenScanConsumer.batches.get(0).size()).isEqualTo(numNodes);
        }
    }

    private static StubStorageCursors someUniformData(int numNodes) {
        StubStorageCursors data = new StubStorageCursors();
        for (int i = 0; i < numNodes; i++) {
            data.withNode(i).labels(LABEL).properties(KEY, stringValue("name_" + i));
        }
        return data;
    }

    private static long[] allNodeIds(StubStorageCursors data) {
        try (StorageNodeCursor cursor = data.allocateNodeCursor(NULL_CONTEXT, StoreCursors.NULL)) {
            cursor.scan();
            MutableLongList ids = LongLists.mutable.empty();
            while (cursor.next()) {
                ids.add(cursor.entityReference());
            }
            return ids.toArray();
        }
    }

    private static class CapturingBatchSender<T> implements BatchSender {
        private final List<T> batches = new ArrayList<>();

        @Override
        public void send(Object batch) {
            batches.add((T) batch);
        }
    }
}

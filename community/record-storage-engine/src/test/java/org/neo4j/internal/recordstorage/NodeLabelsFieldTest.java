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
package org.neo4j.internal.recordstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.internal.helpers.collection.Iterables.addAll;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.allocateRecordsForDynamicLabels;
import static org.neo4j.kernel.impl.store.StoreType.NODE_LABEL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.util.BitBuffer.bits;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.util.BitBuffer;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
@ExtendWith(RandomExtension.class)
class NodeLabelsFieldTest {
    @Inject
    private PageCache pageCache;

    @Inject
    private RandomSupport random;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    private NeoStores neoStores;
    private NodeStore nodeStore;
    private CachedStoreCursors storeCursors;
    private DynamicAllocatorProvider allocatorProvider;

    @BeforeEach
    void startUp() {
        Config config = Config.defaults(GraphDatabaseInternalSettings.label_block_size, 60);
        var pageCacheTracer = PageCacheTracer.NULL;
        StoreFactory storeFactory = new StoreFactory(
                databaseLayout,
                config,
                new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                pageCache,
                pageCacheTracer,
                fs,
                NullLogProvider.getInstance(),
                new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
        neoStores = storeFactory.openAllNeoStores();
        allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);

        nodeStore = neoStores.getNodeStore();
        storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
    }

    @AfterEach
    void cleanUp() {
        closeAllUnchecked(storeCursors, neoStores);
    }

    @Test
    void shouldInlineOneLabel() {
        // GIVEN
        int labelId = 10;
        NodeRecord node = nodeRecordWithInlinedLabels();
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        // WHEN
        nodeLabels.add(labelId, null, null, NULL_CONTEXT, StoreCursors.NULL, INSTANCE);

        // THEN
        assertEquals(inlinedLabelsLongRepresentation(labelId), node.getLabelField());
    }

    @Test
    void shouldInlineOneLabelWithHighId() {
        // GIVEN
        int labelId = 10000;
        NodeRecord node = nodeRecordWithInlinedLabels();
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        // WHEN
        nodeLabels.add(labelId, null, null, NULL_CONTEXT, StoreCursors.NULL, INSTANCE);

        // THEN
        assertEquals(inlinedLabelsLongRepresentation(labelId), node.getLabelField());
    }

    @Test
    void shouldInlineTwoSmallLabels() {
        // GIVEN
        int labelId1 = 10;
        int labelId2 = 30;
        NodeRecord node = nodeRecordWithInlinedLabels(labelId1);
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        // WHEN
        nodeLabels.add(labelId2, null, null, NULL_CONTEXT, StoreCursors.NULL, INSTANCE);

        // THEN
        assertEquals(inlinedLabelsLongRepresentation(labelId1, labelId2), node.getLabelField());
    }

    @Test
    void shouldInlineThreeSmallLabels() {
        // GIVEN
        int labelId1 = 10;
        int labelId2 = 30;
        int labelId3 = 4095;
        NodeRecord node = nodeRecordWithInlinedLabels(labelId1, labelId2);
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        // WHEN
        nodeLabels.add(labelId3, null, null, NULL_CONTEXT, StoreCursors.NULL, INSTANCE);

        // THEN
        assertEquals(inlinedLabelsLongRepresentation(labelId1, labelId2, labelId3), node.getLabelField());
    }

    @Test
    void shouldInlineFourSmallLabels() {
        // GIVEN
        int labelId1 = 10;
        int labelId2 = 30;
        int labelId3 = 45;
        int labelId4 = 60;
        NodeRecord node = nodeRecordWithInlinedLabels(labelId1, labelId2, labelId3);
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        // WHEN
        nodeLabels.add(labelId4, null, null, NULL_CONTEXT, StoreCursors.NULL, INSTANCE);

        // THEN
        assertEquals(inlinedLabelsLongRepresentation(labelId1, labelId2, labelId3, labelId4), node.getLabelField());
    }

    @Test
    void shouldInlineFiveSmallLabels() {
        // GIVEN
        int labelId1 = 10;
        int labelId2 = 30;
        int labelId3 = 45;
        int labelId4 = 60;
        int labelId5 = 61;
        NodeRecord node = nodeRecordWithInlinedLabels(labelId1, labelId2, labelId3, labelId4);
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        // WHEN
        nodeLabels.add(labelId5, null, null, NULL_CONTEXT, StoreCursors.NULL, INSTANCE);

        // THEN
        assertEquals(
                inlinedLabelsLongRepresentation(labelId1, labelId2, labelId3, labelId4, labelId5),
                node.getLabelField());
    }

    @Test
    void shouldSpillOverToDynamicRecordIfExceedsInlinedSpace() {
        // GIVEN -- the upper limit for a label ID for 3 labels would be 36b/3 - 1 = 12b - 1 = 4095
        int labelId1 = 10;
        int labelId2 = 30;
        int labelId3 = 4096;
        NodeRecord node = nodeRecordWithInlinedLabels(labelId1, labelId2);
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        // WHEN
        Collection<DynamicRecord> changedDynamicRecords = nodeLabels.add(
                labelId3,
                nodeStore,
                allocatorProvider.allocator(NODE_LABEL),
                NULL_CONTEXT,
                StoreCursors.NULL,
                INSTANCE);

        // THEN
        assertEquals(1, Iterables.count(changedDynamicRecords));
        assertEquals(dynamicLabelsLongRepresentation(changedDynamicRecords), node.getLabelField());
        Assertions.assertArrayEquals(
                new int[] {labelId1, labelId2, labelId3},
                DynamicNodeLabels.getDynamicLabelsArray(
                        changedDynamicRecords,
                        nodeStore.getDynamicLabelStore(),
                        StoreCursors.NULL,
                        EmptyMemoryTracker.INSTANCE));
    }

    @Test
    void oneDynamicRecordShouldExtendIntoAnAdditionalIfTooManyLabels() {
        // GIVEN
        // will occupy 60B of data, i.e. one dynamic record
        NodeRecord node = nodeRecordWithDynamicLabels(nodeStore, storeCursors, oneByteInt(56));
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        // WHEN
        Set<DynamicRecord> changedDynamicRecords = Iterables.asSet(nodeLabels.add(
                1, nodeStore, allocatorProvider.allocator(NODE_LABEL), NULL_CONTEXT, StoreCursors.NULL, INSTANCE));

        // THEN
        assertTrue(changedDynamicRecords.containsAll(initialRecords));
        assertEquals(initialRecords.size() + 1, changedDynamicRecords.size());
    }

    @Test
    void oneDynamicRecordShouldStoreItsOwner() {
        // GIVEN
        // will occupy 60B of data, i.e. one dynamic record
        long nodeId = 24L;
        NodeRecord node = nodeRecordWithDynamicLabels(nodeId, nodeStore, storeCursors, oneByteInt(56));
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();

        // WHEN
        long owner = DynamicNodeLabels.getDynamicLabelsArrayOwner(
                initialRecords, nodeStore.getDynamicLabelStore(), StoreCursors.NULL, EmptyMemoryTracker.INSTANCE);

        // THEN
        assertEquals(nodeId, owner);
    }

    @Test
    void twoDynamicRecordsShouldShrinkToOneWhenRemoving() {
        // GIVEN
        // will occupy 61B of data, i.e. just two dynamic records
        NodeRecord node = nodeRecordWithDynamicLabels(nodeStore, storeCursors, oneByteInt(57));
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        // WHEN
        List<DynamicRecord> changedDynamicRecords = addAll(
                new ArrayList<>(),
                nodeLabels.remove(
                        255 /*Initial labels go from 255 and down to 255-58*/,
                        nodeStore,
                        allocatorProvider.allocator(NODE_LABEL),
                        NULL_CONTEXT,
                        StoreCursors.NULL,
                        INSTANCE));

        // THEN
        assertEquals(initialRecords, changedDynamicRecords);
        assertTrue(changedDynamicRecords.get(0).inUse());
        Assertions.assertFalse(changedDynamicRecords.get(1).inUse());
    }

    @Test
    void twoDynamicRecordsShouldShrinkToOneWhenRemovingWithoutChangingItsOwner() {
        // GIVEN
        // will occupy 61B of data, i.e. just two dynamic records
        long nodeId = 42L;
        NodeRecord node = nodeRecordWithDynamicLabels(nodeId, nodeStore, storeCursors, oneByteInt(57));
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        List<DynamicRecord> changedDynamicRecords = addAll(
                new ArrayList<>(),
                nodeLabels.remove(
                        255 /*Initial labels go from 255 and down to 255-58*/,
                        nodeStore,
                        allocatorProvider.allocator(NODE_LABEL),
                        NULL_CONTEXT,
                        StoreCursors.NULL,
                        INSTANCE));

        // WHEN
        long owner = DynamicNodeLabels.getDynamicLabelsArrayOwner(
                changedDynamicRecords,
                nodeStore.getDynamicLabelStore(),
                StoreCursors.NULL,
                EmptyMemoryTracker.INSTANCE);

        // THEN
        assertEquals(nodeId, owner);
    }

    @Test
    void oneDynamicRecordShouldShrinkIntoInlinedWhenRemoving() {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels(nodeStore, storeCursors, oneByteInt(5));
        Collection<DynamicRecord> initialRecords = node.getDynamicLabelRecords();
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        // WHEN
        Collection<DynamicRecord> changedDynamicRecords = Iterables.asCollection(nodeLabels.remove(
                255, nodeStore, allocatorProvider.allocator(NODE_LABEL), NULL_CONTEXT, StoreCursors.NULL, INSTANCE));

        // THEN
        assertEquals(initialRecords, changedDynamicRecords);
        Assertions.assertFalse(Iterables.single(changedDynamicRecords).inUse());
        assertEquals(inlinedLabelsLongRepresentation(251, 252, 253, 254), node.getLabelField());
    }

    @Test
    void shouldReadIdOfDynamicRecordFromDynamicLabelsField() {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels(nodeStore, storeCursors, oneByteInt(5));
        DynamicRecord dynamicRecord = node.getDynamicLabelRecords().iterator().next();

        // WHEN
        long dynRecordId = NodeLabelsField.firstDynamicLabelRecordId(node.getLabelField());

        // THEN
        assertEquals(dynamicRecord.getId(), dynRecordId);
    }

    @Test
    void shouldReadNullDynamicRecordFromInlineLabelsField() {
        // GIVEN
        NodeRecord node = nodeRecordWithInlinedLabels(23L);

        // WHEN
        boolean isDynamicReference = NodeLabelsField.fieldPointsToDynamicRecordOfLabels(node.getLabelField());

        // THEN
        Assertions.assertFalse(isDynamicReference);
    }

    @Test
    void maximumOfSevenInlinedLabels() {
        // GIVEN
        long[] labels = new long[] {0, 1, 2, 3, 4, 5, 6};
        NodeRecord node = nodeRecordWithInlinedLabels(labels);
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        // WHEN
        Iterable<DynamicRecord> changedDynamicRecords = nodeLabels.add(
                23, nodeStore, allocatorProvider.allocator(NODE_LABEL), NULL_CONTEXT, StoreCursors.NULL, INSTANCE);

        // THEN
        assertEquals(dynamicLabelsLongRepresentation(changedDynamicRecords), node.getLabelField());
        assertEquals(1, Iterables.count(changedDynamicRecords));
    }

    @Test
    void addingAnAlreadyAddedLabelWhenLabelsAreInlinedShouldFail() {
        // GIVEN
        int labelId = 1;
        NodeRecord node = nodeRecordWithInlinedLabels(labelId);
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        assertThatThrownBy(() -> nodeLabels.add(
                        labelId,
                        nodeStore,
                        allocatorProvider.allocator(NODE_LABEL),
                        NULL_CONTEXT,
                        StoreCursors.NULL,
                        INSTANCE))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void addingAnAlreadyAddedLabelWhenLabelsAreInDynamicRecordsShouldFail() {
        // GIVEN
        int[] labels = oneByteInt(20);
        NodeRecord node = nodeRecordWithDynamicLabels(nodeStore, storeCursors, labels);
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        assertThatThrownBy(() -> nodeLabels.add(
                        safeCastLongToInt(labels[0]),
                        nodeStore,
                        allocatorProvider.allocator(NODE_LABEL),
                        NULL_CONTEXT,
                        StoreCursors.NULL,
                        INSTANCE))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void removingNonExistentInlinedLabelShouldFail() {
        // GIVEN
        int labelId1 = 1;
        int labelId2 = 2;
        NodeRecord node = nodeRecordWithInlinedLabels(labelId1);
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        assertThrows(
                IllegalStateException.class,
                () -> nodeLabels.remove(
                        labelId2,
                        nodeStore,
                        allocatorProvider.allocator(NODE_LABEL),
                        NULL_CONTEXT,
                        StoreCursors.NULL,
                        INSTANCE));
    }

    @Test
    void removingNonExistentLabelInDynamicRecordsShouldFail() {
        // GIVEN
        int[] labels = oneByteInt(20);
        NodeRecord node = nodeRecordWithDynamicLabels(nodeStore, storeCursors, labels);
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        assertThrows(
                IllegalStateException.class,
                () -> nodeLabels.remove(
                        123456,
                        nodeStore,
                        allocatorProvider.allocator(NODE_LABEL),
                        NULL_CONTEXT,
                        StoreCursors.NULL,
                        INSTANCE));
    }

    @Test
    void shouldReallocateSomeOfPreviousDynamicRecords() {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels(nodeStore, storeCursors, oneByteInt(5));
        Set<DynamicRecord> initialRecords = Iterables.asUniqueSet(node.getDynamicLabelRecords());
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        // WHEN
        Set<DynamicRecord> reallocatedRecords = Iterables.asUniqueSet(nodeLabels.put(
                fourByteInts(100),
                nodeStore,
                allocatorProvider.allocator(NODE_LABEL),
                NULL_CONTEXT,
                StoreCursors.NULL,
                INSTANCE));

        // THEN
        assertTrue(reallocatedRecords.containsAll(initialRecords));
        assertTrue(reallocatedRecords.size() > initialRecords.size());
    }

    @Test
    void shouldReallocateAllOfPreviousDynamicRecordsAndThenSome() {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels(nodeStore, storeCursors, fourByteInts(100));
        Set<DynamicRecord> initialRecords = Iterables.asSet(cloned(node.getDynamicLabelRecords(), DynamicRecord.class));
        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField(node);

        // WHEN
        Set<DynamicRecord> reallocatedRecords = Iterables.asUniqueSet(nodeLabels.put(
                fourByteInts(5),
                nodeStore,
                allocatorProvider.allocator(NODE_LABEL),
                NULL_CONTEXT,
                StoreCursors.NULL,
                INSTANCE));

        // THEN
        assertTrue(
                idsOf(initialRecords).containsAll(idsOf(used(reallocatedRecords))),
                "initial:" + initialRecords + ", reallocated:" + reallocatedRecords);
        assertTrue(used(reallocatedRecords).size() < initialRecords.size());
    }

    @Test
    void shouldNotFailWhenDynamicRecordsBecomeUnused() {
        // GIVEN
        NodeRecord node = nodeRecordWithDynamicLabels(nodeStore, storeCursors, fourByteInts(100));
        assertThat(NodeLabelsField.get(node, nodeStore, storeCursors, EmptyMemoryTracker.INSTANCE))
                .isNotEmpty();

        // WHEN
        for (DynamicRecord record : node.getDynamicLabelRecords()) {
            record.setInUse(false);
        }

        // THEN
        assertThat(NodeLabelsField.get(node, nodeStore, storeCursors, EmptyMemoryTracker.INSTANCE))
                .isEmpty();
    }

    /*
     * There was this issue that DynamicNodeLabels#add would consider even unused dynamic records when
     * reading existing label ids before making the change. Previously this would create a duplicate
     * last label id (the one formerly being in the second record).
     *
     * This randomized test found this issue every time when it existed and it will potentially find other
     * unforeseen issues as well.
     */
    @Test
    void shouldHandleRandomAddsAndRemoves() {
        // GIVEN
        Set<Integer> key = new HashSet<>();
        NodeRecord node = new NodeRecord(0);
        node.setInUse(true);

        // WHEN
        DynamicRecordAllocator allocator = allocatorProvider.allocator(NODE_LABEL);
        for (int i = 0; i < 100_000; i++) {
            NodeLabels labels = NodeLabelsField.parseLabelsField(node);
            int labelId = random.nextInt(200);
            if (random.nextBoolean()) {
                if (!key.contains(labelId)) {
                    labels.add(labelId, nodeStore, allocator, NULL_CONTEXT, StoreCursors.NULL, INSTANCE);
                    key.add(labelId);
                }
            } else {
                if (key.remove(labelId)) {
                    labels.remove(labelId, nodeStore, allocator, NULL_CONTEXT, StoreCursors.NULL, INSTANCE);
                }
            }
        }

        // THEN
        NodeLabels labels = NodeLabelsField.parseLabelsField(node);
        int[] readLabelIds = labels.get(nodeStore, StoreCursors.NULL, EmptyMemoryTracker.INSTANCE);
        for (int labelId : readLabelIds) {
            assertTrue(key.remove(labelId), "Found an unexpected label " + labelId);
        }
        assertTrue(key.isEmpty());
    }

    private static long dynamicLabelsLongRepresentation(Iterable<DynamicRecord> records) {
        return 0x8000000000L | Iterables.first(records).getId();
    }

    private static long inlinedLabelsLongRepresentation(long... labelIds) {
        long header = (long) labelIds.length << 36;
        byte bitsPerLabel = (byte) (36 / labelIds.length);
        BitBuffer bits = bits(5);
        for (long labelId : labelIds) {
            bits.put(labelId, bitsPerLabel);
        }
        return header | bits.getLongs()[0];
    }

    private static NodeRecord nodeRecordWithInlinedLabels(long... labels) {
        NodeRecord node = new NodeRecord(0).initialize(false, 0, false, 0, 0);
        if (labels.length > 0) {
            node.setLabelField(inlinedLabelsLongRepresentation(labels), Collections.emptyList());
        }
        return node;
    }

    private NodeRecord nodeRecordWithDynamicLabels(NodeStore nodeStore, StoreCursors storeCursors, int... labels) {
        return nodeRecordWithDynamicLabels(0, nodeStore, storeCursors, labels);
    }

    private NodeRecord nodeRecordWithDynamicLabels(
            long nodeId, NodeStore nodeStore, StoreCursors storeCursors, int... labels) {
        NodeRecord node = new NodeRecord(nodeId).initialize(false, 0, false, 0, 0);
        List<DynamicRecord> initialRecords = allocateAndApply(nodeStore, storeCursors, node.getId(), labels);
        node.setLabelField(dynamicLabelsLongRepresentation(initialRecords), initialRecords);
        return node;
    }

    private List<DynamicRecord> allocateAndApply(
            NodeStore nodeStore, StoreCursors storeCursors, long nodeId, int[] labelIds) {
        List<DynamicRecord> records = allocateRecordsForDynamicLabels(
                nodeId, labelIds, allocatorProvider.allocator(NODE_LABEL), NULL_CONTEXT, INSTANCE);
        nodeStore.updateDynamicLabelRecords(records, IdUpdateListener.DIRECT, NULL_CONTEXT, storeCursors);
        return records;
    }

    private static int[] oneByteInt(int numberOfInts) {
        int[] result = new int[numberOfInts];
        for (int i = 0; i < numberOfInts; i++) {
            result[i] = 255 - i;
        }
        Arrays.sort(result);
        return result;
    }

    private static int[] fourByteInts(int numberOfInts) {
        int[] result = new int[numberOfInts];
        for (int i = 0; i < numberOfInts; i++) {
            result[i] = Integer.MAX_VALUE - i;
        }
        Arrays.sort(result);
        return result;
    }

    private static Set<DynamicRecord> used(Set<DynamicRecord> reallocatedRecords) {
        Set<DynamicRecord> used = new HashSet<>();
        for (DynamicRecord record : reallocatedRecords) {
            if (record.inUse()) {
                used.add(record);
            }
        }
        return used;
    }

    private static <T extends AbstractBaseRecord> Iterable<T> cloned(Iterable<T> items, final Class<T> itemClass) {
        return Iterables.map(obj -> itemClass.cast(ReflectionUtil.callCopyConstructor(obj)), items);
    }

    private static LongSet idsOf(Set<DynamicRecord> records) {
        MutableLongSet ids = LongSets.mutable.empty();
        for (DynamicRecord record : records) {
            ids.add(record.getId());
        }
        return ids;
    }
}

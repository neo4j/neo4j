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
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.MathUtil.ceil;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.StoreType.NODE_LABEL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.stream.LongStream;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@ExtendWith(RandomExtension.class)
@EphemeralPageCacheExtension
class RecordNodeCursorIT {
    private static final int HIGH_LABEL_ID = 0x10000;

    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomSupport random;

    private NeoStores neoStores;
    private NodeStore nodeStore;
    private CachedStoreCursors storeCursors;
    private DynamicAllocatorProvider allocatorProvider;

    @BeforeEach
    void startNeoStores() {
        var pageCacheTracer = PageCacheTracer.NULL;
        neoStores = new StoreFactory(
                        RecordDatabaseLayout.ofFlat(directory.homePath()),
                        Config.defaults(),
                        new DefaultIdGeneratorFactory(directory.getFileSystem(), immediate(), pageCacheTracer, "db"),
                        pageCache,
                        pageCacheTracer,
                        directory.getFileSystem(),
                        NullLogProvider.getInstance(),
                        new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                        false,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL)
                .openAllNeoStores();
        allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);

        nodeStore = neoStores.getNodeStore();
        storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
    }

    @AfterEach
    void stopNeoStores() {
        storeCursors.close();
        neoStores.close();
    }

    @RepeatedTest(10)
    void shouldProperlyReturnHasLabel() {
        // given/when
        var labels = IntSets.mutable.empty();
        long nodeId = createNodeWithRandomLabels(labels);

        // then
        try (RecordNodeCursor nodeCursor = new RecordNodeCursor(
                nodeStore,
                neoStores.getRelationshipStore(),
                neoStores.getRelationshipGroupStore(),
                null,
                NULL_CONTEXT,
                storeCursors,
                EmptyMemoryTracker.INSTANCE)) {
            nodeCursor.single(nodeId);
            assertThat(nodeCursor.next()).isTrue();
            for (int labelId = 0; labelId < HIGH_LABEL_ID; labelId++) {
                boolean fromCursor = nodeCursor.hasLabel(labelId);
                boolean fromSet = labels.contains(labelId);
                assertThat(fromCursor).as("Label " + labelId).isEqualTo(fromSet);
            }
        }
    }

    @RepeatedTest(10)
    void shouldProperlyReturnHasAnyLabel() {
        // given/when
        var labels = IntSets.mutable.empty();
        long nodeId = createNodeWithRandomLabels(labels, 5);

        // then
        try (RecordNodeCursor nodeCursor = new RecordNodeCursor(
                nodeStore,
                neoStores.getRelationshipStore(),
                neoStores.getRelationshipGroupStore(),
                null,
                NULL_CONTEXT,
                storeCursors,
                EmptyMemoryTracker.INSTANCE)) {
            nodeCursor.single(nodeId);
            assertThat(nodeCursor.next()).isTrue();
            boolean fromCursor = nodeCursor.hasLabel();
            boolean fromSet = !labels.isEmpty();
            assertThat(fromCursor).isEqualTo(fromSet);
        }
    }

    @Test
    void shouldExhaustNodesWithBatches() {
        final var ids = createNodes(random.nextInt(23, 42));

        try (var nodes = new RecordNodeCursor(
                nodeStore,
                neoStores.getRelationshipStore(),
                neoStores.getRelationshipGroupStore(),
                null,
                NULL_CONTEXT,
                storeCursors,
                EmptyMemoryTracker.INSTANCE)) {

            final var scan = new RecordNodeScan();
            final var found = LongSets.mutable.withInitialCapacity(ids.size());

            // scan a quarter of the nodes
            assertThat(nodes.scanBatch(scan, ceil(ids.size(), 4))).isTrue();
            while (nodes.next()) {
                assertThat(found.add(nodes.entityReference())).isTrue();
            }
            assertThat(ids.containsAll(found)).isTrue();

            // scan the rest of the nodes
            assertThat(nodes.scanBatch(scan, Long.MAX_VALUE)).isTrue();
            while (nodes.next()) {
                assertThat(found.add(nodes.entityReference())).isTrue();
            }
            assertThat(found).isEqualTo(ids);

            // attempt to scan anything more a few times
            for (int i = 0, n = random.nextInt(2, 10); i < n; i++) {
                assertThat(nodes.scanBatch(scan, Long.MAX_VALUE)).isFalse();
            }
        }
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldCorrectlySupportFastDegreeLookup(boolean dense) {
        // given
        var nodeRecord = createNodeRecord();
        nodeRecord.setDense(dense);
        var nodeId = write(nodeRecord);

        // when
        try (var nodeCursor = new RecordNodeCursor(
                nodeStore,
                neoStores.getRelationshipStore(),
                neoStores.getRelationshipGroupStore(),
                null,
                NULL_CONTEXT,
                storeCursors,
                EmptyMemoryTracker.INSTANCE)) {
            nodeCursor.single(nodeId);
            assertThat(nodeCursor.next()).isTrue();
            var supportsFastDegreesLookup = nodeCursor.supportsFastDegreeLookup();

            // then
            assertThat(supportsFastDegreesLookup).isEqualTo(dense);
        }
    }

    private long createNodeWithRandomLabels(MutableIntSet labelsSet) {
        return createNodeWithRandomLabels(labelsSet, 100);
    }

    private long createNodeWithRandomLabels(MutableIntSet labelsSet, int numberOfLabelsBound) {
        final var nodeRecord = createNodeRecord();
        final var labels = randomLabels(labelsSet, numberOfLabelsBound);
        NodeLabelsField.parseLabelsField(nodeRecord)
                .put(labels, nodeStore, allocatorProvider.allocator(NODE_LABEL), NULL_CONTEXT, storeCursors, INSTANCE);
        return write(nodeRecord);
    }

    private int[] randomLabels(MutableIntSet labelsSet, int numberOfLabelsBound) {
        int count = random.nextInt(0, numberOfLabelsBound);
        int highId = random.nextBoolean() ? HIGH_LABEL_ID : count * 3;
        for (int i = 0; i < count; i++) {
            if (!labelsSet.add(random.nextInt(highId))) {
                i--;
            }
        }
        return labelsSet.toSortedArray();
    }

    private long createNode() {
        return write(createNodeRecord());
    }

    private LongSet createNodes(int numberOfNodes) {
        return LongSets.immutable.ofAll(LongStream.generate(this::createNode).limit(numberOfNodes));
    }

    private NodeRecord createNodeRecord() {
        final var nodeRecord = nodeStore.newRecord();
        nodeRecord.setId(nodeStore.getIdGenerator().nextId(NULL_CONTEXT));
        nodeRecord.initialize(
                true,
                Record.NO_NEXT_PROPERTY.longValue(),
                false,
                Record.NO_NEXT_RELATIONSHIP.longValue(),
                Record.NO_LABELS_FIELD.longValue());
        nodeRecord.setCreated();
        return nodeRecord;
    }

    private long write(NodeRecord nodeRecord) {
        try (var writeCursor = storeCursors.writeCursor(NODE_CURSOR)) {
            nodeStore.updateRecord(nodeRecord, writeCursor, NULL_CONTEXT, storeCursors);
        }
        return nodeRecord.getId();
    }
}

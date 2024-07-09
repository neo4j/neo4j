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
package org.neo4j.internal.batchimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.collection.PrimitiveLongCollections.iterator;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_ARRAY_STORE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_LABEL_STORE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_STRING_STORE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.StoreType.NODE_LABEL;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_ARRAY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_STRING;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
@ExtendWith(RandomExtension.class)
class DeleteDuplicateNodesStepTest {
    @Inject
    private RandomSupport random;

    @Inject
    private PageCache pageCache;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private EphemeralFileSystemAbstraction fs;

    private NeoStores neoStores;
    private CachedStoreCursors storeCursors;
    private CursorContextFactory contextFactory;

    @BeforeEach
    void before() {
        contextFactory = new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);
        var storeFactory = new StoreFactory(
                databaseLayout,
                Config.defaults(),
                new DefaultIdGeneratorFactory(fs, immediate(), PageCacheTracer.NULL, databaseLayout.getDatabaseName()),
                pageCache,
                PageCacheTracer.NULL,
                fs,
                NullLogProvider.getInstance(),
                contextFactory,
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
        neoStores = storeFactory.openAllNeoStores();
        storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
    }

    @AfterEach
    void after() {
        storeCursors.close();
        neoStores.close();
    }

    @RepeatedTest(10)
    void shouldDeleteEverythingAboutTheDuplicatedNodes() throws Exception {
        // given
        Ids[] ids = new Ids[9];
        DataImporter.Monitor monitor = new DataImporter.Monitor();
        ids[0] = createNode(monitor, neoStores, 10, 10); // node with many properties and many labels
        ids[1] = createNode(monitor, neoStores, 10, 1); // node with many properties and few labels
        ids[2] = createNode(monitor, neoStores, 10, 0); // node with many properties and no labels
        ids[3] = createNode(monitor, neoStores, 1, 10); // node with few properties and many labels
        ids[4] = createNode(monitor, neoStores, 1, 1); // node with few properties and few labels
        ids[5] = createNode(monitor, neoStores, 1, 0); // node with few properties and no labels
        ids[6] = createNode(monitor, neoStores, 0, 10); // node with no properties and many labels
        ids[7] = createNode(monitor, neoStores, 0, 1); // node with no properties and few labels
        ids[8] = createNode(monitor, neoStores, 0, 0); // node with no properties and no labels

        // when
        long[] duplicateNodeIds = randomNodes(ids);
        NodeStore nodeStore = neoStores.getNodeStore();
        try (var stage = new DeleteDuplicateNodesStage(
                Configuration.DEFAULT, iterator(duplicateNodeIds), neoStores, monitor, contextFactory)) {
            stage.execute().awaitCompletion(10, TimeUnit.MINUTES);
        }

        // then
        int expectedNodes = 0;
        int expectedProperties = 0;
        var nodeCursor = storeCursors.readCursor(NODE_CURSOR);
        for (Ids entity : ids) {
            boolean expectedToBeInUse = !ArrayUtils.contains(duplicateNodeIds, entity.node.getId());
            int stride = expectedToBeInUse ? 1 : 0;
            expectedNodes += stride;

            // Verify node record
            assertEquals(expectedToBeInUse, nodeStore.isInUse(entity.node.getId(), nodeCursor));

            // Verify label records
            for (DynamicRecord labelRecord : entity.node.getDynamicLabelRecords()) {
                assertEquals(
                        expectedToBeInUse,
                        nodeStore
                                .getDynamicLabelStore()
                                .isInUse(labelRecord.getId(), storeCursors.readCursor(DYNAMIC_LABEL_STORE_CURSOR)));
            }

            // Verify property records
            for (PropertyRecord propertyRecord : entity.properties) {
                assertEquals(
                        expectedToBeInUse,
                        neoStores
                                .getPropertyStore()
                                .isInUse(propertyRecord.getId(), storeCursors.readCursor(PROPERTY_CURSOR)));
                for (PropertyBlock property : propertyRecord) {
                    // Verify property dynamic value records
                    for (DynamicRecord valueRecord : property.getValueRecords()) {
                        AbstractDynamicStore valueStore;
                        PageCursor valueCursor;
                        switch (property.getType()) {
                            case STRING -> {
                                valueStore = neoStores.getPropertyStore().getStringStore();
                                valueCursor = storeCursors.readCursor(DYNAMIC_STRING_STORE_CURSOR);
                            }
                            case ARRAY -> {
                                valueStore = neoStores.getPropertyStore().getArrayStore();
                                valueCursor = storeCursors.readCursor(DYNAMIC_ARRAY_STORE_CURSOR);
                            }
                            default -> throw new IllegalArgumentException(propertyRecord + " " + property);
                        }
                        assertEquals(expectedToBeInUse, valueStore.isInUse(valueRecord.getId(), valueCursor));
                    }
                    expectedProperties += stride;
                }
            }
        }

        assertEquals(expectedNodes, monitor.nodesImported());
        assertEquals(expectedProperties, monitor.propertiesImported());
    }

    @Test
    void tracePageCacheAccessOnNodeDeduplication() throws Exception {
        // given
        Ids[] ids = new Ids[10];
        DataImporter.Monitor monitor = new DataImporter.Monitor();
        for (int i = 0; i < ids.length; i++) {
            ids[i] = createNode(monitor, neoStores, 1, 1);
        }

        long[] duplicateNodeIds = randomNodes(ids);
        var cacheTracer = new DefaultPageCacheTracer();
        try (var stage = new DeleteDuplicateNodesStage(
                Configuration.DEFAULT,
                iterator(duplicateNodeIds),
                neoStores,
                monitor,
                new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER))) {
            stage.execute().awaitCompletion(10, TimeUnit.MINUTES);
        }

        int expectedEventNumber = duplicateNodeIds.length
                * 2; // at least 2 events per node is expected since property size is dynamic random thingy
        assertThat(cacheTracer.pins()).isGreaterThanOrEqualTo(expectedEventNumber);
        assertThat(cacheTracer.unpins()).isGreaterThanOrEqualTo(expectedEventNumber);
        assertThat(cacheTracer.hits()).isGreaterThanOrEqualTo(expectedEventNumber);
    }

    private long[] randomNodes(Ids[] ids) {
        long[] nodeIds = new long[ids.length];
        int cursor = 0;
        for (Ids id : ids) {
            if (random.nextBoolean()) {
                nodeIds[cursor++] = id.node.getId();
            }
        }

        // If none was selected, then pick just one
        if (cursor == 0) {
            nodeIds[cursor++] = random.among(ids).node.getId();
        }
        return Arrays.copyOf(nodeIds, cursor);
    }

    private Ids createNode(DataImporter.Monitor monitor, NeoStores neoStores, int propertyCount, int labelCount) {
        PropertyStore propertyStore = neoStores.getPropertyStore();
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord nodeRecord = nodeStore.newRecord();
        nodeRecord.setId(nodeStore.getIdGenerator().nextId(NULL_CONTEXT));
        nodeRecord.setInUse(true);
        DynamicAllocatorProvider allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);
        NodeLabelsField.parseLabelsField(nodeRecord)
                .put(
                        labelIds(labelCount),
                        nodeStore,
                        allocatorProvider.allocator(NODE_LABEL),
                        NULL_CONTEXT,
                        storeCursors,
                        INSTANCE);
        PropertyRecord[] propertyRecords =
                createPropertyChain(nodeRecord, propertyCount, propertyStore, allocatorProvider);
        if (propertyRecords.length > 0) {
            nodeRecord.setNextProp(propertyRecords[0].getId());
        }
        try (var cursor = storeCursors.writeCursor(NODE_CURSOR)) {
            nodeStore.updateRecord(nodeRecord, cursor, NULL_CONTEXT, storeCursors);
        }
        monitor.nodesImported(1);
        monitor.propertiesImported(propertyCount);
        return new Ids(nodeRecord, propertyRecords);
    }

    // A slight duplication of PropertyCreator logic, please try and remove in favor of that utility later on
    private PropertyRecord[] createPropertyChain(
            NodeRecord nodeRecord,
            int numberOfProperties,
            PropertyStore propertyStore,
            DynamicAllocatorProvider allocatorProvider) {
        List<PropertyRecord> records = new ArrayList<>();
        PropertyRecord current = null;
        int space = PropertyType.getPayloadSizeLongs();
        for (int i = 0; i < numberOfProperties; i++) {
            PropertyBlock block = new PropertyBlock();
            PropertyStore.encodeValue(
                    block,
                    i,
                    random.nextValue(),
                    allocatorProvider.allocator(PROPERTY_STRING),
                    allocatorProvider.allocator(PROPERTY_ARRAY),
                    NULL_CONTEXT,
                    INSTANCE);
            if (current == null || block.getValueBlocks().length > space) {
                PropertyRecord next = propertyStore.newRecord();
                nodeRecord.setIdTo(next);
                next.setId(propertyStore.getIdGenerator().nextId(NULL_CONTEXT));
                if (current != null) {
                    next.setPrevProp(current.getId());
                    current.setNextProp(next.getId());
                }
                next.setInUse(true);
                current = next;
                space = PropertyType.getPayloadSizeLongs();
                records.add(current);
            }
            current.addPropertyBlock(block);
            space -= block.getValueBlocks().length;
        }
        try (var cursor = storeCursors.writeCursor(PROPERTY_CURSOR)) {
            records.forEach(record -> propertyStore.updateRecord(record, cursor, NULL_CONTEXT, storeCursors));
        }
        return records.toArray(new PropertyRecord[0]);
    }

    private static int[] labelIds(int labelCount) {
        int[] result = new int[labelCount];
        for (int i = 0; i < labelCount; i++) {
            result[i] = i;
        }
        return result;
    }

    private record Ids(NodeRecord node, PropertyRecord[] properties) {}
}

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
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_STRING_STORE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_ARRAY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_STRING;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.staging.SimpleStageControl;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
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
class DeleteViolatingRelationshipsStepTest {
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
    private final int NBR_TYPES = 3;

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
    void shouldDeleteEverythingAboutTheViolatingRelationships() throws Exception {
        // given
        Ids[] ids = new Ids[9];
        DataImporter.Monitor monitor = new DataImporter.Monitor();
        DataStatistics relationshipTypeCounts =
                new DataStatistics(monitor, new DataStatistics.RelationshipTypeCount[0]);
        long[] violatingRelIds;
        SimpleStageControl control;
        RelationshipStore relationshipStore;
        try (DataStatistics.Client client = relationshipTypeCounts.newClient()) {
            ids[0] = createRelationship(monitor, client, neoStores, 10);
            ids[1] = createRelationship(monitor, client, neoStores, 10);
            ids[2] = createRelationship(monitor, client, neoStores, 10);
            ids[3] = createRelationship(monitor, client, neoStores, 1);
            ids[4] = createRelationship(monitor, client, neoStores, 1);
            ids[5] = createRelationship(monitor, client, neoStores, 1);
            ids[6] = createRelationship(monitor, client, neoStores, 0);
            ids[7] = createRelationship(monitor, client, neoStores, 0);
            ids[8] = createRelationship(monitor, client, neoStores, 0);
        }
        // when
        violatingRelIds = randomRels(ids);
        control = new SimpleStageControl();
        relationshipStore = neoStores.getRelationshipStore();
        try (DataStatistics.Client client = relationshipTypeCounts.newClient()) {
            try (DeleteViolatingRelationshipsStep step = new DeleteViolatingRelationshipsStep(
                    control,
                    Configuration.DEFAULT,
                    iterator(violatingRelIds),
                    neoStores,
                    monitor,
                    client,
                    contextFactory)) {
                control.steps(step);
                startAndAwaitCompletionOf(step);
            }
        }
        control.assertHealthy();

        // then
        int expectedRelationships = 0;
        int expectedProperties = 0;
        int[] expectedTypeDistribution = new int[NBR_TYPES];
        var relationshipCursor = storeCursors.readCursor(RELATIONSHIP_CURSOR);
        for (Ids entity : ids) {
            boolean expectedToBeInUse = !ArrayUtils.contains(violatingRelIds, entity.relationship.getId());
            int stride = expectedToBeInUse ? 1 : 0;
            expectedRelationships += stride;

            // Verify relationship record
            assertEquals(expectedToBeInUse, relationshipStore.isInUse(entity.relationship.getId(), relationshipCursor));
            expectedTypeDistribution[entity.relationship.getType()] += stride;

            // Verify property records
            for (PropertyRecord propertyRecord : entity.properties) {
                assertEquals(
                        expectedToBeInUse,
                        neoStores
                                .getPropertyStore()
                                .isInUse(propertyRecord.getId(), storeCursors.readCursor(PROPERTY_CURSOR)));
                for (PropertyBlock property : propertyRecord.propertyBlocks()) {
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

        assertEquals(expectedRelationships, monitor.relationshipsImported());
        assertEquals(expectedProperties, monitor.propertiesImported());

        assertThat(relationshipTypeCounts.getNumberOfRelationshipTypes()).isLessThanOrEqualTo(NBR_TYPES);
        for (int i = 0; i < NBR_TYPES; i++) {
            boolean typeExists = i < relationshipTypeCounts.getNumberOfRelationshipTypes();
            assertThat(typeExists ? relationshipTypeCounts.get(i).getCount() : 0)
                    .isEqualTo(expectedTypeDistribution[
                            typeExists ? relationshipTypeCounts.get(i).getTypeId() : i]);
        }
    }

    private long[] randomRels(Ids[] ids) {
        long[] relIds = new long[ids.length];
        int cursor = 0;
        for (Ids id : ids) {
            if (random.nextBoolean()) {
                relIds[cursor++] = id.relationship.getId();
            }
        }

        // If none was selected, then pick just one
        if (cursor == 0) {
            relIds[cursor++] = random.among(ids).relationship.getId();
        }
        return Arrays.copyOf(relIds, cursor);
    }

    private record Ids(RelationshipRecord relationship, PropertyRecord[] properties) {}

    private Ids createRelationship(
            DataImporter.Monitor monitor, DataStatistics.Client client, NeoStores neoStores, int propertyCount) {
        PropertyStore propertyStore = neoStores.getPropertyStore();
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        RelationshipRecord relationshipRecord = relationshipStore.newRecord();
        relationshipRecord.setId(relationshipStore.getIdGenerator().nextId(NULL_CONTEXT));
        relationshipRecord.setInUse(true);
        int type = random.nextInt(NBR_TYPES);
        relationshipRecord.setType(type);
        client.increment(type);

        DynamicAllocatorProvider allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);
        PropertyRecord[] propertyRecords =
                createPropertyChain(relationshipRecord, propertyCount, propertyStore, allocatorProvider);
        if (propertyRecords.length > 0) {
            relationshipRecord.setNextProp(propertyRecords[0].getId());
        }
        try (var cursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            relationshipStore.updateRecord(relationshipRecord, cursor, NULL_CONTEXT, storeCursors);
        }
        monitor.relationshipsImported(1);
        monitor.propertiesImported(propertyCount);
        return new Ids(relationshipRecord, propertyRecords);
    }

    private PropertyRecord[] createPropertyChain(
            RelationshipRecord relationshipRecord,
            int numberOfProperties,
            PropertyStore propertyStore,
            DynamicAllocatorProvider allocatorProvider) {
        List<PropertyRecord> records = new ArrayList<>();
        PropertyRecord current = null;
        int space = PropertyType.getPayloadSizeLongs();
        var idGenerator = propertyStore.getIdGenerator();
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
                relationshipRecord.setIdTo(next);
                next.setId(idGenerator.nextId(NULL_CONTEXT));
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

    private static void startAndAwaitCompletionOf(DeleteViolatingRelationshipsStep step) throws InterruptedException {
        step.start(0);
        step.receive(0, null);
        step.awaitCompleted();
    }
}

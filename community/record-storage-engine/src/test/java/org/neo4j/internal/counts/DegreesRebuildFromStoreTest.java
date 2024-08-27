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
package org.neo4j.internal.counts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.Command.GroupDegreeCommand.combinedKeyOnGroupAndDirection;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.GROUP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordStorageEngineTestUtils.applyLogicalChanges;
import static org.neo4j.internal.recordstorage.RecordStorageEngineTestUtils.openSimpleStorageEngine;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;

import java.util.ArrayList;
import java.util.List;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.factory.primitive.LongLongMaps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.collection.diffset.LongDiffSets;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.FlatRelationshipModifications;
import org.neo4j.kernel.impl.api.FlatRelationshipModifications.RelationshipData;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@ExtendWith(RandomExtension.class)
@EphemeralPageCacheExtension
class DegreesRebuildFromStoreTest {
    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomSupport random;

    @Test
    void skipNotUsedRecordsOnDegreeStoreRebuild() throws Exception {
        // given a dataset containing mixed sparse and dense nodes with relationships in random directions,
        //       where some chains have been marked as having external degrees
        int denseThreshold = dense_node_threshold.defaultValue();
        RecordDatabaseLayout layout = RecordDatabaseLayout.ofFlat(directory.homePath());
        int[] relationshipTypes;
        MutableLongLongMap expectedDegrees = LongLongMaps.mutable.empty();
        Config config = config(denseThreshold);
        try (Lifespan life = new Lifespan()) {
            RecordStorageEngine storageEngine = openStorageEngine(layout, config);
            relationshipTypes = createRelationshipTypes(storageEngine);
            life.add(storageEngine);
            generateData(storageEngine, denseThreshold, relationshipTypes);
            storageEngine
                    .relationshipGroupDegreesStore()
                    .accept(
                            (groupId, direction, degree) ->
                                    expectedDegrees.put(combinedKeyOnGroupAndDirection(groupId, direction), degree),
                            NULL_CONTEXT);
            assertThat(expectedDegrees.isEmpty()).isFalse();

            RelationshipGroupStore groupStore =
                    storageEngine.testAccessNeoStores().getRelationshipGroupStore();
            try (StoreCursors storageCursors = storageEngine.createStorageCursors(NULL_CONTEXT)) {
                long highId = groupStore.getIdGenerator().getHighId();
                assertThat(highId).isGreaterThan(1);
                for (int i = 10; i < highId; i++) {
                    RelationshipGroupRecord record = groupStore.getRecordByCursor(
                            i,
                            new RelationshipGroupRecord(i),
                            RecordLoad.ALWAYS,
                            storageCursors.readCursor(GROUP_CURSOR),
                            EmptyMemoryTracker.INSTANCE);
                    expectedDegrees.remove(
                            combinedKeyOnGroupAndDirection(record.getId(), RelationshipDirection.OUTGOING));
                    expectedDegrees.remove(
                            combinedKeyOnGroupAndDirection(record.getId(), RelationshipDirection.INCOMING));
                    expectedDegrees.remove(combinedKeyOnGroupAndDirection(record.getId(), RelationshipDirection.LOOP));
                    record.setInUse(false);
                    try (var groupStoreCursor = storageCursors.writeCursor(GROUP_CURSOR)) {
                        groupStore.updateRecord(record, groupStoreCursor, NULL_CONTEXT, storageCursors);
                    }
                }
            }
            storageEngine.checkpoint(DatabaseFlushEvent.NULL, NULL_CONTEXT);
        }

        // when
        directory.getFileSystem().deleteFile(layout.relationshipGroupDegreesStore());
        rebuildAndVerify(layout, config, expectedDegrees);
    }

    @Test
    void shouldRebuildDegreesStore() throws Exception {
        // given a dataset containing mixed sparse and dense nodes with relationships in random directions,
        //       where some chains have been marked as having external degrees
        int denseThreshold = dense_node_threshold.defaultValue();
        RecordDatabaseLayout layout = RecordDatabaseLayout.ofFlat(directory.homePath());
        int[] relationshipTypes;
        MutableLongLongMap expectedDegrees = LongLongMaps.mutable.empty();
        Config config = config(denseThreshold);
        try (Lifespan life = new Lifespan()) {
            RecordStorageEngine storageEngine = openStorageEngine(layout, config);
            relationshipTypes = createRelationshipTypes(storageEngine);
            life.add(storageEngine);
            generateData(storageEngine, denseThreshold, relationshipTypes);
            storageEngine
                    .relationshipGroupDegreesStore()
                    .accept(
                            (groupId, direction, degree) ->
                                    expectedDegrees.put(combinedKeyOnGroupAndDirection(groupId, direction), degree),
                            NULL_CONTEXT);
            assertThat(expectedDegrees.isEmpty()).isFalse();
            storageEngine.checkpoint(DatabaseFlushEvent.NULL, NULL_CONTEXT);
        }

        // when
        directory.getFileSystem().deleteFile(layout.relationshipGroupDegreesStore());
        rebuildAndVerify(layout, config, expectedDegrees);
    }

    private void rebuildAndVerify(RecordDatabaseLayout layout, Config config, MutableLongLongMap expectedDegrees) {
        rebuildAndVerifyDirectlyUsingRebuilderDirectly(layout, config, expectedDegrees);
        rebuildAndVerifyByStartingStorageEngine(layout, config, expectedDegrees);
    }

    private void rebuildAndVerifyDirectlyUsingRebuilderDirectly(
            DatabaseLayout layout, Config config, MutableLongLongMap expectedDegrees) {
        MutableLongLongMap builtExpectedDegrees = LongLongMaps.mutable.empty();
        var pageCacheTracer = PageCacheTracer.NULL;
        try (NeoStores neoStores = new StoreFactory(
                        layout,
                        config,
                        new DefaultIdGeneratorFactory(
                                directory.getFileSystem(), immediate(), pageCacheTracer, layout.getDatabaseName()),
                        pageCache,
                        pageCacheTracer,
                        directory.getFileSystem(),
                        NullLogProvider.getInstance(),
                        NULL_CONTEXT_FACTORY,
                        false,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL)
                .openAllNeoStores()) {
            DegreesRebuildFromStore rebuild = new DegreesRebuildFromStore(
                    pageCache,
                    neoStores,
                    layout,
                    NULL_CONTEXT_FACTORY,
                    NullLogProvider.getInstance(),
                    Configuration.withBatchSize(Configuration.DEFAULT, 100));
            rebuild.rebuild(
                    new DegreeUpdater() {
                        @Override
                        public void increment(long groupId, RelationshipDirection direction, long degree) {
                            builtExpectedDegrees.put(combinedKeyOnGroupAndDirection(groupId, direction), degree);
                        }

                        @Override
                        public void close() {}
                    },
                    NULL_CONTEXT,
                    EmptyMemoryTracker.INSTANCE);
        }
        assertThat(builtExpectedDegrees).isEqualTo(expectedDegrees);
    }

    private void rebuildAndVerifyByStartingStorageEngine(
            RecordDatabaseLayout layout, Config config, MutableLongLongMap expectedDegrees) {
        MutableLongLongMap builtExpectedDegrees = LongLongMaps.mutable.empty();
        try (Lifespan life = new Lifespan()) {
            RecordStorageEngine storageEngine = life.add(openStorageEngine(layout, config));
            storageEngine
                    .relationshipGroupDegreesStore()
                    .accept(
                            (groupId, direction, degree) -> builtExpectedDegrees.put(
                                    combinedKeyOnGroupAndDirection(groupId, direction), degree),
                            NULL_CONTEXT);
        }
        assertThat(builtExpectedDegrees).isEqualTo(expectedDegrees);
    }

    private static int[] createRelationshipTypes(RecordStorageEngine storageEngine) {
        int[] types = new int[3];
        IdGenerator idGenerator = storageEngine
                .testAccessNeoStores()
                .getRelationshipTypeTokenStore()
                .getIdGenerator();
        for (int i = 0; i < types.length; i++) {
            types[i] = (int) idGenerator.nextId(NULL_CONTEXT);
        }
        return types;
    }

    private void generateData(RecordStorageEngine storageEngine, int denseThreshold, int[] relationshipTypes)
            throws Exception {
        int numNodes = 100;
        long[] nodes = new long[numNodes];
        applyLogicalChanges(storageEngine, (state, tx) -> {
            NodeStore nodeStore = storageEngine.testAccessNeoStores().getNodeStore();
            IdGenerator idGenerator = nodeStore.getIdGenerator();
            for (int i = 0; i < numNodes; i++) {
                nodes[i] = idGenerator.nextId(NULL_CONTEXT);
                tx.visitCreatedNode(nodes[i]);
            }
        });

        RelationshipStore relationshipStore =
                storageEngine.testAccessNeoStores().getRelationshipStore();
        IdGenerator idGenerator = relationshipStore.getIdGenerator();
        List<RelationshipData> relationships = new ArrayList<>();
        int numRelationships = numNodes * denseThreshold;
        for (int i = 0; i < numRelationships; i++) {
            relationships.add(new RelationshipData(
                    idGenerator.nextId(NULL_CONTEXT),
                    random.among(relationshipTypes),
                    random.among(nodes),
                    random.among(nodes)));
        }
        applyLogicalChanges(storageEngine, (state, tx) -> {
            NodeState nodeState = mock(NodeState.class);
            when(nodeState.labelDiffSets()).thenReturn(LongDiffSets.EMPTY);
            when(state.getNodeState(anyLong())).thenReturn(nodeState);
            tx.visitRelationshipModifications(
                    new FlatRelationshipModifications(relationships.toArray(new RelationshipData[0])));
        });
    }

    private RecordStorageEngine openStorageEngine(RecordDatabaseLayout layout, Config config) {
        return openSimpleStorageEngine(directory.getFileSystem(), pageCache, layout, config);
    }

    private static Config config(int denseThreshold) {
        return Config.defaults(dense_node_threshold, denseThreshold);
    }
}

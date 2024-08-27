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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.GROUP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.internal.recordstorage.RelationshipGroupGetter.RelationshipGroupMonitor.EMPTY;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.RelationshipGroupGetter.RelationshipGroupPosition;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class RelationshipGroupGetterTest {
    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private PageCache pageCache;

    @Inject
    private DatabaseLayout databaseLayout;

    private NeoStores stores;
    private RelationshipGroupStore groupStore;
    private RecordAccess<RelationshipGroupRecord, Integer> groupRecords;
    private RecordAccess<NodeRecord, Void> nodeRecords;
    private RelationshipGroupGetter groupGetter;
    private StoreCursors storeCursors;

    @BeforeEach
    void openStore() {
        InternalLogProvider logProvider = NullLogProvider.getInstance();
        var pageCacheTracer = PageCacheTracer.NULL;
        StoreFactory storeFactory = new StoreFactory(
                databaseLayout,
                Config.defaults(),
                new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                pageCache,
                pageCacheTracer,
                fs,
                logProvider,
                NULL_CONTEXT_FACTORY,
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
        stores = storeFactory.openNeoStores(StoreType.RELATIONSHIP_GROUP, StoreType.NODE, StoreType.NODE_LABEL);
        groupStore = spy(stores.getRelationshipGroupStore());
        NodeStore nodeStore = stores.getNodeStore();
        storeCursors = new CachedStoreCursors(stores, NULL_CONTEXT);
        groupRecords = new DirectRecordAccess<>(
                groupStore,
                Loaders.relationshipGroupLoader(groupStore, storeCursors),
                NULL_CONTEXT,
                GROUP_CURSOR,
                storeCursors,
                EmptyMemoryTracker.INSTANCE);
        nodeRecords = new DirectRecordAccess<>(
                nodeStore,
                Loaders.nodeLoader(nodeStore, storeCursors),
                NULL_CONTEXT,
                NODE_CURSOR,
                storeCursors,
                EmptyMemoryTracker.INSTANCE);
        groupGetter = new RelationshipGroupGetter(groupStore.getIdGenerator(), NULL_CONTEXT);
    }

    @AfterEach
    void closeStore() {
        stores.close();
    }

    @Test
    void shouldAbortLoadingGroupChainIfComeTooFar() {
        // GIVEN a node with relationship group chain (of types) 2 -> 4 -> 10 -> 23
        RelationshipGroupRecord group2 = group(2);
        RelationshipGroupRecord group4 = group(4);
        RelationshipGroupRecord group10 = group(10);
        RelationshipGroupRecord group23 = group(23);
        linkAndWrite(group2, group4, group10, group23);
        RelationshipGroupGetter groupGetter = new RelationshipGroupGetter(groupStore.getIdGenerator(), NULL_CONTEXT);
        NodeRecord node = new NodeRecord(0)
                .initialize(true, NULL_REFERENCE.longValue(), true, group2.getId(), NO_LABELS_FIELD.longValue());

        // WHEN trying to find relationship group 7
        RelationshipGroupPosition result = groupGetter.getRelationshipGroup(node, 7, groupRecords, EMPTY);

        // THEN only groups 2, 4 and 10 should have been loaded
        InOrder verification = inOrder(groupStore);
        verification
                .verify(groupStore)
                .getRecordByCursor(
                        eq(group2.getId()), any(RelationshipGroupRecord.class), any(RecordLoad.class), any(), any());
        verification
                .verify(groupStore)
                .getRecordByCursor(
                        eq(group4.getId()), any(RelationshipGroupRecord.class), any(RecordLoad.class), any(), any());
        verification
                .verify(groupStore)
                .getRecordByCursor(
                        eq(group10.getId()), any(RelationshipGroupRecord.class), any(RecordLoad.class), any(), any());
        verification
                .verify(groupStore, never())
                .getRecordByCursor(
                        eq(group23.getId()), any(RelationshipGroupRecord.class), any(RecordLoad.class), any(), any());

        // it should also be reported as not found
        assertThat(result.group()).isNull();
        // with group 4 as closes previous one
        assertThat(result.closestPrevious().forReadingData()).isEqualTo(group4);
        // the group getter above won't mark any group as changed and will therefore not keep those loaded records,
        // which is why no "prev" fields will be set when now asserting them here.
        assertGroupChain(node, 4, 0);
    }

    @Test
    void shouldRelinkPrevWhenInsertingGroupInTheMiddle() {
        // given a node with relationship group chain (of types) 1 -> 2 -> 10 -> 12
        RelationshipGroupRecord group1 = group(1);
        RelationshipGroupRecord group2 = group(2);
        RelationshipGroupRecord group10 = group(10);
        RelationshipGroupRecord group12 = group(12);
        linkAndWrite(group1, group2, group10, group12);
        RecordAccess.RecordProxy<NodeRecord, Void> nodeChange = nodeRecords.create(0, null, NULL_CONTEXT);
        nodeChange
                .forChangingData()
                .initialize(true, NULL_REFERENCE.longValue(), true, group1.getId(), NO_LABELS_FIELD.longValue());

        // when inserting a group 5, i.e. 1 -> 2 -> 5 -> 10 -> 12
        //                                          ^
        int newType = 5;
        RelationshipGroupRecord createdGroup = groupGetter
                .getOrCreateRelationshipGroup(nodeChange, newType, groupRecords)
                .forReadingLinkage();

        // then all next and prev links should match
        assertThat(createdGroup.getType()).isEqualTo(newType);
        assertGroupChain(nodeChange.forReadingLinkage(), 5, 4);
    }

    @Test
    void shouldRelinkPrevWhenInsertingGroupFirst() {
        // given a node with relationship group chain (of types) 2 -> 3
        RelationshipGroupRecord group2 = group(2);
        RelationshipGroupRecord group3 = group(3);
        linkAndWrite(group2, group3);
        RecordAccess.RecordProxy<NodeRecord, Void> nodeChange = nodeRecords.create(0, null, NULL_CONTEXT);
        nodeChange
                .forChangingData()
                .initialize(true, NULL_REFERENCE.longValue(), true, group2.getId(), NO_LABELS_FIELD.longValue());

        // when inserting a group 1, i.e. 1 -> 2 -> 3
        //                                ^
        int newType = 1;
        // The "prev" field is very special in that it's not persistent, this means that the group getter will not mark
        // the previously-first group
        // as changed, but it will still update its prev field. Let's fake an update to this record so that it sticks in
        // the record changes.
        groupRecords.getOrLoad(group2.getId(), null).forChangingLinkage();
        RelationshipGroupRecord createdGroup = groupGetter
                .getOrCreateRelationshipGroup(nodeChange, newType, groupRecords)
                .forReadingLinkage();

        // then all next and prev links should match
        assertThat(createdGroup.getType()).isEqualTo(newType);
        assertGroupChain(nodeChange.forReadingLinkage(), 3, 2);
    }

    /**
     * @param node to start from.
     * @param expectedChainLength the expected length of the chain.
     * @param numPrevsToAssert number of groups to assert "prev" pointer on.. This is required because "prev" is an in-memory state, not persisted
     * and therefore the test knows how far the {@link RelationshipGroupGetter} has traversed the chain and therefore how far
     * into the chain the "prev" fields are set to valid pointers.
     */
    private void assertGroupChain(NodeRecord node, int expectedChainLength, int numPrevsToAssert) {
        assertThat(node.isDense()).isTrue();
        long groupId = node.getNextRel();
        RelationshipGroupRecord prev = null;
        int count = 0;
        while (!NULL_REFERENCE.is(groupId)) {
            RelationshipGroupRecord group =
                    groupRecords.getOrLoad(groupId, null).forReadingLinkage();
            if (count > 0) {
                if (count < numPrevsToAssert) {
                    assertThat(group.getPrev()).isEqualTo(prev.getId());
                }
                assertThat(prev.getNext()).isEqualTo(group.getId());
            } else {
                assertThat(group.getPrev()).isEqualTo(NULL_REFERENCE.longValue());
            }
            prev = group;
            groupId = group.getNext();
            count++;
        }
        assertThat(prev.getNext()).isEqualTo(NULL_REFERENCE.longValue());
        assertThat(count).isEqualTo(expectedChainLength);
    }

    private void linkAndWrite(RelationshipGroupRecord... groups) {
        for (int i = 0; i < groups.length; i++) {
            if (i > 0) {
                groups[i].setPrev(groups[i - 1].getId());
            }
            if (i < groups.length - 1) {
                groups[i].setNext(groups[i + 1].getId());
            }
        }
        try (var cursor = storeCursors.writeCursor(GROUP_CURSOR)) {
            for (RelationshipGroupRecord group : groups) {
                groupStore.updateRecord(group, cursor, NULL_CONTEXT, storeCursors);
            }
        }
    }

    private RelationshipGroupRecord group(int type) {
        return new RelationshipGroupRecord(groupStore.getIdGenerator().nextId(NULL_CONTEXT))
                .initialize(
                        true,
                        type,
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue(),
                        NULL_REFERENCE.longValue());
    }
}

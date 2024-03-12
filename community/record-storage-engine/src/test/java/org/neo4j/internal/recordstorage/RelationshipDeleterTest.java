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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.recordstorage.RecordAccess.LoadMonitor.NULL_MONITOR;
import static org.neo4j.internal.recordstorage.RelationshipModifier.DEFAULT_EXTERNAL_DEGREES_THRESHOLD_SWITCH;
import static org.neo4j.kernel.impl.api.FlatRelationshipModifications.singleDelete;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.lock.ResourceLocker.IGNORE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.counts.DegreeUpdater;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.cursor.StoreCursors;

class RelationshipDeleterTest {
    private static final long NULL = NULL_REFERENCE.longValue();

    private RelationshipDeleter deleter;
    private MapRecordStore store;
    private RecordAccessSet recordChanges;

    @BeforeEach
    void setUp() {
        RelationshipGroupGetter relationshipGroupGetter =
                new RelationshipGroupGetter(idSequence(), CursorContext.NULL_CONTEXT);
        PropertyTraverser propertyTraverser = new PropertyTraverser();
        PropertyDeleter propertyDeleter = new PropertyDeleter(
                propertyTraverser,
                null,
                null,
                NullLogProvider.getInstance(),
                Config.defaults(),
                CursorContext.NULL_CONTEXT,
                StoreCursors.NULL);
        deleter = new RelationshipDeleter(
                relationshipGroupGetter, propertyDeleter, DEFAULT_EXTERNAL_DEGREES_THRESHOLD_SWITCH, false);
        store = new MapRecordStore();
        recordChanges = store.newRecordChanges(NULL_MONITOR, MapRecordStore.Monitor.NULL);
    }

    @Test
    void shouldDecrementDegreeOnceOnFirstIfLoopOnSparseChain() {
        // given
        // a node w/ relationship chain (A) -> (B) -> (C) from start node POV where (A) is a loop
        long startNode = 1;
        long otherNode1 = 2;
        long otherNode2 = 3;
        long relA = 11;
        long relB = 12;
        long relC = 13;
        int type = 0;
        store.write(new NodeRecord(startNode).initialize(true, NULL, false, relA, NO_LABELS_FIELD.longValue()));
        store.write(new NodeRecord(otherNode1).initialize(true, NULL, false, relB, NO_LABELS_FIELD.longValue()));
        store.write(new NodeRecord(otherNode2).initialize(true, NULL, false, relC, NO_LABELS_FIELD.longValue()));
        store.write(new RelationshipRecord(relA)
                .initialize(true, NULL, startNode, startNode, type, 3, relB, 3, relB, true, true));
        store.write(new RelationshipRecord(relB)
                .initialize(true, NULL, startNode, otherNode1, type, relA, relC, 1, NULL, false, true));
        store.write(new RelationshipRecord(relC)
                .initialize(true, NULL, startNode, otherNode2, type, relB, NULL, 1, NULL, false, true));

        // when deleting relB
        deleter.relationshipDelete(
                singleDelete(relB, type, startNode, otherNode1).deletions(),
                recordChanges,
                mock(DegreeUpdater.class),
                mock(MappedNodeDataLookup.class),
                INSTANCE,
                IGNORE);

        // then relA should be updated with correct degrees, i.e. from 3 -> 2 on both its chains
        RecordAccess.RecordProxy<RelationshipRecord, Void> relAChange =
                recordChanges.getRelRecords().getIfLoaded(relA);
        assertTrue(relAChange.isChanged());
        assertEquals(2, relAChange.forReadingData().getFirstPrevRel());
        assertEquals(2, relAChange.forReadingData().getSecondPrevRel());
    }

    @Test
    void shouldDecrementDegreeOnceOnFirstIfLoopOnDenseLoopChain() {
        // given
        // a node w/ relationship group chain (G,type:0) -> (H,type:1)
        // where (H) has relationship chain (A) -> (B) -> (C) from start node POV where (A) is a loop
        long node = 1;
        long groupG = 7;
        long groupH = 8;
        long relA = 11;
        long relB = 12;
        long relC = 13;
        int type = 1;
        store.write(new RelationshipGroupRecord(groupG).initialize(true, 0, NULL, NULL, NULL, node, groupH));
        store.write(new RelationshipGroupRecord(groupH).initialize(true, type, NULL, NULL, relA, node, NULL));
        store.write(new NodeRecord(node).initialize(true, NULL, true, groupG, NO_LABELS_FIELD.longValue()));
        store.write(
                new RelationshipRecord(relA).initialize(true, NULL, node, node, type, 3, relB, 3, relB, true, true));
        store.write(new RelationshipRecord(relB)
                .initialize(true, NULL, node, node, type, relA, relC, relA, relC, false, false));
        store.write(new RelationshipRecord(relC)
                .initialize(true, NULL, node, node, type, relB, NULL, relB, NULL, false, false));
        MappedNodeDataLookup groupLookup = mock(MappedNodeDataLookup.class);
        when(groupLookup.group(node, type, false))
                .thenAnswer(
                        invocationOnMock -> recordChanges.getRelGroupRecords().getOrLoad(groupH, null));

        // when deleting relB
        deleter.relationshipDelete(
                singleDelete(relB, type, node, node).deletions(),
                recordChanges,
                mock(DegreeUpdater.class),
                groupLookup,
                INSTANCE,
                IGNORE);

        // then relA should be updated with correct degrees, i.e. from 3 -> 2 on both its chains
        RecordAccess.RecordProxy<RelationshipRecord, Void> relAChange =
                recordChanges.getRelRecords().getIfLoaded(relA);
        assertTrue(relAChange.isChanged());
        assertEquals(2, relAChange.forReadingData().getFirstPrevRel());
        assertEquals(2, relAChange.forReadingData().getSecondPrevRel());
    }

    private static IdSequence idSequence() {
        AtomicLong nextId = new AtomicLong();
        return cursorContext -> nextId.getAndIncrement();
    }
}

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
package org.neo4j.kernel.impl.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

class RelationshipStoreConsistentReadTest extends RecordStoreConsistentReadTest<RelationshipRecord, RelationshipStore> {
    // Constants for the contents of the existing record
    private static final int FIRST_NODE = 2;
    private static final int SECOND_NODE = 3;
    private static final int TYPE = 4;
    private static final int FIRST_PREV_REL = 5;
    private static final int FIRST_NEXT_REL = 6;
    private static final int SECOND_PREV_REL = 7;
    private static final int SECOND_NEXT_REL = 8;

    @Override
    protected RelationshipRecord createNullRecord(long id) {
        RelationshipRecord record = new RelationshipRecord(id);
        record.initialize(false, 0, 0, 0, 0, 0, 0, 0, 0, false, false);
        return record;
    }

    @Override
    protected RelationshipRecord createExistingRecord(boolean light) {
        RelationshipRecord record = new RelationshipRecord(ID);
        record.initialize(
                true,
                0,
                FIRST_NODE,
                SECOND_NODE,
                TYPE,
                FIRST_PREV_REL,
                FIRST_NEXT_REL,
                SECOND_PREV_REL,
                SECOND_NEXT_REL,
                true,
                true);
        return record;
    }

    @Override
    protected RelationshipRecord getLight(long id, RelationshipStore store, PageCursor pageCursor) {
        return store.getRecordByCursor(id, store.newRecord(), NORMAL, pageCursor, EmptyMemoryTracker.INSTANCE);
    }

    @Override
    protected void assertRecordsEqual(RelationshipRecord actualRecord, RelationshipRecord expectedRecord) {
        assertNotNull(actualRecord, "actualRecord");
        assertNotNull(expectedRecord, "expectedRecord");
        assertThat(actualRecord.getFirstNextRel()).as("getFirstNextRel").isEqualTo(expectedRecord.getFirstNextRel());
        assertThat(actualRecord.getFirstNode()).as("getFirstNode").isEqualTo(expectedRecord.getFirstNode());
        assertThat(actualRecord.getFirstPrevRel()).as("getFirstPrevRel").isEqualTo(expectedRecord.getFirstPrevRel());
        assertThat(actualRecord.getSecondNextRel()).as("getSecondNextRel").isEqualTo(expectedRecord.getSecondNextRel());
        assertThat(actualRecord.getSecondNode()).as("getSecondNode").isEqualTo(expectedRecord.getSecondNode());
        assertThat(actualRecord.getSecondPrevRel()).as("getSecondPrevRel").isEqualTo(expectedRecord.getSecondPrevRel());
        assertThat(actualRecord.getType()).as("getType").isEqualTo(expectedRecord.getType());
        assertThat(actualRecord.isFirstInFirstChain())
                .as("isFirstInFirstChain")
                .isEqualTo(expectedRecord.isFirstInFirstChain());
        assertThat(actualRecord.isFirstInSecondChain())
                .as("isFirstInSecondChain")
                .isEqualTo(expectedRecord.isFirstInSecondChain());
        assertThat(actualRecord.getId()).as("getId").isEqualTo(expectedRecord.getId());
        assertThat(actualRecord.getId()).as("getLongId").isEqualTo(expectedRecord.getId());
        assertThat(actualRecord.getNextProp()).as("getNextProp").isEqualTo(expectedRecord.getNextProp());
        assertThat(actualRecord.inUse()).as("inUse").isEqualTo(expectedRecord.inUse());
    }

    @Override
    protected RelationshipStore getStore(NeoStores neoStores) {
        return neoStores.getRelationshipStore();
    }

    @Override
    protected PageCursor getCursor(StoreCursors storeCursors) {
        return storeCursors.readCursor(RELATIONSHIP_CURSOR);
    }
}

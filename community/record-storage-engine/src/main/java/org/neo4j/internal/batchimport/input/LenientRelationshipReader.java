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
package org.neo4j.internal.batchimport.input;

import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;

import java.io.IOException;
import org.neo4j.batchimport.api.ReadBehaviour;
import org.neo4j.batchimport.api.ReadBehaviour.PropertyInclusion;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.common.EntityType;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.TokenHolders;

class LenientRelationshipReader extends LenientStoreInputChunk {
    private final RelationshipStore relationshipStore;
    private final RelationshipRecord record;

    LenientRelationshipReader(
            ReadBehaviour readBehaviour,
            RelationshipStore relationshipStore,
            PropertyStore propertyStore,
            TokenHolders tokenHolders,
            CursorContextFactory contextFactory,
            StoreCursors storeCursors,
            Group group,
            MemoryTracker memoryTracker) {
        super(
                readBehaviour,
                propertyStore,
                tokenHolders,
                contextFactory,
                storeCursors,
                storeCursors.readCursor(RELATIONSHIP_CURSOR),
                group,
                memoryTracker);
        this.relationshipStore = relationshipStore;
        this.record = relationshipStore.newRecord();
    }

    @Override
    void readAndVisit(long id, InputEntityVisitor visitor, StoreCursors storeCursors, MemoryTracker memoryTracker)
            throws IOException {
        relationshipStore.getRecordByCursor(id, record, RecordLoad.LENIENT_CHECK, cursor, memoryTracker);
        if (record.inUse()) {
            relationshipStore.ensureHeavy(record, storeCursors, memoryTracker);
            int relationshipType = record.getType();
            String relationshipTypeName = LenientStoreInput.getTokenByIdSafe(
                            tokenHolders.relationshipTypeTokens(), relationshipType)
                    .name();
            if (readBehaviour.shouldIncludeRelationship(
                    record.getFirstNode(), record.getSecondNode(), id, relationshipTypeName)) {
                visitor.type(relationshipTypeName);
                visitor.startId(record.getFirstNode(), group);
                visitor.endId(record.getSecondNode(), group);
                visitPropertyChainNoThrow(
                        visitor, record, EntityType.RELATIONSHIP, new String[] {relationshipTypeName}, memoryTracker);
                visitor.endOfEntity();
            }
        } else {
            readBehaviour.unused();
        }
    }

    @Override
    String recordType() {
        return "Relationship";
    }

    @Override
    boolean shouldIncludeProperty(ReadBehaviour readBehaviour, String key, String[] owningEntityTokens) {
        return readBehaviour.shouldIncludeRelationshipProperty(key, owningEntityTokens[0]) == PropertyInclusion.INCLUDE;
    }
}

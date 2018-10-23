/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.newchecker;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.consistency.checking.cache.CacheSlots.longOf;

/**
 * A batch of relationship records as one contiguous piece of memory. Used as means of communication between threads checking relationship chains.
 */
class BatchedRelationshipRecords
{
    // id
    // start node
    // end node
    // start node prev rel
    // start node next rel
    // end node prev rel
    // end node next rel
    // flags
    private static final int FIELDS_PER_RELATIONSHIP = 8;
    private static final int BATCH_SIZE = 1_000;

    private long[] fields = new long[BATCH_SIZE * FIELDS_PER_RELATIONSHIP];
    private int writeCursor;
    private int readCursor;

    int numberOfRelationships()
    {
        return writeCursor / FIELDS_PER_RELATIONSHIP;
    }

    boolean hasMoreSpace()
    {
        return writeCursor < fields.length;
    }

    void add( RelationshipRecord relationshipRecord )
    {
        fields[writeCursor++] = relationshipRecord.getId();
        fields[writeCursor++] = relationshipRecord.getFirstNode();
        fields[writeCursor++] = relationshipRecord.getSecondNode();
        fields[writeCursor++] = relationshipRecord.getFirstPrevRel();
        fields[writeCursor++] = relationshipRecord.getFirstNextRel();
        fields[writeCursor++] = relationshipRecord.getSecondPrevRel();
        fields[writeCursor++] = relationshipRecord.getSecondNextRel();
        fields[writeCursor++] = longOf( relationshipRecord.isFirstInFirstChain() ) | longOf( relationshipRecord.isFirstInSecondChain() ) << 1;
    }

    boolean fillNext( RelationshipRecord relationshipRecord )
    {
        if ( readCursor < writeCursor )
        {
            relationshipRecord.setId( fields[readCursor++] );
            relationshipRecord.setFirstNode( fields[readCursor++] );
            relationshipRecord.setSecondNode( fields[readCursor++] );
            relationshipRecord.setFirstPrevRel( fields[readCursor++] );
            relationshipRecord.setFirstNextRel( fields[readCursor++] );
            relationshipRecord.setSecondPrevRel( fields[readCursor++] );
            relationshipRecord.setSecondNextRel( fields[readCursor++] );
            long flags = fields[readCursor++];
            relationshipRecord.setFirstInFirstChain( (flags & 0x1) != 0 );
            relationshipRecord.setFirstInSecondChain( (flags & 0x2) != 0 );
            relationshipRecord.setInUse( true );
            return true;
        }
        return false;
    }
}

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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.Direction;

import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

class DegreeCounter
{
    private DegreeCounter()
    {
    }

    static long countByFirstPrevPointer( long relationshipId, RecordCursor<RelationshipRecord> cursor,
            long nodeId, RelationshipRecord relationshipRecord )
    {
        if ( relationshipId == Record.NO_NEXT_RELATIONSHIP.longValue() )
        {
            return 0;
        }
        cursor.next( relationshipId, relationshipRecord, FORCE );
        if ( relationshipRecord.getFirstNode() == nodeId )
        {
            return relationshipRecord.getFirstPrevRel();
        }
        if ( relationshipRecord.getSecondNode() == nodeId )
        {
            return relationshipRecord.getSecondPrevRel();
        }
        throw new InvalidRecordException( "Node " + nodeId + " neither start nor end node of " + relationshipRecord );
    }

    static int countRelationshipsInGroup( long groupId, Direction direction, Integer type, long nodeId,
            RelationshipRecord relationshipRecord, RelationshipGroupRecord groupRecord, RecordCursors cursors )
    {
        int count = 0;
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.longValue() )
        {
            boolean groupRecordInUse = cursors.relationshipGroup().next( groupId, groupRecord, FORCE );
            if ( groupRecordInUse && ( type == null || groupRecord.getType() == type ) )
            {
                count += nodeDegreeByDirection( direction, nodeId, relationshipRecord, groupRecord, cursors );
                if ( type != null )
                {
                    // we have read the only type we were interested on, so break the look
                    break;
                }
            }
            groupId = groupRecord.getNext();
        }
        return count;
    }

    private static long nodeDegreeByDirection( Direction direction, long nodeId,
            RelationshipRecord relationshipRecord, RelationshipGroupRecord groupRecord, RecordCursors cursors )
    {
        long firstLoop = groupRecord.getFirstLoop();
        RecordCursor<RelationshipRecord> cursor = cursors.relationship();
        long loopCount = countByFirstPrevPointer( firstLoop, cursor, nodeId, relationshipRecord );
        switch ( direction )
        {
        case OUTGOING:
        {
            long firstOut = groupRecord.getFirstOut();
            return countByFirstPrevPointer( firstOut, cursor, nodeId, relationshipRecord ) + loopCount;
        }
        case INCOMING:
        {
            long firstIn = groupRecord.getFirstIn();
            return countByFirstPrevPointer( firstIn, cursor, nodeId, relationshipRecord ) + loopCount;
        }
        case BOTH:
        {
            long firstOut = groupRecord.getFirstOut();
            long firstIn = groupRecord.getFirstIn();
            return countByFirstPrevPointer( firstOut, cursor, nodeId, relationshipRecord ) +
                    countByFirstPrevPointer( firstIn, cursor, nodeId, relationshipRecord ) + loopCount;
        }
        default:
            throw new IllegalArgumentException( direction.name() );
        }
    }
}

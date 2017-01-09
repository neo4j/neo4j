/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.DegreeItem;

import static org.neo4j.kernel.impl.api.store.DegreeCounter.countByFirstPrevPointer;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

class DegreeItemDenseCursor implements Cursor<DegreeItem>, DegreeItem
{
    private final NodeRecord nodeRecord;
    private final RelationshipGroupRecord relationshipGroupRecord;
    private final RelationshipRecord relationshipRecord;
    private final RecordCursors recordCursors;

    private long groupId;
    private int type;
    private long outgoing;
    private long incoming;

    DegreeItemDenseCursor( long groupId,
            NodeRecord nodeRecord,
            RelationshipGroupRecord relationshipGroupRecord,
            RelationshipRecord relationshipRecord,
            RecordCursors recordCursors )
    {
        this.groupId = groupId;
        this.nodeRecord = nodeRecord;
        this.relationshipGroupRecord = relationshipGroupRecord;
        this.relationshipRecord = relationshipRecord;
        this.recordCursors = recordCursors;
    }

    @Override
    public boolean next()
    {
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            boolean groupRecordInUse = recordCursors.relationshipGroup().next( groupId, relationshipGroupRecord, FORCE );
            groupId = relationshipGroupRecord.getNext();
            if ( groupRecordInUse )
            {
                this.type = relationshipGroupRecord.getType();

                long firstLoop = relationshipGroupRecord.getFirstLoop();
                long firstOut = relationshipGroupRecord.getFirstOut();
                long firstIn = relationshipGroupRecord.getFirstIn();

                RecordCursor<RelationshipRecord> relationshipCursor = recordCursors.relationship();
                long loop = countByFirstPrevPointer( firstLoop, relationshipCursor, nodeRecord, relationshipRecord );
                this.outgoing =
                        countByFirstPrevPointer( firstOut, relationshipCursor, nodeRecord, relationshipRecord ) + loop;
                this.incoming =
                        countByFirstPrevPointer( firstIn, relationshipCursor, nodeRecord, relationshipRecord ) + loop;
                return true;
            }
        }
        return false;
    }

    @Override
    public void close()
    {
    }

    @Override
    public DegreeItem get()
    {
        return this;
    }

    @Override
    public int type()
    {
        return type;
    }

    @Override
    public long outgoing()
    {
        return outgoing;
    }

    @Override
    public long incoming()
    {
        return incoming;
    }
}

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
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.storageengine.api.RelationshipTypeItem;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

class RelationshipTypeDenseCursor implements Cursor<RelationshipTypeItem>, RelationshipTypeItem
{
    private final RelationshipGroupRecord groupRecord;
    private RecordCursors recordCursors;

    private long groupId;
    private int value = NO_SUCH_RELATIONSHIP_TYPE;

    RelationshipTypeDenseCursor( long groupId, RelationshipGroupRecord groupRecord, RecordCursors recordCursors )
    {
        this.groupId = groupId;
        this.groupRecord = groupRecord;
        this.recordCursors = recordCursors;
    }

    @Override
    public boolean next()
    {
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            boolean groupRecordInUse = recordCursors.relationshipGroup().next( groupId, groupRecord, FORCE );
            groupId = groupRecord.getNext();
            if ( groupRecordInUse )
            {
                value = groupRecord.getType();
                return true;
            }
        }

        value = NO_SUCH_RELATIONSHIP_TYPE;
        return false;
    }

    @Override
    public void close()
    {
    }

    @Override
    public RelationshipTypeItem get()
    {
        return this;
    }

    @Override
    public int getAsInt()
    {
        return value;
    }
}

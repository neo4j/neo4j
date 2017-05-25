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

import java.util.function.Consumer;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Disposable;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.storageengine.api.RelationshipGroupItem;

import static org.neo4j.kernel.impl.api.store.StoreStatement.read;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

public class RelationshipGroupCursor implements RelationshipGroupItem, Cursor<RelationshipGroupItem>, Disposable
{
    private final RelationshipGroupStore relationshipGroupStore;
    private final RelationshipGroupRecord relationshipGroupRecord;
    private final PageCursor cursor;
    private final Consumer<RelationshipGroupCursor> cache;

    private boolean fetched;
    private long relationshipGroupId;

    RelationshipGroupCursor( RelationshipGroupStore relationshipGroupStore, Consumer<RelationshipGroupCursor> cache )
    {
        this.relationshipGroupStore = relationshipGroupStore;
        this.relationshipGroupRecord = relationshipGroupStore.newRecord();
        this.cursor = relationshipGroupStore.newPageCursor();
        this.cache = cache;
    }

    public RelationshipGroupCursor init( long relationshipGroupId )
    {
        this.relationshipGroupId = relationshipGroupId;
        return this;
    }

    @Override
    public boolean next()
    {
        return fetched = fetchNext();
    }

    private boolean fetchNext()
    {

        while ( true )
        {
            if ( Record.NO_NEXT_RELATIONSHIP_GROUP.is( relationshipGroupId ) )
            {
                return false;
            }

            RelationshipGroupRecord record =
                    read( relationshipGroupId, relationshipGroupStore, relationshipGroupRecord, FORCE, cursor );
            relationshipGroupId = record.getNext();
            if ( record.inUse() )
            {
                return true;
            }

            // this record is not in use try with the next
        }
    }

    @Override
    public RelationshipGroupItem get()
    {
        if ( fetched )
        {
            return this;
        }

        throw new IllegalStateException( "Nothing available" );
    }

    @Override
    public long id()
    {
        return relationshipGroupRecord.getId();
    }

    @Override
    public int type()
    {
        return relationshipGroupRecord.getType();
    }

    @Override
    public void close()
    {
        fetched = false;
        relationshipGroupId = StatementConstants.NO_SUCH_RELATIONSHIP;
        cache.accept( this );
    }

    @Override
    public void dispose()
    {
        cursor.close();
    }
}

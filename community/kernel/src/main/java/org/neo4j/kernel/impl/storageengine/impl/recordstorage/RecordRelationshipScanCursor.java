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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;

class RecordRelationshipScanCursor extends RecordRelationshipCursor implements StorageRelationshipScanCursor
{
    private int filterType;
    private long next;
    private long highMark;
    private long nextStoreReference;
    private PageCursor pageCursor;
    private boolean open;

    RecordRelationshipScanCursor( RelationshipStore relationshipStore )
    {
        super( relationshipStore );
    }

    @Override
    public void scan()
    {
        scan( -1 );
    }

    @Override
    public void scan( int type )
    {
        if ( getId() != NO_ID )
        {
            resetState();
        }
        if ( pageCursor == null )
        {
            pageCursor = relationshipPage( 0 );
        }
        this.next = 0;
        this.filterType = type;
        this.highMark = relationshipHighMark();
        this.nextStoreReference = NO_ID;
        this.open = true;
    }

    @Override
    public void single( long reference )
    {
        if ( getId() != NO_ID )
        {
            resetState();
        }
        if ( pageCursor == null )
        {
            pageCursor = relationshipPage( reference );
        }
        this.next = reference >= 0 ? reference : NO_ID;
        this.filterType = -1;
        this.highMark = NO_ID;
        this.nextStoreReference = NO_ID;
        this.open = true;
    }

    @Override
    public boolean next()
    {
        if ( next == NO_ID )
        {
            resetState();
            return false;
        }

        do
        {
            if ( nextStoreReference == next )
            {
                relationshipAdvance( this, pageCursor );
                next++;
                nextStoreReference++;
            }
            else
            {
                relationship( this, next++, pageCursor );
                nextStoreReference = next;
            }

            if ( next > highMark )
            {
                if ( isSingle() )
                {
                    //we are a "single cursor"
                    next = NO_ID;
                    return inUse();
                }
                else
                {
                    //we are a "scan cursor"
                    //Check if there is a new high mark
                    highMark = relationshipHighMark();
                    if ( next > highMark )
                    {
                        next = NO_ID;
                        return isWantedTypeAndInUse();
                    }
                }
            }
        }
        while ( !isWantedTypeAndInUse() );
        return true;
    }

    private boolean isWantedTypeAndInUse()
    {
        return (filterType == -1 || type() == filterType) && inUse();
    }

    @Override
    public void reset()
    {
        if ( open )
        {
            open = false;
            resetState();
        }
    }

    private void resetState()
    {
        setId( next = NO_ID );
    }

    @Override
    public String toString()
    {
        if ( !open )
        {
            return "RelationshipScanCursor[closed state]";
        }
        else
        {
            return "RelationshipScanCursor[id=" + getId() + ", open state with: highMark=" + highMark + ", next=" + next + ", type=" + filterType +
                    ", underlying record=" + super.toString() + "]";
        }
    }

    private boolean isSingle()
    {
        return highMark == NO_ID;
    }

    @Override
    public void close()
    {
        if ( pageCursor != null )
        {
            pageCursor.close();
            pageCursor = null;
        }
    }

    private void relationshipAdvance( RelationshipRecord record, PageCursor pageCursor )
    {
        // When scanning, we inspect RelationshipRecord.inUse(), so using RecordLoad.CHECK is fine
        relationshipStore.nextRecordByCursor( record, RecordLoad.CHECK, pageCursor );
    }
}

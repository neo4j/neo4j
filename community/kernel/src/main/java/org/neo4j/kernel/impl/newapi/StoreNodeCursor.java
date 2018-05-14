/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import java.util.function.LongPredicate;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;

class StoreNodeCursor extends NodeRecord
{
    private Read read;
    private RecordCursor<DynamicRecord> labelCursor;
    private PageCursor pageCursor;
    private long next;
    private long highMark;
    private long nextStoreReference;

    StoreNodeCursor()
    {
        super( NO_ID );
    }

    void scan( Read read )
    {
        if ( getId() != NO_ID )
        {
            reset();
        }
        if ( pageCursor == null )
        {
            pageCursor = read.nodePage( 0 );
        }
        this.next = 0;
        this.highMark = read.nodeHighMark();
        this.nextStoreReference = NO_ID;
        this.read = read;
    }

    void single( long reference, Read read )
    {
        if ( getId() != NO_ID )
        {
            reset();
        }
        if ( pageCursor == null )
        {
            pageCursor = read.nodePage( reference );
        }
        this.next = reference;
        //This marks the cursor as a "single cursor"
        this.highMark = NO_ID;
        this.nextStoreReference = NO_ID;
        this.read = read;
    }

    public long nodeReference()
    {
        return getId();
    }

    public long[] labels()
    {
        return NodeLabelsField.get( this, labelCursor() );
    }

    public boolean hasLabel( int label )
    {
        //Get labels from store and put in intSet, unfortunately we get longs back
        long[] longs = NodeLabelsField.get( this, labelCursor() );
        for ( long labelToken : longs )
        {
            if ( labelToken == label )
            {
                assert (int) labelToken == labelToken : "value too big to be represented as and int";
                return true;
            }
        }
        return false;
    }

    public boolean hasProperties()
    {
        return nextProp != NO_ID;
    }

    public long relationshipGroupReference()
    {
        return isDense() ? getNextRel() : GroupReferenceEncoding.encodeRelationship( getNextRel() );
    }

    public long allRelationshipsReference()
    {
        return isDense() ? RelationshipReferenceEncoding.encodeGroup( getNextRel() ) : getNextRel();
    }

    public long propertiesReference()
    {
        return getNextProp();
    }

    public boolean next( LongPredicate filter )
    {
        if ( next == NO_ID )
        {
            reset();
            return false;
        }

        do
        {
            if ( filter.test( next ) )
            {
                next++;
                setInUse( false );
            }
            else if ( nextStoreReference == next )
            {
                read.nodeAdvance( this, pageCursor );
                next++;
                nextStoreReference++;
            }
            else
            {
                read.node( this, next++, pageCursor );
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
                    highMark = read.nodeHighMark();
                    if ( next > highMark )
                    {
                        next = NO_ID;
                        return inUse();
                    }
                }
            }
        }
        while ( !inUse() );
        return true;
    }

    public void setCurrent( long nodeReference )
    {
        setId( nodeReference );
        setInUse( true );
    }

    public void close()
    {
        if ( !isClosed() )
        {
            read = null;
            reset();
        }
    }

    public boolean isClosed()
    {
        return read == null;
    }

    void reset()
    {
        next = NO_ID;
        setId( NO_ID );
        clear();
    }

    private RecordCursor<DynamicRecord> labelCursor()
    {
        if ( labelCursor == null )
        {
            labelCursor = read.labelCursor();
        }
        return labelCursor;
    }

    private boolean isSingle()
    {
        return highMark == NO_ID;
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "StoreNodeCursor[closed state]";
        }
        else
        {
            return "StoreNodeCursor[id=" + getId() +
                    ", open state with: highMark=" + highMark +
                    ", next=" + next +
                    ", underlying record=" + super.toString() + " ]";
        }
    }

    void release()
    {
        if ( labelCursor != null )
        {
            labelCursor.close();
            labelCursor = null;
        }

        if ( pageCursor != null )
        {
            pageCursor.close();
            pageCursor = null;
        }
    }
}

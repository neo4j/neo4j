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
package org.neo4j.kernel.impl.newapi;

import java.util.Set;

import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.txstate.TransactionState;

import static java.util.Collections.emptySet;

class DefaultRelationshipScanCursor extends RelationshipCursor implements RelationshipScanCursor
{
    private int type;
    private long next;
    private long highMark;
    private PageCursor pageCursor;
    private Set<Long> addedRelationships;

    DefaultRelationshipScanCursor( DefaultCursors pool )
    {
        super( pool );
    }

    void scan( int type, Read read )
    {
        if ( getId() != NO_ID )
        {
            reset();
        }
        if ( pageCursor == null )
        {
            pageCursor = read.relationshipPage( 0 );
        }
        next = 0;
        this.type = type;
        highMark = read.relationshipHighMark();
        init( read );
        this.addedRelationships = emptySet();
    }

    void single( long reference, Read read )
    {
        if ( getId() != NO_ID )
        {
            reset();
        }
        if ( pageCursor == null )
        {
            pageCursor = read.relationshipPage( reference );
        }
        next = reference >= 0 ? reference : NO_ID;
        type = -1;
        highMark = NO_ID;
        init( read );
        this.addedRelationships = emptySet();
    }

    @Override
    public boolean next()
    {
        if ( next == NO_ID )
        {
            reset();
            return false;
        }

        // Check tx state
        boolean hasChanges = hasChanges();
        TransactionState txs = hasChanges ? read.txState() : null;

        do
        {
            if ( hasChanges && containsRelationship( txs ) )
            {
                loadFromTxState( next++ );
                setInUse( true );
            }
            else if ( hasChanges && txs.relationshipIsDeletedInThisTx( next ) )
            {
                next++;
                setInUse( false );
            }
            else
            {
                read.relationship( this, next++, pageCursor );
            }

            if ( next > highMark )
            {
                if ( isSingle() )
                {
                    next = NO_ID;
                    return isWantedTypeAndInUse();
                }
                else
                {
                    highMark = read.relationshipHighMark();
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
        return (type == -1 || type() == type) && inUse();
    }

    private boolean containsRelationship( TransactionState txs )
    {
        return isSingle() ? txs.relationshipIsAddedInThisTx( next ) : addedRelationships.contains( next );
    }

    @Override
    public void close()
    {
        super.close();
        if ( !isClosed() )
        {
            read = null;
            reset();

            pool.accept( this );
        }
    }

    private void reset()
    {
        setId( next = NO_ID );
    }

    @Override
    public boolean isClosed()
    {
        return read == null;
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "RelationshipScanCursor[closed state]";
        }
        else
        {
            return "RelationshipScanCursor[id=" + getId() + ", open state with: highMark=" + highMark + ", next=" + next + ", type=" + type +
                    ", underlying record=" + super.toString() + " ]";
        }
    }

    private boolean isSingle()
    {
        return highMark == NO_ID;
    }

    protected void collectAddedTxStateSnapshot()
    {
        if ( !isSingle() )
        {
            addedRelationships = read.txState().addedAndRemovedRelationships().getAddedSnapshot();
        }
    }

    public void release()
    {
        if ( pageCursor != null )
        {
            pageCursor.close();
            pageCursor = null;
        }
    }
}

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

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.txstate.TransactionState;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptySet;


class DefaultRelationshipScanCursor extends RelationshipCursor implements RelationshipScanCursor
{
    private int label;
    private long next;
    private long highMark;
    private PageCursor pageCursor;
    PrimitiveLongSet addedRelationships;

    void scan( int label, Read read )
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
        this.label = label;
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
        next = reference;
        label = -1;
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
                    return isWantedLabelAndInUse();
                }
                else
                {
                    highMark = read.relationshipHighMark();
                    if ( next > highMark )
                    {
                        next = NO_ID;
                        return isWantedLabelAndInUse();
                    }
                }
            }
        }
        while ( !isWantedLabelAndInUse() );

        return true;
    }

    private boolean isWantedLabelAndInUse()
    {
        return (label == -1 || label() == label) && inUse();
    }

    private boolean containsRelationship( TransactionState txs )
    {
        return isSingle() ? txs.relationshipIsAddedInThisTx( next ) : addedRelationships.contains( next );
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
        if ( pageCursor != null )
        {
            pageCursor.close();
            pageCursor = null;
        }
        read = null;
        reset();
    }

    private void reset()
    {
        setId( next = NO_ID );
    }

    @Override
    public boolean isClosed()
    {
        return pageCursor == null;
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
            return "RelationshipScanCursor[id=" + getId() + ", open state with: highMark=" + highMark + ", next=" + next + ", label=" + label +
                    ", underlying record=" + super.toString() + " ]";
        }
    }

    private boolean isSingle()
    {
        return highMark == NO_ID;
    }

    @Override
    protected void collectAddedTxStateSnapshot()
    {
        if ( !isSingle() )
        {
            addedRelationships = read.txState().addedAndRemovedRelationships().getAddedSnapshot();
        }
    }
}

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

import org.neo4j.helpers.collection.Iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.LongPredicate;

import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class DefaultRelationshipScanCursor extends DefaultRelationshipCursor<StoreRelationshipScanCursor> implements RelationshipScanCursor
{
    private int type;
    private long single;
    private Iterator<Long> addedRelationships;

    private final DefaultCursors pool;

    DefaultRelationshipScanCursor( DefaultCursors pool )
    {
        super( new StoreRelationshipScanCursor() );
        this.pool = pool;
    }

    void scan( int type, Read read )
    {
        storeCursor.scan( type, read );
        this.type = type;
        this.single = NO_ID;
        init( read );
        this.addedRelationships = Collections.emptyIterator();
    }

    void single( long reference, Read read )
    {
        storeCursor.single( reference, read );
        type = -1;
        this.single = reference;
        init( read );
        this.addedRelationships = Collections.emptyIterator();
    }

    @Override
    public boolean next()
    {
        // Check tx state
        boolean hasChanges = hasChanges();
        LongPredicate isDeleted = hasChanges ? read.txState()::relationshipIsDeletedInThisTx : Predicates.alwaysFalseLong;

        if ( hasChanges && addedRelationships.hasNext() )
        {
            read.txState().relationshipVisit( addedRelationships.next(), storeCursor );
            return true;
        }

        return storeCursor.next( isDeleted );
    }

    @Override
    public void close()
    {
        if ( !isClosed() )
        {
            read = null;
            storeCursor.reset();

            pool.accept( this );
        }
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
            return "RelationshipScanCursor[id=" + storeCursor.relationshipReference() +
                    ", open state with: single=" + single +
                    ", type=" + type +
                    ", underlying record=" + super.toString() + " ]";
        }
    }

    protected void collectAddedTxStateSnapshot()
    {
        if ( isSingle() )
        {
            addedRelationships = read.txState().relationshipIsAddedInThisTx( single ) ?
                         Iterators.iterator( single ) : Collections.emptyIterator();
        }
        else
        {
            addedRelationships = read.txState().addedAndRemovedRelationships().getAddedSnapshot().iterator();
        }
    }

    private boolean isSingle()
    {
        return single != NO_ID;
    }

    public void release()
    {
        storeCursor.release();
    }
}

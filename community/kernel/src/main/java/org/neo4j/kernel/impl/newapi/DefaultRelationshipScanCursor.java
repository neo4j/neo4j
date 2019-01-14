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

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class DefaultRelationshipScanCursor extends DefaultRelationshipCursor<StorageRelationshipScanCursor> implements RelationshipScanCursor
{
    private int type;
    private long single;
    private LongIterator addedRelationships;

    DefaultRelationshipScanCursor( DefaultCursors pool, StorageRelationshipScanCursor storeCursor )
    {
        super( pool, storeCursor );
    }

    void scan( int type, Read read )
    {
        storeCursor.scan( type );
        this.type = type;
        this.single = NO_ID;
        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    void single( long reference, Read read )
    {
        storeCursor.single( reference );
        type = -1;
        this.single = reference;
        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    @Override
    public boolean next()
    {
        // Check tx state
        boolean hasChanges = hasChanges();

        if ( hasChanges && addedRelationships.hasNext() )
        {
            read.txState().relationshipVisit( addedRelationships.next(), storeCursor );
            return true;
        }

        while ( storeCursor.next() )
        {
            if ( !hasChanges || !read.txState().relationshipIsDeletedInThisTx( storeCursor.entityReference() ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close()
    {
        if ( !isClosed() )
        {
            read = null;
            storeCursor.close();

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
            return "RelationshipScanCursor[id=" + storeCursor.entityReference() +
                    ", open state with: single=" + single +
                    ", type=" + type +
                    ", " + storeCursor.toString() + "]";
        }
    }

    @Override
    protected void collectAddedTxStateSnapshot()
    {
        if ( isSingle() )
        {
            addedRelationships = read.txState().relationshipIsAddedInThisTx( single )
                                 ? LongHashSet.newSetWith( single ).longIterator()
                                 : ImmutableEmptyLongIterator.INSTANCE;
        }
        else
        {
            addedRelationships = read.txState().addedAndRemovedRelationships().getAdded().longIterator();
        }
    }

    private boolean isSingle()
    {
        return single != NO_ID;
    }

    public void release()
    {
        storeCursor.close();
    }
}

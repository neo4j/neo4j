/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.eclipse.collections.api.set.primitive.LongSet;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexProgressor.EntityTokenClient;

import static org.neo4j.internal.kernel.api.Read.NO_ID;

public class DefaultRelationshipTypeIndexCursor extends IndexCursor<IndexProgressor> implements RelationshipTypeIndexCursor
{
    private final CursorPool<DefaultRelationshipTypeIndexCursor> pool;
    private Read read;
    private long relationship;
    private int type;
    private LongIterator added;
    private LongSet removed;

    DefaultRelationshipTypeIndexCursor( CursorPool<DefaultRelationshipTypeIndexCursor> pool )
    {
        this.pool = pool;
        this.relationship = NO_ID;
    }

    @Override
    public void relationship( RelationshipScanCursor cursor )
    {
        read.singleRelationship( relationship, cursor );
    }

    @Override
    public void sourceNode( NodeCursor cursor )
    {
        throw new UnsupportedOperationException( "We have not yet implemented tracking of the relationship end nodes in the relationship type index cursor." );
    }

    @Override
    public void targetNode( NodeCursor cursor )
    {
        throw new UnsupportedOperationException( "We have not yet implemented tracking of the relationship end nodes in the relationship type index cursor." );
    }

    @Override
    public int type()
    {
        return type;
    }

    @Override
    public long sourceNodeReference()
    {
        throw new UnsupportedOperationException( "We have not yet implemented tracking of the relationship end nodes in the relationship type index cursor." );
    }

    @Override
    public long targetNodeReference()
    {
        throw new UnsupportedOperationException( "We have not yet implemented tracking of the relationship end nodes in the relationship type index cursor." );
    }

    @Override
    public long relationshipReference()
    {
        return relationship;
    }

    @Override
    public boolean next()
    {
        if ( added != null && added.hasNext() )
        {
            this.relationship = added.next();
            if ( tracer != null )
            {
                tracer.onRelationship( this.relationship );
            }
            return true;
        }
        else
        {
            boolean hasNext = innerNext();
            if ( tracer != null && hasNext )
            {
                tracer.onRelationship( this.relationship );
            }
            return hasNext;
        }
    }

    @Override
    public void closeInternal()
    {
        if ( !isClosed() )
        {
            closeProgressor();
            relationship = NO_ID;
            type = Math.toIntExact( NO_ID );
            read = null;
            added = null;
            removed = null;

            pool.accept( this );
        }
    }

    @Override
    public boolean isClosed()
    {
        return isProgressorClosed();
    }

    @Override
    public float score()
    {
        return Float.NaN;
    }

    public void setRead( Read read )
    {
        this.read = read;
    }

    EntityTokenClient relationshipTypeClient()
    {
        return ( reference, tokens ) ->
        {
            if ( isRemoved( reference ) )
            {
                return false;
            }
            else
            {
                DefaultRelationshipTypeIndexCursor.this.relationship = reference;
                return true;
            }
        };
    }

    public void scan( IndexProgressor progressor, int type )
    {
        super.initialize( progressor );
        if ( read.hasTxStateWithChanges() )
        {
            added = read.txState().relationshipsWithTypeChanged( type ).getAdded().freeze().longIterator();
            removed = read.txState().addedAndRemovedRelationships().getRemoved().freeze();
        }
        this.type = type;
        if ( tracer != null )
        {
            tracer.onRelationshipTypeScan( type );
        }
    }

    public void release()
    {
        // nothing to do
    }

    private boolean isRemoved( long reference )
    {
        return removed != null && removed.contains( reference );
    }
}

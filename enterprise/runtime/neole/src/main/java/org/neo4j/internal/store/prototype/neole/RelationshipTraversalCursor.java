/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.store.prototype.neole;

import static java.lang.String.format;

class RelationshipTraversalCursor extends RelationshipCursor
        implements org.neo4j.internal.kernel.api.RelationshipTraversalCursor
{
    private long originNodeReference;

    RelationshipTraversalCursor( ReadStore store )
    {
        super( store );
        this.originNodeReference = Long.MIN_VALUE;
    }

    void init( StoreFile relationships, long originNodeReference, long reference )
    {
        relationships.initializeCursor( reference, this );
        this.originNodeReference = ~originNodeReference;
    }

    @Override
    protected void closeImpl()
    {
        this.originNodeReference = Long.MIN_VALUE;
    }

    @Override
    public Position suspend()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void resume( Position position )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void neighbour( org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        store.singleNode( neighbourNodeReference(), cursor );
    }

    @Override
    public long originNodeReference()
    {
        return originNodeReference;
    }

    @Override
    public long neighbourNodeReference()
    {
        final long source = sourceNodeReference(), target = targetNodeReference();
        if ( source == originNodeReference )
        {
            return target;
        }
        if ( target == originNodeReference )
        {
            return source;
        }
        throw new IllegalStateException( format(
                "not part of this chain! source=0x%x, target=0x%x, origin=0x%x",
                source,
                target,
                originNodeReference ) );
    }

    @Override
    public boolean next()
    {
        long next;
        if ( originNodeReference < 0 )
        {
            if ( originNodeReference == Long.MIN_VALUE )
            {
                return false;
            }
            else
            {
                originNodeReference = ~originNodeReference;
                next = virtualAddress(); // same as current - we haven't been there yet.
            }
        }
        else
        {
            next = nextRelationshipReference();
        }
        if ( next == NO_RELATIONSHIP )
        {
            close();
            return false;
        }
        return moveToVirtualAddress( next );
    }

    private long nextRelationshipReference()
    {
        final long source = sourceNodeReference(), target = targetNodeReference();
        if ( source == originNodeReference )
        {
            return sourceNextRelationshipReference();
        }
        if ( target == originNodeReference )
        {
            return targetNextRelationshipReference();
        }
        throw new IllegalStateException( format(
                "%d is not part of this chain! source=0x%x, target=0x%x, origin=0x%x",
                relationshipReference(),
                source,
                target,
                originNodeReference ) );
    }

    boolean isOutgoing()
    {
        return originNodeReference == sourceNodeReference();
    }

    boolean isIncoming()
    {
        return originNodeReference == targetNodeReference();
    }

    boolean isLoop()
    {
        return sourceNodeReference() == targetNodeReference();
    }
}

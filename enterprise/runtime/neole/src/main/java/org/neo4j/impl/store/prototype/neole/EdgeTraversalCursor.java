/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.impl.store.prototype.neole;

class EdgeTraversalCursor extends EdgeCursor implements org.neo4j.impl.kernel.api.EdgeTraversalCursor
{
    private long originNodeReference;

    EdgeTraversalCursor( ReadStore store )
    {
        super( store );
        this.originNodeReference = Long.MIN_VALUE;
    }

    void init( StoreFile edges, long originNodeReference, long reference )
    {
        int pageId = edges.pageOf( reference );
        prepareReadCursor( reference -1, edges, pageId, edges.page( pageId ) );
        this.originNodeReference = ~originNodeReference;
    }

    void init( long originNodeReference )
    {
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
    public void neighbour( org.neo4j.impl.kernel.api.NodeCursor cursor )
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
        throw new IllegalStateException( String.format(
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
                next = virtualAddress() + 1; // same as current - we haven't been there yet.
            }
        }
        else
        {
            next = nextEdgeReference();
        }
        if ( next == NO_EDGE )
        {
            close();
            return false;
        }
        return gotoVirtualAddress( next );
    }

    private long nextEdgeReference()
    {
        final long source = sourceNodeReference(), target = targetNodeReference();
        if ( source == originNodeReference )
        {
            return sourceNextEdgeReference();
        }
        if ( target == originNodeReference )
        {
            return targetNextEdgeReference();
        }
        throw new IllegalStateException( String.format(
                "%d is not part of this chain! source=0x%x, target=0x%x, origin=0x%x",
                edgeReference(),
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

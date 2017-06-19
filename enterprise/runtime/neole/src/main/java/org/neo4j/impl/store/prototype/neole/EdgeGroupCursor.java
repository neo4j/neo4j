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

import static org.neo4j.impl.store.prototype.EdgeCursor.NO_EDGE;
import static org.neo4j.impl.store.prototype.neole.ReadStore.combineReference;

class EdgeGroupCursor extends org.neo4j.impl.store.prototype.EdgeGroupCursor<ReadStore>
{
    /**
     * <pre>
     *  0: in_use, high_1 (1 bytes)
     *  1: high_2         (1 bytes)
     *  2: type           (2 bytes)
     *  4: next           (4 bytes)
     *  8: out            (4 bytes)
     * 12: in             (4 bytes)
     * 16: loop           (4 bytes)
     * 20: node           (4 bytes)
     * 24: node_high      (1 bytes)
     * </pre>
     * <h2>high_1</h2>
     * <pre>
     * [    ,   x] in_use
     * [    ,xxx ] high(next)
     * [ xxx,    ] high(out)
     * </pre>
     * <h2>high_2</h2>
     * <pre>
     * [    ,xxx ] high(in)
     * [ xxx,    ] high(loop)
     * </pre>
     */
    static final int RECORD_SIZE = 25;
    /** used for accessing counts */
    private final EdgeTraversalCursor edge;
    private long originNodeReference;

    EdgeGroupCursor( ReadStore store, EdgeTraversalCursor edge )
    {
        super( store );
        this.edge = edge;
        this.originNodeReference = Long.MIN_VALUE;
    }

    void init( StoreFile groups, StoreFile edges, long originNodeReference, long reference )
    {
        if ( reference < 0 )
        {
            close();
            if ( reference != NO_EDGE )
            {
                edge.init( edges, originNodeReference, decodeDirectEdgeReference( reference ) );
            }
        }
        else
        {
            ReadStore.setup( groups, this, reference );
            this.originNodeReference = ~originNodeReference;
        }
    }

    private boolean nonDenseHack()
    {
        return !hasPageReference() && edge.hasPageReference();
    }

    static long encodeDirectEdgeReference( long reference )
    {
        return (~reference) - 1;
    }

    private static long decodeDirectEdgeReference( long reference )
    {
        return ~(reference + 1);
    }

    @Override
    protected int dataBound()
    {
        return RECORD_SIZE;
    }

    @Override
    protected void closeImpl()
    {
        edge.close();
        originNodeReference = Long.MIN_VALUE;
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
    public boolean next()
    {
        if ( nonDenseHack() )
        {
            return edge.next();
        }
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
            next = nextReference();
        }
        if ( next == NO_EDGE )
        {
            close();
            return false;
        }
        return gotoVirtualAddress( next );
    }

    @Override
    public int edgeLabel()
    {
        if ( nonDenseHack() )
        {
            return edge.label();
        }
        return unsignedShort( 2 );
    }

    @Override
    public int outgoingCount()
    {
        if ( nonDenseHack() )
        {
            return edge.isOutgoing() ? (int) edge.sourcePrevEdgeReference() : 0;
        }
        return count( outgoingReference(), true );
    }

    @Override
    public int incomingCount()
    {
        if ( nonDenseHack() )
        {
            return edge.isIncoming() ? (int) edge.targetPrevEdgeReference() : 0;
        }
        return count( incomingReference(), false );
    }

    @Override
    public int loopCount()
    {
        if ( nonDenseHack() )
        {
            return edge.isLoop() ? (int) edge.sourcePrevEdgeReference() : 0;
        }
        return count( loopsReference(), true );
    }

    private int count( long edgeReference, boolean source )
    {
        if ( NO_EDGE == edgeReference )
        {
            return 0;
        }
        try ( EdgeTraversalCursor edge = this.edge )
        {
            store.edges( nodeReference(), edgeReference, edge );
            if ( !edge.next() )
            {
                return 0;
            }
            return source ? (int) edge.sourcePrevEdgeReference() : (int) edge.targetPrevEdgeReference();
        }
    }

    private long nextReference()
    {
        return combineReference( unsignedInt( 4 ), ((long) (unsignedByte( 0 ) & 0x0E)) << 31 );
    }

    @Override
    public long outgoingReference()
    {
        if ( nonDenseHack() )
        {
            return edge.isOutgoing() ? edge.edgeReference() : NO_EDGE;
        }
        return combineReference( unsignedInt( 8 ), ((long) (unsignedByte( 0 ) & 0x70)) << 28 );
    }

    @Override
    public long incomingReference()
    {
        if ( nonDenseHack() )
        {
            return edge.isIncoming() ? edge.edgeReference() : NO_EDGE;
        }
        return combineReference( unsignedInt( 12 ), ((long) (unsignedByte( 1 ) & 0x0E)) << 31 );
    }

    @Override
    public long loopsReference()
    {
        if ( nonDenseHack() )
        {
            return edge.isLoop() ? edge.edgeReference() : NO_EDGE;
        }
        return combineReference( unsignedInt( 16 ), ((long) (unsignedByte( 1 ) & 0x70)) << 28 );
    }

    private long nodeReference()
    {
        return combineReference( unsignedInt( 20 ), ((long) unsignedByte( 24 )) << 32 );
    }

    @Override
    protected long originNodeReference()
    {
        if ( nonDenseHack() )
        {
            return edge.originNodeReference();
        }
        return originNodeReference;
    }
}

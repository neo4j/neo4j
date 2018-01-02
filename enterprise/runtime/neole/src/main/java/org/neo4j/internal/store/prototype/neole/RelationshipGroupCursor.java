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

import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.store.cursors.ReadCursor;

import static org.neo4j.internal.store.prototype.neole.ReadStore.combineReference;
import static org.neo4j.internal.store.prototype.neole.RelationshipCursor.NO_RELATIONSHIP;

class RelationshipGroupCursor extends ReadCursor implements org.neo4j.internal.kernel.api.RelationshipGroupCursor
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
    protected final ReadStore store;
    /** used for accessing counts */
    private final org.neo4j.internal.store.prototype.neole.RelationshipTraversalCursor relationship;
    private long originNodeReference;

    RelationshipGroupCursor( ReadStore store,
            org.neo4j.internal.store.prototype.neole.RelationshipTraversalCursor relationship )
    {
        this.store = store;
        this.relationship = relationship;
        this.originNodeReference = Long.MIN_VALUE;
    }

    void init( StoreFile groups, StoreFile relationships, long originNodeReference, long reference )
    {
        if ( reference < 0 )
        {
            close();
            if ( reference != NO_RELATIONSHIP )
            {
                relationship.init( relationships, originNodeReference, decodeDirectRelationshipReference( reference ) );
            }
        }
        else
        {
            groups.initializeCursor( reference, this );
            this.originNodeReference = ~originNodeReference;
        }
    }

    private boolean nonDenseHack()
    {
        return !hasPageReference() && relationship.hasPageReference();
    }

    static long encodeDirectRelationshipReference( long reference )
    {
        return (~reference) - 1;
    }

    private static long decodeDirectRelationshipReference( long reference )
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
        relationship.close();
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
            return relationship.next();
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
        if ( next == NO_RELATIONSHIP )
        {
            close();
            return false;
        }
        return moveToVirtualAddress( next );
    }

    @Override
    public int relationshipLabel()
    {
        if ( nonDenseHack() )
        {
            return relationship.label();
        }
        return unsignedShort( 2 );
    }

    @Override
    public int outgoingCount()
    {
        if ( nonDenseHack() )
        {
            return relationship.isOutgoing() ? (int) relationship.sourcePrevRelationshipReference() : 0;
        }
        return count( outgoingReference(), true );
    }

    @Override
    public int incomingCount()
    {
        if ( nonDenseHack() )
        {
            return relationship.isIncoming() ? (int) relationship.targetPrevRelationshipReference() : 0;
        }
        return count( incomingReference(), false );
    }

    @Override
    public int loopCount()
    {
        if ( nonDenseHack() )
        {
            return relationship.isLoop() ? (int) relationship.sourcePrevRelationshipReference() : 0;
        }
        return count( loopsReference(), true );
    }

    private int count( long relationshipReference, boolean source )
    {
        if ( NO_RELATIONSHIP == relationshipReference )
        {
            return 0;
        }
        try ( org.neo4j.internal.store.prototype.neole.RelationshipTraversalCursor relationship = this.relationship )
        {
            store.relationships( nodeReference(), relationshipReference, relationship );
            if ( !relationship.next() )
            {
                return 0;
            }
            return source ? (int) relationship.sourcePrevRelationshipReference()
                          : (int) relationship.targetPrevRelationshipReference();
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
            return relationship.isOutgoing() ? relationship.relationshipReference() : NO_RELATIONSHIP;
        }
        return combineReference( unsignedInt( 8 ), ((long) (unsignedByte( 0 ) & 0x70)) << 28 );
    }

    @Override
    public long incomingReference()
    {
        if ( nonDenseHack() )
        {
            return relationship.isIncoming() ? relationship.relationshipReference() : NO_RELATIONSHIP;
        }
        return combineReference( unsignedInt( 12 ), ((long) (unsignedByte( 1 ) & 0x0E)) << 31 );
    }

    @Override
    public long loopsReference()
    {
        if ( nonDenseHack() )
        {
            return relationship.isLoop() ? relationship.relationshipReference() : NO_RELATIONSHIP;
        }
        return combineReference( unsignedInt( 16 ), ((long) (unsignedByte( 1 ) & 0x70)) << 28 );
    }

    private long nodeReference()
    {
        return combineReference( unsignedInt( 20 ), ((long) unsignedByte( 24 )) << 32 );
    }

    private long originNodeReference()
    {
        if ( nonDenseHack() )
        {
            return relationship.originNodeReference();
        }
        return originNodeReference;
    }

    @Override
    public void outgoing( RelationshipTraversalCursor cursor )
    {
        store.relationships( originNodeReference(), outgoingReference(), cursor );
    }

    @Override
    public void incoming( RelationshipTraversalCursor cursor )
    {
        store.relationships( originNodeReference(), incomingReference(), cursor );
    }

    @Override
    public void loops( RelationshipTraversalCursor cursor )
    {
        store.relationships( originNodeReference(), loopsReference(), cursor );
    }
}

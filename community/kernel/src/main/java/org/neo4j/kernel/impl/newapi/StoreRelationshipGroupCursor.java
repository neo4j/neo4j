/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.newapi.StoreRelationshipTraversalCursor.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeForFiltering;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeForTxStateFiltering;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeNoIncomingRels;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeNoLoopRels;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeNoOutgoingRels;

class StoreRelationshipGroupCursor extends RelationshipGroupRecord
{
    private Read read;
    private final RelationshipRecord edge = new RelationshipRecord( NO_ID );

    private BufferedGroup bufferedGroup;
    private PageCursor page;
    private PageCursor edgePage;

    StoreRelationshipGroupCursor()
    {
        super( NO_ID );
    }

    void buffer( long nodeReference, long relationshipReference, Read read )
    {
        setOwningNode( nodeReference );
        setId( NO_ID );
        setNext( NO_ID );

        try ( PageCursor edgePage = read.relationshipPage( relationshipReference ) )
        {
            final MutableIntObjectMap<BufferedGroup> buffer = new IntObjectHashMap<>();
            BufferedGroup current = null;
            while ( relationshipReference != NO_ID )
            {
                read.relationshipFull( edge, relationshipReference, edgePage );
                // find the group
                BufferedGroup group = buffer.get( edge.getType() );
                if ( group == null )
                {
                    buffer.put( edge.getType(), current = group = new BufferedGroup( edge, current ) );
                }
                // buffer the relationship into the group
                if ( edge.getFirstNode() == nodeReference ) // outgoing or loop
                {
                    if ( edge.getSecondNode() == nodeReference ) // loop
                    {
                        group.loop( edge );
                    }
                    else // outgoing
                    {
                        group.outgoing( edge );
                    }
                    relationshipReference = edge.getFirstNextRel();
                }
                else if ( edge.getSecondNode() == nodeReference ) // incoming
                {
                    group.incoming( edge );
                    relationshipReference = edge.getSecondNextRel();
                }
                else
                {
                    throw new IllegalStateException( "not a part of the chain! TODO: better exception" );
                }
            }
            this.bufferedGroup = new BufferedGroup( edge, current ); // we need a dummy before the first to denote the initial pos
            this.read = read;
        }
    }

    void direct( long nodeReference, long reference, Read read )
    {
        bufferedGroup = null;
        clear();
        setOwningNode( nodeReference );
        setNext( reference );
        if ( page == null )
        {
            page = read.groupPage( reference );
        }
        this.read = read;
    }

    public boolean next()
    {
        if ( isBuffered() )
        {
            bufferedGroup = bufferedGroup.next;
            if ( bufferedGroup != null )
            {
                loadFromBuffer();
                return true;
            }
        }

        do
        {
            if ( getNext() == NO_ID )
            {
                //We have now run out of groups from the store, however there may still
                //be new types that was added in the transaction that we haven't visited yet.
                return false;
            }
            read.group( this, getNext(), page );
        } while ( !inUse() );

        return true;
    }

    public void setCurrent( int groupReference, int firstOut, int firstIn, int firstLoop )
    {
        setType( groupReference );
        setFirstOut( firstOut );
        setFirstIn( firstIn );
        setFirstLoop( firstLoop );
    }

    private void loadFromBuffer()
    {
        setType( bufferedGroup.label );
        setFirstOut( bufferedGroup.outgoing() );
        setFirstIn( bufferedGroup.incoming() );
        setFirstLoop( bufferedGroup.loops() );
    }

    public void close()
    {
        if ( !isClosed() )
        {
            bufferedGroup = null;
            read = null;
            setId( NO_ID );
            clear();
        }
    }

    public int type()
    {
        return getType();
    }

    public int outgoingCount()
    {
        return isBuffered() ? bufferedGroup.outgoingCount : count( outgoingRawId() );
    }

    public int incomingCount()
    {
        return isBuffered() ? bufferedGroup.incomingCount : count( incomingRawId() );
    }

    public int loopCount()
    {
        return isBuffered() ? bufferedGroup.loopsCount : count( loopsRawId() );
    }

    private int count( long reference )
    {
        if ( reference == NO_ID )
        {
            return 0;
        }
        if ( edgePage == null )
        {
            edgePage = read.relationshipPage( reference );
        }
        read.relationship( edge, reference, edgePage );
        if ( edge.getFirstNode() == getOwningNode() )
        {
            return (int) edge.getFirstPrevRel();
        }
        else
        {
            return (int) edge.getSecondPrevRel();
        }
    }

    public void outgoing( RelationshipTraversalCursor cursor )
    {
        if ( isBuffered() )
        {
            ((DefaultRelationshipTraversalCursor) cursor).buffered(
                    getOwningNode(), bufferedGroup.outgoing, RelationshipDirection.OUTGOING, bufferedGroup.label, read );
        }
        else
        {
            read.relationships( getOwningNode(), outgoingReference(), cursor );
        }
    }

    public void incoming( RelationshipTraversalCursor cursor )
    {
        if ( isBuffered() )
        {
            ((DefaultRelationshipTraversalCursor) cursor).buffered(
                    getOwningNode(), bufferedGroup.incoming, RelationshipDirection.INCOMING, bufferedGroup.label, read );
        }
        else
        {
            read.relationships( getOwningNode(), incomingReference(), cursor );
        }
    }

    public void loops( RelationshipTraversalCursor cursor )
    {
        if ( isBuffered() )
        {
            ((DefaultRelationshipTraversalCursor) cursor).buffered(
                    getOwningNode(), bufferedGroup.loops, RelationshipDirection.LOOP, bufferedGroup.label, read );
        }
        else
        {
            read.relationships( getOwningNode(), loopsReference(), cursor );
        }
    }

    public long outgoingReference()
    {
        long outgoing = getFirstOut();
        return outgoing == NO_ID ? encodeNoOutgoingRels( getType() ) : encodeRelationshipReference( outgoing );
    }

    public long incomingReference()
    {
        long incoming = getFirstIn();
        return incoming == NO_ID ? encodeNoIncomingRels( getType() ) : encodeRelationshipReference( incoming );
    }

    public long loopsReference()
    {
        long loops = getFirstLoop();
        return loops == NO_ID ? encodeNoLoopRels( getType() ) : encodeRelationshipReference( loops );
    }

    public boolean isClosed()
    {
        return read == null && bufferedGroup == null;
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "RelationshipGroupCursor[closed state]";
        }
        else
        {
            String mode = "mode=";
            if ( isBuffered() )
            {
                mode = mode + "group";
            }
            else
            {
                mode = mode + "direct";
            }
            return "RelationshipGroupCursor[id=" + getId() + ", open state with: " + mode + ", underlying record=" + super.toString() + " ]";
        }
    }

    /**
     * Implementation detail which provides the raw non-encoded outgoing relationship id
     */
    long outgoingRawId()
    {
        return getFirstOut();
    }

    /**
     * Implementation detail which provides the raw non-encoded incoming relationship id
     */
    long incomingRawId()
    {
        return getFirstIn();
    }

    /**
     * Implementation detail which provides the raw non-encoded loops relationship id
     */
    long loopsRawId()
    {
        return getFirstLoop();
    }

    private boolean isBuffered()
    {
        return bufferedGroup != null;
    }

    private long encodeRelationshipReference( long relationshipId )
    {
        assert relationshipId != NO_ID;
        return isBuffered() ? encodeForFiltering( relationshipId ) : encodeForTxStateFiltering( relationshipId );
    }

    public void release()
    {
        if ( edgePage != null )
        {
            edgePage.close();
            edgePage = null;
        }

        if ( page != null )
        {
            page.close();
            page = null;
        }
    }

    static class BufferedGroup
    {
        final int label;
        final BufferedGroup next;
        Record outgoing;
        Record incoming;
        Record loops;
        private long firstOut = NO_ID;
        private long firstIn = NO_ID;
        private long firstLoop = NO_ID;
        int outgoingCount;
        int incomingCount;
        int loopsCount;

        BufferedGroup( RelationshipRecord edge, BufferedGroup next )
        {
            this.label = edge.getType();
            this.next = next;
        }

        void outgoing( RelationshipRecord edge )
        {
            if ( outgoing == null )
            {
                firstOut = edge.getId();
            }
            outgoing = new Record( edge, outgoing );
            outgoingCount++;
        }

        void incoming( RelationshipRecord edge )
        {
            if ( incoming == null )
            {
                firstIn = edge.getId();
            }
            incoming = new Record( edge, incoming );
            incomingCount++;
        }

        void loop( RelationshipRecord edge )
        {
            if ( loops == null )
            {
                firstLoop = edge.getId();
            }
            loops = new Record( edge, loops );
            loopsCount++;
        }

        long outgoing()
        {
            return firstOut;
        }

        long incoming()
        {
            return firstIn;
        }

        long loops()
        {
            return firstLoop;
        }
    }
}

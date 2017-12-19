/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.newapi.RelationshipTraversalCursor.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.kernel.impl.newapi.References.setFilterFlag;

class RelationshipGroupCursor extends RelationshipGroupRecord implements org.neo4j.internal.kernel.api.RelationshipGroupCursor
{
    private Read read;
    private final RelationshipRecord edge = new RelationshipRecord( NO_ID );
    private BufferedGroup bufferedGroup;
    private PageCursor page;
    private PageCursor edgePage;

    RelationshipGroupCursor()
    {
        super( NO_ID );
    }

    void buffer( long nodeReference, long relationshipReference, Read read )
    {
        setOwningNode( nodeReference );
        setId( NO_ID );
        setNext( NO_ID );
        // TODO: read first record to get the required capacity (from the count value in the prev field)
        try ( PrimitiveIntObjectMap<BufferedGroup> buffer = Primitive.intObjectMap();
              PageCursor edgePage = read.relationshipPage( relationshipReference ) )
        {
            BufferedGroup current = null;
            while ( relationshipReference != NO_ID )
            {
                read.relationship( edge, relationshipReference, edgePage );
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
        setOwningNode( nodeReference );
        bufferedGroup = null;
        clear();
        setNext( reference );
        if ( page == null )
        {
            page = read.groupPage( reference );
        }
        this.read = read;
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
        if ( isBuffered() )
        {
            bufferedGroup = bufferedGroup.next;
            if ( bufferedGroup == null )
            {
                return false; // we never have both types of traversal, so terminate early
            }
            setType( bufferedGroup.label );
            setFirstOut( bufferedGroup.outgoing() );
            setFirstIn( bufferedGroup.incoming() );
            setFirstLoop( bufferedGroup.loops() );
            return true;
        }
        if ( getNext() == NO_ID )
        {
            return false;
        }
        read.group( this, getNext(), page );
        return true;
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
        if ( page != null )
        {
            page.close();
            page = null;
        }

        if ( edgePage != null )
        {
            edgePage.close();
            edgePage = null;
        }
        bufferedGroup = null;
        read = null;
        setId( NO_ID );
        clear();
    }

    @Override
    public int relationshipLabel()
    {
        return getType();
    }

    @Override
    public int outgoingCount()
    {
        if ( isBuffered() )
        {
            return bufferedGroup.outgoingCount;
        }
        return count( outgoingReference() );
    }

    @Override
    public int incomingCount()
    {
        if ( isBuffered() )
        {
            return bufferedGroup.incomingCount;
        }
        return count( incomingReference() );
    }

    @Override
    public int loopCount()
    {
        if ( isBuffered() )
        {
            return bufferedGroup.loopsCount;
        }
        return count( loopsReference() );
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

    @Override
    public void outgoing( org.neo4j.internal.kernel.api.RelationshipTraversalCursor cursor )
    {
        if ( isBuffered() )
        {
            ((RelationshipTraversalCursor) cursor).buffered( getOwningNode(), bufferedGroup.outgoing, read );
        }
        else
        {
            read.relationships( getOwningNode(), outgoingReference(), cursor );
        }
    }

    @Override
    public void incoming( org.neo4j.internal.kernel.api.RelationshipTraversalCursor cursor )
    {
        if ( isBuffered() )
        {
            ((RelationshipTraversalCursor) cursor).buffered( getOwningNode(), bufferedGroup.incoming, read );
        }
        else
        {
            read.relationships( getOwningNode(), incomingReference(), cursor );
        }
    }

    @Override
    public void loops( org.neo4j.internal.kernel.api.RelationshipTraversalCursor cursor )
    {
        if ( isBuffered() )
        {
            ((RelationshipTraversalCursor) cursor).buffered( getOwningNode(), bufferedGroup.loops, read );
        }
        else
        {
            read.relationships( getOwningNode(), loopsReference(), cursor );
        }
    }

    @Override
    public long outgoingReference()
    {
        return getFirstOut();
    }

    @Override
    public long incomingReference()
    {
        return getFirstIn();
    }

    @Override
    public long loopsReference()
    {
        return getFirstLoop();
    }

    @Override
    public boolean isClosed()
    {
        return page == null && bufferedGroup == null;
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

    private boolean isBuffered()
    {
        return bufferedGroup != null;
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
            outgoing = new RelationshipTraversalCursor.Record( edge, outgoing );
            outgoingCount++;
        }

        void incoming( RelationshipRecord edge )
        {
            if ( incoming == null )
            {
                firstIn = edge.getId();
            }
            incoming = new RelationshipTraversalCursor.Record( edge, incoming );
            incomingCount++;
        }

        void loop( RelationshipRecord edge )
        {
            if ( loops == null )
            {
                firstLoop = edge.getId();
            }
            loops = new RelationshipTraversalCursor.Record( edge, loops );
            loopsCount++;
        }

        long outgoing()
        {
            return setFilterFlag( firstOut );
        }

        long incoming()
        {
            return setFilterFlag( firstIn );
        }

        long loops()
        {
            return setFilterFlag( firstLoop );
        }
    }
}

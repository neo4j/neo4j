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

import static org.neo4j.kernel.impl.newapi.Read.addFilteringFlag;

class RelationshipGroupCursor extends RelationshipGroupRecord
        implements org.neo4j.internal.kernel.api.RelationshipGroupCursor
{
    private final Read read;
    private final RelationshipRecord edge = new RelationshipRecord( NO_ID );
    private Group current;
    private PageCursor page;
    private PageCursor edgePage;

    RelationshipGroupCursor( Read read )
    {
        super( -1 );
        this.read = read;
    }

    void buffer( long nodeReference, long relationshipReference )
    {
        setOwningNode( nodeReference );
        setId( NO_ID );
        setNext( NO_ID );
        // TODO: read first record to get the required capacity (from the count value in the prev field)
        try ( PrimitiveIntObjectMap<Group> buffer = Primitive.intObjectMap();
              PageCursor edgePage = read.relationshipPage( relationshipReference ) )
        {
            Group current = null;
            while ( relationshipReference != NO_ID )
            {
                read.relationship( edge, relationshipReference, edgePage );
                // find the group
                Group group = buffer.get( edge.getType() );
                if ( group == null )
                {
                    buffer.put( edge.getType(), current = group = new Group( edge, current ) );
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
            this.current = new Group( edge, current ); // we need a dummy before the first to denote the initial pos
        }
    }

    void direct( long nodeReference, long reference )
    {
        setOwningNode( nodeReference );
        current = null;
        clear();
        setNext( reference );
        if ( page == null )
        {
            page = read.groupPage( reference );
        }
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
        if ( current != null )
        {
            current = current.next;
            if ( current == null )
            {
                return false; // we never have both types of traversal, so terminate early
            }
            setType( current.label );
            setFirstOut( current.outgoing() );
            setFirstIn( current.incoming() );
            setFirstLoop( current.loops() );
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
        current = null;
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
        if ( current != null )
        {
            return current.outgoingCount;
        }
        return count( outgoingReference() );
    }

    @Override
    public int incomingCount()
    {
        if ( current != null )
        {
            return current.incomingCount;
        }
        return count( incomingReference() );
    }

    @Override
    public int loopCount()
    {
        if ( current != null )
        {
            return current.loopsCount;
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
        if ( current != null && cursor instanceof RelationshipTraversalCursor )
        {
            ((RelationshipTraversalCursor) cursor).buffered( getOwningNode(), current.outgoing );
        }
        else
        {
            read.relationships( getOwningNode(), outgoingReference(), cursor );
        }
    }

    @Override
    public void incoming( org.neo4j.internal.kernel.api.RelationshipTraversalCursor cursor )
    {
        if ( current != null && cursor instanceof RelationshipTraversalCursor )
        {
            ((RelationshipTraversalCursor) cursor).buffered( getOwningNode(), current.incoming );
        }
        else
        {
            read.relationships( getOwningNode(), incomingReference(), cursor );
        }
    }

    @Override
    public void loops( org.neo4j.internal.kernel.api.RelationshipTraversalCursor cursor )
    {
        if ( current != null && cursor instanceof RelationshipTraversalCursor )
        {
            ((RelationshipTraversalCursor) cursor).buffered( getOwningNode(), current.loops );
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

    static class Group
    {
        final int label;
        final Group next;
        Record outgoing;
        Record incoming;
        Record loops;
        private long firstOut = NO_ID;
        private long firstIn = NO_ID;
        private long firstLoop = NO_ID;
        int outgoingCount;
        int incomingCount;
        int loopsCount;

        Group( RelationshipRecord edge, Group next )
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
            return addFilteringFlag( firstOut );
        }

        long incoming()
        {
            return addFilteringFlag( firstIn );
        }

        long loops()
        {
            return addFilteringFlag( firstLoop );
        }

    }
}

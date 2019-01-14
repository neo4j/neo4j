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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.newapi.DefaultRelationshipTraversalCursor.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.RelationshipState;

import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeForFiltering;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeForTxStateFiltering;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeNoIncomingRels;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeNoLoopRels;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeNoOutgoingRels;

class DefaultRelationshipGroupCursor extends RelationshipGroupRecord implements RelationshipGroupCursor
{
    private Read read;
    private final RelationshipRecord edge = new RelationshipRecord( NO_ID );
    private final DefaultCursors pool;

    private BufferedGroup bufferedGroup;
    private PageCursor page;
    private PageCursor edgePage;
    private boolean hasCheckedTxState;
    private final PrimitiveIntSet txTypes = Primitive.intSet();
    private PrimitiveIntIterator txTypeIterator;

    DefaultRelationshipGroupCursor( DefaultCursors pool )
    {
        super( NO_ID );
        this.pool = pool;
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
            this.txTypes.clear();
            this.txTypeIterator = null;
            this.hasCheckedTxState = false;
            this.bufferedGroup = new BufferedGroup( edge, current ); // we need a dummy before the first to denote the initial pos
            this.read = read;
        }
    }

    void direct( long nodeReference, long reference, Read read )
    {
        bufferedGroup = null;
        clear();
        txTypes.clear();
        txTypeIterator = null;
        hasCheckedTxState = false;
        setOwningNode( nodeReference );
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
        //We need to check tx state if there are new types added
        //on the first call of next
        if ( !hasCheckedTxState )
        {
            checkTxStateForUpdates();
            hasCheckedTxState = true;
        }

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
                return nextFromTxState();
            }
            read.group( this, getNext(), page );
        } while ( !inUse() );

        markTypeAsSeen( type() );
        return true;
    }

    private void loadFromBuffer()
    {
        markTypeAsSeen( bufferedGroup.label );
        setType( bufferedGroup.label );
        setFirstOut( bufferedGroup.outgoing() );
        setFirstIn( bufferedGroup.incoming() );
        setFirstLoop( bufferedGroup.loops() );
    }

    private boolean nextFromTxState()
    {
        if ( txTypeIterator == null && !txTypes.isEmpty() )
        {
            txTypeIterator = txTypes.iterator();
            //here it may be tempting to do txTypes.clear()
            //however that will also clear the iterator
        }
        if ( txTypeIterator != null && txTypeIterator.hasNext() )
        {
            setType( txTypeIterator.next() );
            setFirstOut( NO_ID );
            setFirstIn( NO_ID );
            setFirstLoop( NO_ID );
            return true;
        }
        return false;
    }

    /**
     * Marks the given type as already seen
     * @param type the type we have seen
     */
    private void markTypeAsSeen( int type )
    {
        if ( txTypes != null )
        {
            txTypes.remove( type );
        }
    }

    /**
     * Store all types that was added in the transaction for the current node
     */
    private void checkTxStateForUpdates()
    {
        if ( read.hasTxStateWithChanges() )
        {
            NodeState nodeState = read.txState().getNodeState( getOwningNode() );
            PrimitiveLongIterator addedRelationships = nodeState.getAddedRelationships();
            while ( addedRelationships.hasNext() )
            {
                RelationshipState relationshipState = read.txState().getRelationshipState( addedRelationships.next() );
                relationshipState.accept(
                        (RelationshipVisitor<RuntimeException>) ( relationshipId, typeId, startNodeId, endNodeId ) ->
                                txTypes.add( typeId ) );
            }
        }
    }

    @Override
    public void close()
    {
        if ( !isClosed() )
        {
            bufferedGroup = null;
            read = null;
            setId( NO_ID );
            clear();

            if ( pool != null )
            {
                pool.accept( this );
            }
        }
    }

    @Override
    public int type()
    {
        return getType();
    }

    @Override
    public int outgoingCount()
    {
        int count;
        if ( isBuffered() )
        {
            count = bufferedGroup.outgoingCount;
        }
        else
        {
            count = count( outgoingRawId() );
        }
        return read.hasTxStateWithChanges()
               ? read.txState().getNodeState( getOwningNode() )
                       .augmentDegree( RelationshipDirection.OUTGOING, count, getType() ) : count;
    }

    @Override
    public int incomingCount()
    {
        int count;

        if ( isBuffered() )
        {
            count = bufferedGroup.incomingCount;
        }
        else
        {
            count = count( incomingRawId() );
        }
        return read.hasTxStateWithChanges()
               ? read.txState().getNodeState( getOwningNode() )
                       .augmentDegree( RelationshipDirection.INCOMING, count, getType() ) : count;
    }

    @Override
    public int loopCount()
    {
        int count;
        if ( isBuffered() )
        {
            count = bufferedGroup.loopsCount;
        }
        else
        {
            count = count( loopsRawId() );
        }

        return read.hasTxStateWithChanges()
               ? read.txState().getNodeState( getOwningNode() )
                       .augmentDegree( RelationshipDirection.LOOP, count, getType() ) : count;

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

    @Override
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

    @Override
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

    @Override
    public long outgoingReference()
    {
        long outgoing = getFirstOut();
        return outgoing == NO_ID ? encodeNoOutgoingRels( getType() ) : encodeRelationshipReference( outgoing );
    }

    @Override
    public long incomingReference()
    {
        long incoming = getFirstIn();
        return incoming == NO_ID ? encodeNoIncomingRels( getType() ) : encodeRelationshipReference( incoming );
    }

    @Override
    public long loopsReference()
    {
        long loops = getFirstLoop();
        return loops == NO_ID ? encodeNoLoopRels( getType() ) : encodeRelationshipReference( loops );
    }

    @Override
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
            outgoing = new DefaultRelationshipTraversalCursor.Record( edge, outgoing );
            outgoingCount++;
        }

        void incoming( RelationshipRecord edge )
        {
            if ( incoming == null )
            {
                firstIn = edge.getId();
            }
            incoming = new DefaultRelationshipTraversalCursor.Record( edge, incoming );
            incomingCount++;
        }

        void loop( RelationshipRecord edge )
        {
            if ( loops == null )
            {
                firstLoop = edge.getId();
            }
            loops = new DefaultRelationshipTraversalCursor.Record( edge, loops );
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

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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordRelationshipTraversalCursor.Record;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.StorageRelationshipGroupCursor;

import static org.neo4j.kernel.impl.newapi.References.clearEncoding;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeForFiltering;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeForTxStateFiltering;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.GroupReferenceEncoding.isRelationship;

class RecordRelationshipGroupCursor extends RelationshipGroupRecord implements StorageRelationshipGroupCursor
{
    private final RelationshipStore relationshipStore;
    private final RelationshipGroupStore groupStore;
    private final RelationshipRecord edge = new RelationshipRecord( NO_ID );

    private BufferedGroup bufferedGroup;
    private PageCursor page;
    private PageCursor edgePage;
    private boolean open;

    RecordRelationshipGroupCursor( RelationshipStore relationshipStore, RelationshipGroupStore groupStore )
    {
        super( NO_ID );
        this.relationshipStore = relationshipStore;
        this.groupStore = groupStore;
    }

    @Override
    public void init( long nodeReference, long reference )
    {
        // the relationships for this node are not grouped in the store
        if ( reference != NO_ID && isRelationship( reference ) )
        {
            buffer( nodeReference, clearEncoding( reference ) );
        }
        else // this is a normal group reference.
        {
            direct( nodeReference, reference );
        }
        open = true;
    }

    /**
     * Sparse node, i.e. fake groups by reading the whole chain and buffering it.
     */
    private void buffer( long nodeReference, long relationshipReference )
    {
        setOwningNode( nodeReference );
        setId( NO_ID );
        setNext( NO_ID );

        try ( PageCursor edgePage = relationshipStore.openPageCursorForReading( relationshipReference ) )
        {
            final MutableIntObjectMap<BufferedGroup> buffer = new IntObjectHashMap<>();
            BufferedGroup current = null;
            while ( relationshipReference != NO_ID )
            {
                relationshipStore.getRecordByCursor( relationshipReference, edge, RecordLoad.FORCE, edgePage );
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
        }
    }

    /**
     * Dense node, real groups iterated with every call to next.
     */
    void direct( long nodeReference, long reference )
    {
        bufferedGroup = null;
        clear();
        setOwningNode( nodeReference );
        setNext( reference );
        if ( page == null )
        {
            page = groupPage( reference );
        }
    }

    @Override
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
            group( this, getNext(), page );
        } while ( !inUse() );

        return true;
    }

    @Override
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

    @Override
    public void reset()
    {
        if ( open )
        {
            open = false;
            bufferedGroup = null;
            setId( NO_ID );
            clear();
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
        return isBuffered() ? bufferedGroup.outgoingCount : count( outgoingRawId() );
    }

    @Override
    public int incomingCount()
    {
        return isBuffered() ? bufferedGroup.incomingCount : count( incomingRawId() );
    }

    @Override
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
            edgePage = relationshipStore.openPageCursorForReading( reference );
        }
        relationshipStore.getRecordByCursor( reference, edge, RecordLoad.FORCE, edgePage );
        if ( edge.getFirstNode() == getOwningNode() )
        {
            return (int) edge.getFirstPrevRel();
        }
        else
        {
            return (int) edge.getSecondPrevRel();
        }
    }

    /**
     * If the returned reference points to a chain of relationships that aren't physically filtered by direction and type then
     * a flag in this reference can be set so that external filtering will be performed as the cursor progresses.
     * See {@link RelationshipReferenceEncoding#encodeForFiltering(long)}.
     */
    @Override
    public long outgoingReference()
    {
        long outgoing = getFirstOut();
        return outgoing == NO_ID ? NO_ID : encodeRelationshipReference( outgoing );
    }

    /**
     * If the returned reference points to a chain of relationships that aren't physically filtered by direction and type then
     * a flag in this reference can be set so that external filtering will be performed as the cursor progresses.
     * See {@link RelationshipReferenceEncoding#encodeForFiltering(long)}.
     */
    @Override
    public long incomingReference()
    {
        long incoming = getFirstIn();
        return incoming == NO_ID ? NO_ID : encodeRelationshipReference( incoming );
    }

    /**
     * If the returned reference points to a chain of relationships that aren't physically filtered by direction and type then
     * a flag in this reference can be set so that external filtering will be performed as the cursor progresses.
     * See {@link RelationshipReferenceEncoding#encodeForFiltering(long)}.
     */
    @Override
    public long loopsReference()
    {
        long loops = getFirstLoop();
        return loops == NO_ID ? NO_ID : encodeRelationshipReference( loops );
    }

    @Override
    public String toString()
    {
        if ( !open )
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
            return "RelationshipGroupCursor[id=" + getId() + ", open state with: " + mode + ", underlying record=" + super.toString() + "]";
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

    private long encodeRelationshipReference( long relationshipId )
    {
        assert relationshipId != NO_ID;
        return isBuffered() ? encodeForFiltering( relationshipId ) : encodeForTxStateFiltering( relationshipId );
    }

    private boolean isBuffered()
    {
        return bufferedGroup != null;
    }

    @Override
    public void close()
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

    @Override
    public long groupReference()
    {
        return getId();
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

    private PageCursor groupPage( long reference )
    {
        return groupStore.openPageCursorForReading( reference );
    }

    private void group( RelationshipGroupRecord record, long reference, PageCursor page )
    {
        // We need to load forcefully here since otherwise we cannot traverse over groups
        // records which have been concurrently deleted (flagged as inUse = false).
        // @see #org.neo4j.kernel.impl.store.RelationshipChainPointerChasingTest
        groupStore.getRecordByCursor( reference, record, RecordLoad.FORCE, page );
    }
}

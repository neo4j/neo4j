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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

import static org.neo4j.kernel.impl.newapi.References.clearEncoding;

class RecordRelationshipTraversalCursor extends RecordRelationshipCursor implements StorageRelationshipTraversalCursor
{
    private enum GroupState
    {
        INCOMING,
        OUTGOING,
        LOOP,
        NONE
    }

    private long originNodeReference;
    private long next;
    private Record buffer;
    private PageCursor pageCursor;
    private final RecordRelationshipGroupCursor group;
    private GroupState groupState;
    private boolean open;

    RecordRelationshipTraversalCursor( RelationshipStore relationshipStore, RelationshipGroupStore groupStore )
    {
        super( relationshipStore );
        this.group = new RecordRelationshipGroupCursor( relationshipStore, groupStore );
    }

    @Override
    public void init( long nodeReference, long reference )
    {
        /* There are basically two ways a relationship traversal cursor can be initialized:
         *
         * 1. From a dense node, where multiple relationship chains are discovered from relationship groups
         *    as the internal group cursor sees them.
         * 2. From a sparse node, where a single relationship chain is traversed.
         */

        RelationshipReferenceEncoding encoding = RelationshipReferenceEncoding.parseEncoding( reference );

        switch ( encoding )
        {
        case NONE: // this is a normal relationship reference
            chain( nodeReference, reference );
            break;

        case GROUP: // this reference is actually to a group record
            groups( nodeReference, clearEncoding( reference ) );
            break;

        default:
            throw new IllegalStateException( "Unknown encoding " + encoding );
        }

        open = true;
    }

    /*
     * Normal traversal. Traversal returns mixed types and directions.
     */
    private void chain( long nodeReference, long reference )
    {
        if ( pageCursor == null )
        {
            pageCursor = relationshipPage( reference );
        }
        setId( NO_ID );
        this.groupState = GroupState.NONE;
        this.originNodeReference = nodeReference;
        this.next = reference;
    }

    /*
     * Reference to a group record. Traversal returns mixed types and directions.
     */
    private void groups( long nodeReference, long groupReference )
    {
        setId( NO_ID );
        this.next = NO_ID;
        this.groupState = GroupState.INCOMING;
        this.originNodeReference = nodeReference;
        group.direct( nodeReference, groupReference );
    }

    @Override
    public long neighbourNodeReference()
    {
        final long source = sourceNodeReference(), target = targetNodeReference();
        if ( source == originNodeReference )
        {
            return target;
        }
        else if ( target == originNodeReference )
        {
            return source;
        }
        else
        {
            throw new IllegalStateException( "NOT PART OF CHAIN" );
        }
    }

    @Override
    public long originNodeReference()
    {
        return originNodeReference;
    }

    @Override
    public boolean next()
    {
        if ( hasBufferedData() )
        {   // We have buffered data, iterate the chain of buffered records
            return nextBuffered();
        }

        do
        {
            if ( traversingDenseNode() )
            {
                traverseDenseNode();
            }

            if ( next == NO_ID )
            {
                resetState();
                return false;
            }

            relationshipFull( this, next, pageCursor );
            computeNext();
        } while ( !inUse() );

        return true;
    }

    private boolean nextBuffered()
    {
        buffer = buffer.next;
        if ( !hasBufferedData() )
        {
            resetState();
            return false;
        }
        else
        {
            // Copy buffer data to self
            copyFromBuffer();
        }

        return true;
    }

    private void traverseDenseNode()
    {
        while ( next == NO_ID )
        {
             /*
              Dense nodes looks something like:

                    Node(dense=true)

                            |
                            v

                        Group(:HOLDS)   -incoming-> Rel(id=2) -> Rel(id=3)
                                        -outgoing-> Rel(id=5) -> Rel(id=10) -> Rel(id=3)
                                        -loop->     Rel(id=9)
                            |
                            v

                        Group(:USES)    -incoming-> Rel(id=14)
                                        -outgoing-> Rel(id=55) -> Rel(id=51) -> ...
                                        -loop->     Rel(id=21) -> Rel(id=11)

                            |
                            v
                            ...

              We iterate over dense nodes using a small state machine staring in state INCOMING.
              1) fetch next group, if no more group stop.
              2) set next to group.incomingReference, switch state to OUTGOING
              3) Iterate relationship chain until we reach the end
              4) set next to group.outgoingReference and state to LOOP
              5) Iterate relationship chain until we reach the end
              6) set next to group.loop and state back to INCOMING
              7) Iterate relationship chain until we reach the end
              8) GOTO 1
             */
            switch ( groupState )
            {
            case INCOMING:
                boolean hasNext = group.next();
                if ( !hasNext )
                {
                    assert next == NO_ID;
                    return; // no more groups nor relationships
                }
                next = group.incomingRawId();
                if ( pageCursor == null )
                {
                    pageCursor = relationshipPage( Math.max( next, 0L ) );
                }
                groupState = GroupState.OUTGOING;
                break;

            case OUTGOING:
                next = group.outgoingRawId();
                groupState = GroupState.LOOP;
                break;

            case LOOP:
                next = group.loopsRawId();
                groupState = GroupState.INCOMING;
                break;

            default:
                throw new IllegalStateException( "We cannot get here, but checkstyle forces this!" );
            }
        }
    }

    private void computeNext()
    {
        final long source = sourceNodeReference(), target = targetNodeReference();
        if ( source == originNodeReference )
        {
            next = getFirstNextRel();
        }
        else if ( target == originNodeReference )
        {
            next = getSecondNextRel();
        }
        else
        {
            throw new IllegalStateException( "NOT PART OF CHAIN! " + this );
        }
    }

    private void copyFromBuffer()
    {
        this.setId( buffer.id );
        this.setType( buffer.type );
        this.setNextProp( buffer.nextProp );
        this.setFirstNode( buffer.firstNode );
        this.setSecondNode( buffer.secondNode );
    }

    private boolean traversingDenseNode()
    {
        return groupState != GroupState.NONE;
    }

    @Override
    public void reset()
    {
        if ( open )
        {
            open = false;
            buffer = null;
            resetState();
        }
    }

    private void resetState()
    {
        setId( next = NO_ID );
        groupState = GroupState.NONE;
        buffer = null;
    }

    @Override
    public void close()
    {
        if ( pageCursor != null )
        {
            pageCursor.close();
            pageCursor = null;
        }

        group.close();
    }

    @Override
    public String toString()
    {
        if ( !open )
        {
            return "RelationshipTraversalCursor[closed state]";
        }
        else
        {
            String dense = "denseNode=" + traversingDenseNode();
            String mode = "mode=";

            if ( hasBufferedData() )
            {
                mode = mode + "bufferedData";
            }
            else
            {
                mode = mode + "regular";
            }
            return "RelationshipTraversalCursor[id=" + getId() +
                    ", open state with: " + dense +
                    ", next=" + next + ", " + mode +
                    ", underlying record=" + super.toString() + "]";
        }
    }

    private boolean hasBufferedData()
    {
        return buffer != null;
    }

    /*
     * Record is both a data holder for buffering data from a RelationshipRecord
     * as well as a linked list over the records in the group.
     */
    static class Record
    {
        final long id;
        final int type;
        final long nextProp;
        final long firstNode;
        final long secondNode;
        final Record next;

        /*
         * Initialize the record chain or push a new record as the new head of the record chain
         */
        Record( RelationshipRecord record, Record next )
        {
            if ( record != null )
            {
                id = record.getId();
                type = record.getType();
                nextProp = record.getNextProp();
                firstNode = record.getFirstNode();
                secondNode = record.getSecondNode();
            }
            else
            {
                id = NO_ID;
                type = NO_ID;
                nextProp = NO_ID;
                firstNode = NO_ID;
                secondNode = NO_ID;
            }
            this.next = next;
        }
    }
}

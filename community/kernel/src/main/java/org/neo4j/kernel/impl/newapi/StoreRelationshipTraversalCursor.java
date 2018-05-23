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

import java.util.function.LongPredicate;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

class StoreRelationshipTraversalCursor extends StoreRelationshipCursor
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
    private final StoreRelationshipGroupCursor group;
    private GroupState groupState;

    StoreRelationshipTraversalCursor()
    {
        this.group = new StoreRelationshipGroupCursor();
    }

    /*
     * Cursor being called as a group, use the buffered records in Record
     * instead. These are guaranteed to all have the same type and direction.
     */
    void buffered( long nodeReference, Record record, Read read )
    {
        this.originNodeReference = nodeReference;
        this.buffer = Record.initialize( record );
        this.groupState = GroupState.NONE;
        init( read );
    }

    /*
     * Normal traversal. Traversal returns mixed types and directions.
     */
    void chain( long nodeReference, long reference, Read read )
    {
        if ( pageCursor == null )
        {
            pageCursor = read.relationshipPage( reference );
        }
        setId( NO_ID );
        this.groupState = GroupState.NONE;
        this.originNodeReference = nodeReference;
        this.next = reference;
        init( read );
    }

    /*
     * Reference to a group record. Traversal returns mixed types and directions.
     */
    void groups( long nodeReference, long groupReference, Read read )
    {
        setId( NO_ID );
        this.next = NO_ID;
        this.groupState = GroupState.INCOMING;
        this.originNodeReference = nodeReference;
        group.direct( nodeReference, groupReference, read );
        init( read );
    }

    /*
     * Grouped traversal of non-dense node. Same type and direction as first read relationship. Store relationships are
     * all assumed to be of wanted relationship type and direction iff filterStore == false.
     */
    void filtered( long nodeReference, long reference, Read read )
    {
        if ( pageCursor == null )
        {
            pageCursor = read.relationshipPage( reference );
        }
        setId( NO_ID );
        this.groupState = GroupState.NONE;
        this.originNodeReference = nodeReference;
        this.next = reference;
        init( read );
    }

    /*
     * Empty chain in store. Return from tx-state with provided relationship type and direction.
     */
    void filteredTxState( long nodeReference, Read read )
    {
        setId( NO_ID );
        this.groupState = GroupState.NONE;
        this.originNodeReference = nodeReference;
        this.next = NO_ID;
        init( read );
    }

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

    public long originNodeReference()
    {
        return originNodeReference;
    }

    public boolean next( LongPredicate filter )
    {
        if ( hasBufferedData() )
        {   // We have buffered data, iterate the chain of buffered records
            return nextBuffered( filter );
        }

        do
        {
            if ( traversingDenseNode() )
            {
                traverseDenseNode();
            }

            if ( next == NO_ID )
            {
                reset();
                return false;
            }

            read.relationshipFull( this, next, pageCursor );
            computeNext();

        } while ( filter.test( getId() ) );

        return true;
    }

    private boolean nextBuffered( LongPredicate isDeleted )
    {
        do
        {
            buffer = buffer.next;
            if ( !hasBufferedData() )
            {
                reset();
                return false;
            }
            else
            {
                // Copy buffer data to self
                copyFromBuffer();
            }
        } while ( isDeleted.test( getId() ) );

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
                    pageCursor = read.relationshipPage( Math.max( next, 0L ) );
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

    public void close()
    {
        if ( !isClosed() )
        {
            read = null;
            buffer = null;
            reset();
        }
    }

    private void reset()
    {
        setId( next = NO_ID );
        groupState = GroupState.NONE;
        buffer = null;
    }

    public boolean isClosed()
    {
        return read == null && !hasBufferedData();
    }

    public void release()
    {
        if ( pageCursor != null )
        {
            pageCursor.close();
            pageCursor = null;
        }

        group.release();
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
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
                    ", underlying record=" + super.toString() + " ]";
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
        private static final RelationshipRecord DUMMY = null;
        final long id;
        final int type;
        final long nextProp;
        final long firstNode;
        final long secondNode;
        final Record next;

        /*
         * Initialize the chain of records
         */
        static Record initialize( Record first )
        {
            return new Record( DUMMY, first );
        }

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

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

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;


class RelationshipTraversalCursor extends RelationshipCursor
        implements org.neo4j.internal.kernel.api.RelationshipTraversalCursor
{
    private enum GroupState
    {
        INCOMING,
        OUTGOING,
        LOOP,
        NONE
    }

    private enum FilterState
    {
        NOT_INITIALIZED
                {
                    @Override
                    boolean check( long source, long target, long origin )
                    {
                        throw new IllegalStateException( "Cannot call check on uninitialized filter" );
                    }
                },
        INCOMING
                {
                    @Override
                    boolean check( long source, long target, long origin )
                    {
                        return origin == target;
                    }
                },
        OUTGOING
                {
                    @Override
                    boolean check( long source, long target, long origin )
                    {
                        return origin == source;
                    }
                },
        LOOP
                {
                    @Override
                    boolean check( long source, long target, long origin )
                    {
                        return source == target;
                    }
                },
        NONE
                {
                    @Override
                    boolean check( long source, long target, long origin )
                    {
                        return true;
                    }
                };

        abstract boolean check( long source, long target, long origin );
    }

    private long originNodeReference;
    private long next;
    private Record buffer;
    private PageCursor pageCursor;
    private final RelationshipGroupCursor group;
    private GroupState groupState;
    private FilterState filterState;
    private int filterType = NO_ID;

    RelationshipTraversalCursor( RelationshipGroupCursor group )
    {
        this.group = group;
    }

    /*
     * Cursor being called as a group, use the buffered records in Record
     * instead.
     */
    void buffered( long nodeReference, Record record, Read read )
    {
        this.originNodeReference = nodeReference;
        this.buffer = Record.initialize( record );
        this.groupState = GroupState.NONE;
        this.filterState = FilterState.NONE;
        this.filterType = NO_ID;
        this.read = read;
    }

    /*
     * Normal traversal.
     */
    void chain( long nodeReference, long reference, Read read )
    {
        if ( pageCursor == null )
        {
            pageCursor = read.relationshipPage( reference );
        }
        setId( NO_ID );
        groupState = GroupState.NONE;
        filterState = FilterState.NONE;
        filterType = NO_ID;
        originNodeReference = nodeReference;
        next = reference;
        this.read = read;
    }

    /*
     * Reference to a group record
     */
    void groups( long nodeReference, long groupReference, Read read )
    {
        setId( NO_ID );
        next = NO_ID;
        groupState = GroupState.INCOMING;
        filterState = FilterState.NONE;
        filterType = NO_ID;
        originNodeReference = nodeReference;
        read.relationshipGroups( nodeReference, groupReference, group );
        this.read = read;
    }

    void filtered( long nodeReference, long reference, Read read )
    {
        if ( pageCursor == null )
        {
            pageCursor = read.relationshipPage( reference );
        }
        setId( NO_ID );
        groupState = GroupState.NONE;
        filterState = FilterState.NOT_INITIALIZED;
        originNodeReference = nodeReference;
        next = reference;
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
    public void neighbour( NodeCursor cursor )
    {
        read.singleNode( neighbourNodeReference(), cursor );
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
        if ( traversingDenseNode() )
        {
            traverseDenseNode();
        }

        if ( hasBufferedData() )
        {   //We have buffered data, iterate the chain of buffered records
            return nextBufferedData();
        }

        if ( filteringTraversal() )
        {
            return nextFilteredData();
        }

        if ( next == NO_ID )
        {
            reset();
            return false;
        }

        //Not a group, nothing buffered, no filtering.
        //Just a plain old traversal.
        read.relationship( this, next, pageCursor );
        computeNext();
        return true;
    }

    private boolean nextFilteredData()
    {
        if ( next == NO_ID )
        {
            reset();
            return false;
        }
        if ( filterState == FilterState.NOT_INITIALIZED )
        {
            //Initialize filtering:
            //  - Read first record
            //  - Check type and direction
            //  - Subsequent records need to have same type and direction
            read.relationship( this, next, pageCursor );
            filterType = getType();
            final long source = sourceNodeReference(), target = targetNodeReference();
            if ( source == target )
            {
                next = getFirstNextRel();
                filterState = FilterState.LOOP;
            }
            else if ( source == originNodeReference )
            {
                next = getFirstNextRel();
                filterState = FilterState.OUTGOING;
            }
            else if ( target == originNodeReference )
            {
                next = getSecondNextRel();
                filterState = FilterState.INCOMING;
            }
            return true;
        }
        else
        {
            //Iterate until we stop on a valid record,
            //i.e. one with the same type and direction.
            while ( true )
            {
                read.relationship( this, next, pageCursor );
                computeNext();
                if ( predicate() )
                {
                    return true;
                }
                if ( next == NO_ID )
                {
                    reset();
                    return false;
                }

            }
        }
    }

    private boolean nextBufferedData()
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
            return true;
        }
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
                    reset();
                    return;
                }
                next = group.incomingReference();
                if ( pageCursor == null )
                {
                    pageCursor = read.relationshipPage( Math.max( next, 0L ) );
                }
                groupState = GroupState.OUTGOING;
                break;

            case OUTGOING:
                next = group.outgoingReference();
                groupState = GroupState.LOOP;
                break;

            case LOOP:
                next = group.loopsReference();
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
            throw new IllegalStateException( "NOT PART OF CHAIN" );
        }
    }

    private boolean predicate()
    {
        return filterType == getType() &&
               filterState.check( sourceNodeReference(), targetNodeReference(), originNodeReference );
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

    private boolean filteringTraversal()
    {
        return filterState != FilterState.NONE;
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
        if ( pageCursor != null )
        {
            pageCursor.close();
            pageCursor = null;
        }
        read = null;
        reset();
    }

    private void reset()
    {
        setId( next = NO_ID );
        groupState = GroupState.NONE;
        filterState = FilterState.NONE;
        filterType = NO_ID;
        buffer = null;
    }

    @Override
    public boolean isClosed()
    {
        return pageCursor == null && !hasBufferedData();
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
            else if ( filteringTraversal() )
            {
                mode = mode + "filteringTraversal";
            }
            else
            {
                mode = mode + "regular";
            }
            return "RelationshipTraversalCursor[id=" + getId() + ", open state with: " + dense + ", next=" + next + ", " + mode + ", underlying record=" +
                    super.toString() + " ]";
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

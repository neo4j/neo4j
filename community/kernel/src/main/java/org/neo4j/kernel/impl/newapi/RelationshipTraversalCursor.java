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

import static org.neo4j.kernel.impl.newapi.RelationshipTraversalCursor.GroupState.INCOMING;
import static org.neo4j.kernel.impl.newapi.RelationshipTraversalCursor.GroupState.LOOP;
import static org.neo4j.kernel.impl.newapi.RelationshipTraversalCursor.GroupState.NONE;
import static org.neo4j.kernel.impl.newapi.RelationshipTraversalCursor.GroupState.OUTGOING;

class RelationshipTraversalCursor extends RelationshipCursor
        implements org.neo4j.internal.kernel.api.RelationshipTraversalCursor
{
    enum GroupState { INCOMING, OUTGOING, LOOP, NONE };

    private long originNodeReference;
    private long next;
    private Record buffer;
    private PageCursor pageCursor;
    private final RelationshipGroupCursor group;
    private GroupState groupState;


    RelationshipTraversalCursor( Read read )
    {
        super( read );
        this.group = new RelationshipGroupCursor( read );
    }

    /*
     * Cursor being called as a group, use the buffered records in Record
     * instead.
     */
    void buffered( long nodeReference, Record record )
    {
        this.originNodeReference = nodeReference;
        this.buffer = Record.initialize( record );
    }

    /*
     * Normal traversal.
     */
    void chain( long nodeReference, long reference )
    {
        if ( pageCursor == null )
        {
            pageCursor = read.relationshipPage( reference );
        }
        setId( NO_ID );
        groupState = NONE;
        originNodeReference = nodeReference;
        next = reference;
    }

    /*
     * Reference to a group record
     */
    void groups( long nodeReference, long groupReference )
    {
        setId( NO_ID );
        next = NO_ID;
        groupState = INCOMING;
        originNodeReference = nodeReference;
        read.relationshipGroups( nodeReference, groupReference, group );
    }

    void filtered( long nodeReference, long reference )
    {
        // TODO: read the first record and use the type of it for filtering the chain
        // - only include records with that type
        throw new UnsupportedOperationException( "not implemented" );
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
        /*
            Node(dense=true)

                |
                v

            Group(:HOLDS) -incoming-> Rel(id=2) -> Rel(id=3)
                          -outgoing-> Rel(id=5) -> Rel(id=10) -> Rel(id=3)
                          -loop->     Rel(id=9)
                |
                v

            Group(:USES)  -incoming-> Rel(id=14)
                          -outgoing-> Rel(id=55) -> Rel(id=51) -> ...
                          -loop->     Rel(id=21) -> Rel(id=11)

                |
                v
                ...

         */
        if ( traversingDenseNode() )
        {
            while ( next == NO_ID )
            {
                /*
                  Defines a small state machine, we start in state INCOMING.
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
                        return false;
                    }
                    next = group.incomingReference();
                    if ( pageCursor == null )
                    {
                        pageCursor = read.relationshipPage( Math.max( next, 0L ) );
                    }
                    groupState = OUTGOING;
                    break;

                case OUTGOING:
                    next = group.outgoingReference();
                    groupState = LOOP;
                    break;

                case LOOP:
                    next = group.loopsReference();
                    groupState = INCOMING;
                    break;

                default:
                    throw new IllegalStateException( "We cannot get here, but checkstyle forces this!" );
                }
            }
        }

        if ( hasBufferedData() )
        {   //We have buffered data, iterate the chain of buffered records
            buffer = buffer.next;
            if ( !hasBufferedData() )
            {
                reset();
                return false;
            }
            else
            {
                // Copy buffer data to self
                this.setId( buffer.id );
                this.setType( buffer.type );
                this.setNextProp( buffer.nextProp );
                this.setFirstNode( buffer.firstNode );
                this.setSecondNode( buffer.secondNode );
                return true;
            }
        }


        if ( next == NO_ID )
        {
            reset();
            return false;
        }
        read.relationship( this, next, pageCursor );
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
        return true;
    }

    private boolean traversingDenseNode()
    {
        return groupState != NONE;
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
        reset();
    }

    private void reset()
    {
        setId( next = NO_ID );
        groupState = NONE;
        buffer = null;
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
        static Record initialize(Record first)
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

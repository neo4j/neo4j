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

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.txstate.NodeState;

import static java.lang.String.format;

class DefaultRelationshipTraversalCursor extends RelationshipCursor
        implements RelationshipTraversalCursor
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
        // need filter, and need to read filter state from first store relationship
        NOT_INITIALIZED( RelationshipDirection.ERROR )
                {
                    @Override
                    boolean check( long source, long target, long origin )
                    {
                        throw new IllegalStateException( "Cannot call check on uninitialized filter" );
                    }
                },
        // allow only incoming relationships
        INCOMING( RelationshipDirection.INCOMING )
                {
                    @Override
                    boolean check( long source, long target, long origin )
                    {
                        return origin == target && source != target;
                    }
                },
        // allow only outgoing relationships
        OUTGOING( RelationshipDirection.OUTGOING )
                {
                    @Override
                    boolean check( long source, long target, long origin )
                    {
                        return origin == source && source != target;
                    }
                },
        // allow only loop relationships
        LOOP( RelationshipDirection.LOOP )
                {
                    @Override
                    boolean check( long source, long target, long origin )
                    {
                        return source == target;
                    }
                },
        // no filtering required
        NONE( RelationshipDirection.ERROR )
                {
                    @Override
                    boolean check( long source, long target, long origin )
                    {
                        return true;
                    }
                };

        abstract boolean check( long source, long target, long origin );

        private final RelationshipDirection direction;

        FilterState( RelationshipDirection direction )
        {
            this.direction = direction;
        }

        private static FilterState fromRelationshipDirection( RelationshipDirection direction )
        {
            switch ( direction )
            {
            case OUTGOING:
                return FilterState.OUTGOING;
            case INCOMING:
                return FilterState.INCOMING;
            case LOOP:
                return FilterState.LOOP;
            case ERROR:
                throw new IllegalArgumentException( "There has been a RelationshipDirection.ERROR" );
            default:
                throw new IllegalStateException(
                        format( "Still poking my eye, dear checkstyle... (cannot filter on direction '%s')", direction ) );
            }
        }
    }

    private long originNodeReference;
    private long next;
    private Record buffer;
    private PageCursor pageCursor;
    private final DefaultRelationshipGroupCursor group;
    private GroupState groupState;
    private FilterState filterState;
    private boolean filterStore;
    private int filterType = NO_ID;

    private PrimitiveLongIterator addedRelationships;

    DefaultRelationshipTraversalCursor( DefaultRelationshipGroupCursor group, DefaultCursors pool )
    {
        super( pool );
        this.group = group;
    }

    /*
     * Cursor being called as a group, use the buffered records in Record
     * instead. These are guaranteed to all have the same type and direction.
     */
    void buffered( long nodeReference, Record record, RelationshipDirection direction, int type, Read read )
    {
        this.originNodeReference = nodeReference;
        this.buffer = Record.initialize( record );
        this.groupState = GroupState.NONE;
        this.filterState = FilterState.fromRelationshipDirection( direction );
        this.filterType = type;
        init( read );
        this.addedRelationships = PrimitiveLongCollections.emptyIterator();
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
        this.filterState = FilterState.NONE;
        this.filterType = NO_ID;
        this.originNodeReference = nodeReference;
        this.next = reference;
        init( read );
        this.addedRelationships = PrimitiveLongCollections.emptyIterator();
    }

    /*
     * Reference to a group record. Traversal returns mixed types and directions.
     */
    void groups( long nodeReference, long groupReference, Read read )
    {
        setId( NO_ID );
        this.next = NO_ID;
        this.groupState = GroupState.INCOMING;
        this.filterState = FilterState.NONE;
        this.filterType = NO_ID;
        this.originNodeReference = nodeReference;
        read.relationshipGroups( nodeReference, groupReference, group );
        init( read );
        this.addedRelationships = PrimitiveLongCollections.emptyIterator();
    }

    /*
     * Grouped traversal of non-dense node. Same type and direction as first read relationship. Store relationships are
     * all assumed to be of wanted relationship type and direction iff filterStore == false.
     */
    void filtered( long nodeReference, long reference, Read read, boolean filterStore )
    {
        if ( pageCursor == null )
        {
            pageCursor = read.relationshipPage( reference );
        }
        setId( NO_ID );
        this.groupState = GroupState.NONE;
        this.filterState = FilterState.NOT_INITIALIZED;
        this.filterStore = filterStore;
        this.originNodeReference = nodeReference;
        this.next = reference;
        init( read );
        this.addedRelationships = PrimitiveLongCollections.emptyIterator();
    }

    /*
     * Empty chain in store. Return from tx-state with provided relationship type and direction.
     */
    void filteredTxState( long nodeReference, Read read, int filterType, RelationshipDirection direction )
    {
        setId( NO_ID );
        this.groupState = GroupState.NONE;
        this.filterType = filterType;
        this.filterState = FilterState.fromRelationshipDirection( direction );
        this.filterStore = false;
        this.originNodeReference = nodeReference;
        this.next = NO_ID;
        init( read );
        this.addedRelationships = PrimitiveLongCollections.emptyIterator();
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
        boolean hasChanges;
        TransactionState txs;

        if ( filterState == FilterState.NOT_INITIALIZED )
        {
            // Initialize filtering:
            //  - Read first record
            //  - Remember type and direction
            //  - Return first relationship if it's not deleted
            // Subsequent relationships need to have same type and direction

            hasChanges = hasChanges(); // <- will setup filter state if needed
            txs = hasChanges ? read.txState() : null;

            if ( filterState == FilterState.NOT_INITIALIZED && filterStore )
            {
                read.relationshipFull( this, next, pageCursor );
                setupFilterState();
            }

            if ( filterState != FilterState.NOT_INITIALIZED && (!hasChanges || !txs.relationshipIsDeletedInThisTx( getId() ) ) )
            {
                return true;
            }
        }
        else
        {
            hasChanges = hasChanges();
            txs = hasChanges ? read.txState() : null;
        }

        // tx-state relationships
        if ( hasChanges && addedRelationships.hasNext() )
        {
            loadFromTxState( addedRelationships.next() );
            return true;
        }

        if ( hasBufferedData() )
        {   // We have buffered data, iterate the chain of buffered records
            return nextBuffered( txs );
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

        } while ( ( filterStore && !correctTypeAndDirection() ) ||
                  ( hasChanges && txs.relationshipIsDeletedInThisTx( getId() ) ) );

        return true;
    }

    private void setupFilterState()
    {
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
    }

    private boolean nextBuffered( TransactionState txs )
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
        } while ( txs != null && txs.relationshipIsDeletedInThisTx( getId() ) );

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

    private boolean correctTypeAndDirection()
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

    @Override
    public void close()
    {
        super.close();
        if ( !isClosed() )
        {
            read = null;
            buffer = null;
            reset();

            pool.accept( this );
        }
    }

    private void reset()
    {
        setId( next = NO_ID );
        groupState = GroupState.NONE;
        filterState = FilterState.NONE;
        filterType = NO_ID;
        filterStore = false;
        buffer = null;
    }

    @Override
    protected void collectAddedTxStateSnapshot()
    {
        if ( filterState == FilterState.NOT_INITIALIZED )
        {
            read.relationshipFull( this, next, pageCursor );
            setupFilterState();
        }

        NodeState nodeState = read.txState().getNodeState( originNodeReference );
        addedRelationships = hasTxStateFilter() ?
                             nodeState.getAddedRelationships( filterState.direction, filterType ) :
                             nodeState.getAddedRelationships();
    }

    private boolean hasTxStateFilter()
    {
        return filterState != FilterState.NONE;
    }

    @Override
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
            else if ( filterStore )
            {
                mode = mode + "filterStore";
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

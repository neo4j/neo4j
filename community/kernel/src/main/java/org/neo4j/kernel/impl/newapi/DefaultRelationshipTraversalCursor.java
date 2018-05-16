/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;

import java.util.function.LongPredicate;

import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.storageengine.api.txstate.NodeState;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class DefaultRelationshipTraversalCursor extends DefaultRelationshipCursor<StoreRelationshipTraversalCursor>
        implements RelationshipTraversalCursor
{
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
                        return origin == target;
                    }
                },
        // allow only outgoing relationships
        OUTGOING( RelationshipDirection.OUTGOING )
                {
                    @Override
                    boolean check( long source, long target, long origin )
                    {
                        return origin == source;
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

    private final DefaultCursors pool;
    private FilterState filterState;
    private boolean filterStore;
    private int filterType = NO_ID;

    private LongIterator addedRelationships;

    DefaultRelationshipTraversalCursor( DefaultCursors pool )
    {
        super( new StoreRelationshipTraversalCursor() );
        this.pool = pool;
    }

    /*
     * Cursor being called as a group, use the buffered records in Record
     * instead. These are guaranteed to all have the same type and direction.
     */
    void buffered( long nodeReference, StoreRelationshipTraversalCursor.Record record, RelationshipDirection direction, int type, Read read )
    {
        storeCursor.buffered( nodeReference, record, read );
        this.filterState = FilterState.fromRelationshipDirection( direction );
        this.filterType = type;
        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    /*
     * Normal traversal. Traversal returns mixed types and directions.
     */
    void chain( long nodeReference, long reference, Read read )
    {
        storeCursor.chain( nodeReference, reference, read );
        this.filterState = FilterState.NONE;
        this.filterType = NO_ID;
        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    /*
     * Reference to a group record. Traversal returns mixed types and directions.
     */
    void groups( long nodeReference, long groupReference, Read read )
    {
        storeCursor.groups( nodeReference, groupReference, read );
        this.filterState = FilterState.NONE;
        this.filterType = NO_ID;
        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    /*
     * Grouped traversal of non-dense node. Same type and direction as first read relationship. Store relationships are
     * all assumed to be of wanted relationship type and direction iff filterStore == false.
     */
    void filtered( long nodeReference, long reference, Read read, boolean filterStore )
    {
        storeCursor.filtered( nodeReference, reference, read );
        this.filterState = FilterState.NOT_INITIALIZED;
        this.filterStore = filterStore;
        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    /*
     * Empty chain in store. Return from tx-state with provided relationship type and direction.
     */
    void filteredTxState( long nodeReference, Read read, int filterType, RelationshipDirection direction )
    {
        storeCursor.filteredTxState( nodeReference, read );
        this.filterType = filterType;
        this.filterState = FilterState.fromRelationshipDirection( direction );
        this.filterStore = false;
        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
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
        return storeCursor.neighbourNodeReference();
    }

    @Override
    public long originNodeReference()
    {
        return storeCursor.originNodeReference();
    }

    @Override
    public boolean next()
    {
        boolean hasChanges;
        LongPredicate isDeleted;

        if ( filterState == FilterState.NOT_INITIALIZED )
        {
            hasChanges = hasChanges(); // <- will setup filter state if needed
            isDeleted = hasChanges ? read.txState()::relationshipIsDeletedInThisTx : Predicates.alwaysFalseLong;

            if ( filterState == FilterState.NOT_INITIALIZED && filterStore )
            {
                storeCursor.next( Predicates.alwaysFalseLong );
                setupFilterState();
            }

            if ( filterState != FilterState.NOT_INITIALIZED && !isDeleted.test( relationshipReference() ) )
            {
                return true;
            }
        }
        else
        {
            hasChanges = hasChanges();
            isDeleted = hasChanges ? read.txState()::relationshipIsDeletedInThisTx : Predicates.alwaysFalseLong;
        }

        // tx-state relationships
        if ( hasChanges && addedRelationships.hasNext() )
        {
            read.txState().relationshipVisit( addedRelationships.next(), storeCursor );
            return true;
        }

        return storeCursor.next( ref -> ( filterStore && !correctTypeAndDirection() ) || isDeleted.test( ref ) );
    }

    private void setupFilterState()
    {
        filterType = storeCursor.type();
        final long source = sourceNodeReference(), target = targetNodeReference();
        if ( source == target )
        {
            filterState = FilterState.LOOP;
        }
        else if ( source == storeCursor.originNodeReference() )
        {
            filterState = FilterState.OUTGOING;
        }
        else if ( target == storeCursor.originNodeReference() )
        {
            filterState = FilterState.INCOMING;
        }
    }

    private boolean correctTypeAndDirection()
    {
        return filterType == storeCursor.type() &&
                filterState.check( sourceNodeReference(), targetNodeReference(), storeCursor.originNodeReference() );
    }

    @Override
    public void close()
    {
        if ( !isClosed() )
        {
            read = null;
            filterState = FilterState.NONE;
            filterType = NO_ID;
            filterStore = false;
            storeCursor.close();

            pool.accept( this );
        }
    }

    @Override
    protected void collectAddedTxStateSnapshot()
    {
        if ( filterState == FilterState.NOT_INITIALIZED )
        {
            storeCursor.next( Predicates.alwaysFalseLong );
            setupFilterState();
        }

        NodeState nodeState = read.txState().getNodeState( storeCursor.originNodeReference() );
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
        return storeCursor.isClosed();
    }

    public void release()
    {
        storeCursor.release();
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
            String dense = "denseNode=?";
            String mode = "mode=";

            if ( filterStore )
            {
                mode = mode + "filterStore";
            }
            else
            {
                mode = mode + "regular";
            }
            return "RelationshipTraversalCursor[id=" + storeCursor.relationshipReference() +
                    ", open state with: " + dense +
                    ", " + mode +
                    ", underlying record=" + super.toString() + " ]";
        }
    }
}

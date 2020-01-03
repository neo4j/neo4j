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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.api.txstate.NodeState;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.Read.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.kernel.impl.newapi.References.clearEncoding;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;

class DefaultRelationshipTraversalCursor extends DefaultRelationshipCursor<StorageRelationshipTraversalCursor>
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

    private FilterState filterState;
    private boolean filterStore;
    private int filterType = NO_ID;
    private LongIterator addedRelationships;

    DefaultRelationshipTraversalCursor( DefaultCursors pool, StorageRelationshipTraversalCursor storeCursor )
    {
        super( pool, storeCursor );
    }

    void init( long nodeReference, long reference, Read read )
    {
        /* There are 5 different ways a relationship traversal cursor can be initialized:
         *
         * 1. From a batched group in a detached way. This happens when the user manually retrieves the relationships
         *    references from the group cursor and passes it to this method and if the group cursor was based on having
         *    batched all the different types in the single (mixed) chain of relationships.
         *    In this case we should pass a reference marked with some flag to the first relationship in the chain that
         *    has the type of the current group in the group cursor. The traversal cursor then needs to read the type
         *    from that first record and use that type as a filter for when reading the rest of the chain.
         *    - NOTE: we probably have to do the same sort of filtering for direction - so we need a flag for that too.
         *
         * 2. From a batched group in a DIRECT way. This happens when the traversal cursor is initialized directly from
         *    the group cursor, in this case we can simply initialize the traversal cursor with the buffered state from
         *    the group cursor, so this method here does not have to be involved, and things become pretty simple.
         *
         * 3. Traversing all relationships - regardless of type - of a node that has grouped relationships. In this case
         *    the traversal cursor needs to traverse through the group records in order to get to the actual
         *    relationships. The initialization of the cursor (through this here method) should be with a FLAGGED
         *    reference to the (first) group record.
         *
         * 4. Traversing a single chain - this is what happens in the cases when
         *    a) Traversing all relationships of a node without grouped relationships.
         *    b) Traversing the relationships of a particular group of a node with grouped relationships.
         *
         * 5. There are no relationships - i.e. passing in NO_ID to this method.
         *
         * This means that we need reference encodings (flags) for cases: 1, 3, 4, 5
         */

        RelationshipReferenceEncoding encoding = RelationshipReferenceEncoding.parseEncoding( reference );

        switch ( encoding )
        {
        case NONE:
        case GROUP:
            storeCursor.init( nodeReference, reference );
            initFiltering( FilterState.NONE, false );
            break;

        case FILTER_TX_STATE:
            // The relationships in tx-state needs to be filtered according to the first relationship we discover,
            // but let's not have the store cursor bother with this detail.
            storeCursor.init( nodeReference, clearEncoding( reference ) );
            initFiltering( FilterState.NOT_INITIALIZED, false );
            break;

        case FILTER:
            // The relationships needs to be filtered according to the first relationship we discover
            storeCursor.init( nodeReference, clearEncoding( reference ) );
            initFiltering( FilterState.NOT_INITIALIZED, true );
            break;

        case NO_OUTGOING_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            storeCursor.init( nodeReference, NO_ID );
            initFiltering( FilterState.fromRelationshipDirection( OUTGOING ), false );
            this.filterType = (int) clearEncoding( reference );
            break;

        case NO_INCOMING_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            storeCursor.init( nodeReference, NO_ID );
            initFiltering( FilterState.fromRelationshipDirection( INCOMING ), false );
            this.filterType = (int) clearEncoding( reference );
            break;

        case NO_LOOP_OF_TYPE: // nothing in store, but proceed to check tx-state changes
            storeCursor.init( nodeReference, NO_ID );
            initFiltering( FilterState.fromRelationshipDirection( LOOP ), false );
            this.filterType = (int) clearEncoding( reference );
            break;

        default:
            throw new IllegalStateException( "Unknown encoding " + encoding );
        }

        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    private void initFiltering( FilterState filterState, boolean filterStore )
    {
        this.filterState = filterState;
        this.filterStore = filterStore;
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

        if ( filterState == FilterState.NOT_INITIALIZED )
        {
            hasChanges = hasChanges(); // <- will setup filter state if needed

            if ( filterState == FilterState.NOT_INITIALIZED && filterStore )
            {
                storeCursor.next();
                setupFilterState();
            }

            if ( filterState != FilterState.NOT_INITIALIZED && !(hasChanges && read.txState().relationshipIsDeletedInThisTx( relationshipReference() )) )
            {
                return true;
            }
        }
        else
        {
            hasChanges = hasChanges();
        }

        // tx-state relationships
        if ( hasChanges && addedRelationships.hasNext() )
        {
            read.txState().relationshipVisit( addedRelationships.next(), storeCursor );
            return true;
        }

        while ( storeCursor.next() )
        {
            boolean skip = (filterStore && !correctTypeAndDirection()) ||
                    (hasChanges && read.txState().relationshipIsDeletedInThisTx( storeCursor.entityReference() ) );
            if ( !skip )
            {
                return true;
            }
        }
        return false;
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
        return (filterType == ANY_RELATIONSHIP_TYPE || filterType == storeCursor.type()) &&
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
            storeCursor.next();
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
        return read == null;
    }

    public void release()
    {
        storeCursor.close();
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
            String mode = "mode=";
            if ( filterStore )
            {
                mode = mode + "filterStore";
            }
            else
            {
                mode = mode + "regular";
            }
            return "RelationshipTraversalCursor[id=" + storeCursor.entityReference() +
                    ", " + mode +
                    ", " + storeCursor.toString() + "]";
        }
    }
}

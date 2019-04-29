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

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;

import java.util.Arrays;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.api.txstate.NodeState;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.kernel.impl.newapi.Read.NO_ID;

class DefaultRelationshipTraversalCursor extends DefaultRelationshipCursor<StorageRelationshipTraversalCursor>
        implements RelationshipTraversalCursor
{
    private final CursorPool<DefaultRelationshipTraversalCursor> pool;
    private LongIterator addedRelationships;
    private int type = ANY_RELATIONSHIP_TYPE;
    private RelationshipDirection direction;
    private boolean lazySelection;
    private boolean filterInitialized;

    DefaultRelationshipTraversalCursor( CursorPool<DefaultRelationshipTraversalCursor> pool, StorageRelationshipTraversalCursor storeCursor )
    {
        super( storeCursor );
        this.pool = pool;
    }

    /**
     * Initializes this cursor to traverse over all relationships.
     * @param nodeReference reference to the origin node.
     * @param reference reference to the place to start traversing these relationships.
     * @param nodeIsDense whether or not the origin node is dense.
     * @param read reference to {@link Read}.
     */
    void init( long nodeReference, long reference, boolean nodeIsDense, Read read )
    {
        // For lazily initialized filtering the type/direction will be null, which is what the storage cursor should get in this scenario
        this.storeCursor.init( nodeReference, reference, nodeIsDense );
        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
        this.filterInitialized = true;
    }

    /**
     * Initializes this cursor to traverse over relationships of a specific type and direction.
     * In cases where the {@code reference} have been exposed to the client and comes back via
     * {@link org.neo4j.internal.kernel.api.Read#relationships(long, long, RelationshipTraversalCursor)} the type/direction is unknown at this point,
     * i.e. {@code type} is {@link Read#NO_ID}. In this case whatever type/direction if the first relationship read from storage will be used
     * to initialize tx-state iterator with. This is part of the contract of exposing detached references.
     *
     * @param nodeReference reference to the origin node.
     * @param reference reference to the place to start traversing these relationships.
     * @param type relationship type. May be {@link Read#NO_ID}, where it will be initialized to the type of first relationship read from storage later.
     * @param direction relationship direction, relative to {@code nodeReference}. May be {@code null}, where it will be initialized to the direction
     * of first relationship read from storage later.
     * @param nodeIsDense whether or not the origin node is dense.
     * @param read reference to {@link Read}.
     */
    void init( long nodeReference, long reference, int type, RelationshipDirection direction, boolean nodeIsDense, Read read )
    {
        this.type = type;
        this.direction = direction;
        this.lazySelection = type == NO_ID;
        this.storeCursor.init( nodeReference, reference, type, direction, nodeIsDense );
        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
        this.filterInitialized = !lazySelection;
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
        if ( currentAddedInTx != NO_ID )
        {
            // Here we compare the source/target nodes from tx-state to the origin node and decide the neighbour node from it
            long originNodeReference = originNodeReference();
            if ( txStateSourceNodeReference == originNodeReference )
            {
                return txStateTargetNodeReference;
            }
            else if ( txStateTargetNodeReference == originNodeReference )
            {
                return txStateSourceNodeReference;
            }
            else
            {
                throw new IllegalStateException( format(
                        "Relationship[%d] which was added in tx has an origin node [%d] which is neither source [%d] nor target [%d]",
                        currentAddedInTx, originNodeReference, txStateSourceNodeReference, txStateTargetNodeReference ) );
            }
        }
        return storeCursor.neighbourNodeReference();
    }

    @Override
    public long originNodeReference()
    {
        // This will be correct regardless of currentAddedInTx set or not
        return storeCursor.originNodeReference();
    }

    @Override
    public boolean next()
    {
        boolean hasChanges;

        if ( !filterInitialized )
        {
            hasChanges = hasChanges(); // <- may setup filter state if needed, for getting the correct relationships from tx-state
            setupFilterStateIfNeeded();
            if ( filterInitialized && !(hasChanges && read.txState().relationshipIsDeletedInThisTx( relationshipReference() )) )
            {
                return true;
            }
        }
        else
        {
            hasChanges = hasChanges();
        }

        // tx-state relationships
        if ( hasChanges )
        {
            if ( addedRelationships.hasNext() )
            {
                read.txState().relationshipVisit( addedRelationships.next(), relationshipTxStateDataVisitor );
                return true;
            }
            else
            {
                currentAddedInTx = NO_ID;
            }
        }

        while ( storeCursor.next() )
        {
            boolean skip = hasChanges && read.txState().relationshipIsDeletedInThisTx( storeCursor.entityReference() );
            if ( !skip && allowedToSeeEndNode() )
            {
                return true;
            }
        }
        return false;
    }

    private boolean allowedToSeeEndNode()
    {
        AccessMode mode = read.ktx.securityContext().mode();
        if ( mode.allowsTraverseAllLabels() )
        {
            return true;
        }
        try ( NodeCursor nodeCursor = read.cursors().allocateFullAccessNodeCursor() )
        {
            read.singleNode( storeCursor.neighbourNodeReference(), nodeCursor );
            boolean allowed = nodeCursor.next() && mode.allowsTraverseLabels( Arrays.stream( nodeCursor.labels().all() ).mapToInt( l -> (int) l ) );
            nodeCursor.close();
            return allowed;
        }
    }

    private void setupFilterStateIfNeeded()
    {
        if ( !filterInitialized && lazySelection )
        {
            storeCursor.next();
            type = storeCursor.type();
            direction = RelationshipDirection.directionOfStrict( storeCursor.originNodeReference(), storeCursor.sourceNodeReference(),
                    storeCursor.targetNodeReference() );
            filterInitialized = true;
        }
    }

    @Override
    public void close()
    {
        if ( !isClosed() )
        {
            read = null;
            type = ANY_RELATIONSHIP_TYPE;
            direction = null;
            filterInitialized = false;
            lazySelection = false;
            storeCursor.close();

            pool.accept( this );
        }
    }

    @Override
    protected void collectAddedTxStateSnapshot()
    {
        setupFilterStateIfNeeded();
        NodeState nodeState = read.txState().getNodeState( storeCursor.originNodeReference() );
        addedRelationships = type != NO_ID ?
                             nodeState.getAddedRelationships( direction, type ) :
                             nodeState.getAddedRelationships();
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
            return "RelationshipTraversalCursor[id=" + storeCursor.entityReference() +
                    ", " + storeCursor.toString() + "]";
        }
    }
}

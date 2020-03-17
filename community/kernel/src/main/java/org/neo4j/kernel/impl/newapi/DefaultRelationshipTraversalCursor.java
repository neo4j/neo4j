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
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.RelationshipSelection;
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
    private final PageCursorTracer cursorTracer;
    private LongIterator addedRelationships;
    private RelationshipSelection selection;

    DefaultRelationshipTraversalCursor( CursorPool<DefaultRelationshipTraversalCursor> pool, StorageRelationshipTraversalCursor storeCursor,
            PageCursorTracer cursorTracer )
    {
        super( storeCursor );
        this.pool = pool;
        this.cursorTracer = cursorTracer;
    }

    /**
     * Initializes this cursor to traverse over all relationships.
     * @param nodeReference reference to the origin node.
     * @param reference reference to the place to start traversing these relationships.
     * @param nodeIsDense whether or not the origin node is dense.
     * @param read reference to {@link Read}.
     */
    void init( long nodeReference, long reference, boolean nodeIsDense, RelationshipSelection selection, Read read )
    {
        this.selection = selection;
        // For lazily initialized filtering the type/direction will be null, which is what the storage cursor should get in this scenario
        this.storeCursor.init( nodeReference, reference, nodeIsDense, selection );
        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    @Override
    public void otherNode( NodeCursor cursor )
    {
        read.singleNode( otherNodeReference(), cursor );
    }

    @Override
    public long otherNodeReference()
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

        if ( !selection.isInitialized() )
        {
            hasChanges = hasChanges(); // <- may setup filter state if needed, for getting the correct relationships from tx-state
            setupFilterStateIfNeeded();
            if ( selection.isInitialized() && !(hasChanges && read.txState().relationshipIsDeletedInThisTx( relationshipReference() )) )
            {
                if ( tracer != null )
                {
                    tracer.onRelationship( relationshipReference() );
                }
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
                if ( tracer != null )
                {
                    tracer.onRelationship( relationshipReference() );
                }
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
            AccessMode mode = read.ktx.securityContext().mode();
            if ( !skip && mode.allowsTraverseRelType( storeCursor.type() ) && allowedToSeeEndNode( mode ) )
            {
                if ( tracer != null )
                {
                    tracer.onRelationship( relationshipReference() );
                }
                return true;
            }
        }
        return false;
    }

    private boolean allowedToSeeEndNode( AccessMode mode )
    {
        if ( mode.allowsTraverseAllLabels() )
        {
            return true;
        }
        try ( NodeCursor nodeCursor = read.cursors().allocateNodeCursor( cursorTracer ) )
        {
            read.singleNode( storeCursor.neighbourNodeReference(), nodeCursor );
            return nodeCursor.next();
        }
    }

    private void setupFilterStateIfNeeded()
    {
        if ( !selection.isInitialized() )
        {
            storeCursor.next(); // <-- since the store cursor has this selection too it will initialize it right here
        }
    }

    @Override
    public void closeInternal()
    {
        if ( !isClosed() )
        {
            read = null;
            selection = null;
            storeCursor.close();

            pool.accept( this );
        }
    }

    @Override
    protected void collectAddedTxStateSnapshot()
    {
        setupFilterStateIfNeeded();
        NodeState nodeState = read.txState().getNodeState( storeCursor.originNodeReference() );
        addedRelationships = selection.addedRelationship( nodeState );
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
            return "RelationshipTraversalCursor[id=" + storeCursor.entityReference() +
                    ", " + storeCursor.toString() + "]";
        }
    }
}

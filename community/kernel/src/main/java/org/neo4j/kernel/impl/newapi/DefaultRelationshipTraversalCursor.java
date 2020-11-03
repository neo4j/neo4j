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

import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.api.txstate.NodeState;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.newapi.Read.NO_ID;

class DefaultRelationshipTraversalCursor extends DefaultRelationshipCursor<StorageRelationshipTraversalCursor,DefaultRelationshipTraversalCursor>
        implements RelationshipTraversalCursor
{
    private final DefaultNodeCursor nodeCursor;
    private LongIterator addedRelationships;
    private long originNodeReference;
    private RelationshipSelection selection;
    private AccessMode mode;

    DefaultRelationshipTraversalCursor( CursorPool<DefaultRelationshipTraversalCursor> pool, StorageRelationshipTraversalCursor storeCursor,
            DefaultNodeCursor nodeCursor )
    {
        super( storeCursor, pool );
        this.nodeCursor = nodeCursor;
    }

    /**
     * Initializes this cursor to traverse over relationships, with a reference that was earlier retrieved from {@link NodeCursor#relationshipsReference()}.
     *
     * @param nodeReference reference to the origin node.
     * @param reference reference to the place to start traversing these relationships.
     * @param read reference to {@link Read}.
     */
    void init( long nodeReference, long reference, RelationshipSelection selection, Read read )
    {
        this.originNodeReference = nodeReference;
        this.selection = selection;
        this.storeCursor.init( nodeReference, reference, selection );
        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    /**
     * Initializes this cursor to traverse over relationships, directly from the {@link NodeCursor}.
     *
     * @param nodeCursor {@link NodeCursor} at the origin node.
     * @param read reference to {@link Read}.
     */
    void init( DefaultNodeCursor nodeCursor, RelationshipSelection selection, Read read )
    {
        this.originNodeReference = nodeCursor.nodeReference();
        this.selection = selection;
        nodeCursor.storeCursor.relationships( storeCursor, selection );
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
        return originNodeReference;
    }

    @Override
    public boolean next()
    {
        boolean hasChanges = hasChanges();

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
            if ( !skip && allowed() )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setTracer( KernelReadTracer tracer )
    {
        super.setTracer( tracer );
        storeCursor.setTracer( tracer );
    }

    @Override
    public void removeTracer()
    {
        storeCursor.removeTracer();
        super.removeTracer();
    }

    boolean allowed()
    {
        if ( mode == null )
        {
            mode = read.ktx.securityContext().mode();
        }
        return mode.allowsTraverseRelType( storeCursor.type() ) && allowedToSeeEndNode( mode );
    }

    private boolean allowedToSeeEndNode( AccessMode mode )
    {
        if ( mode.allowsTraverseAllLabels() )
        {
            return true;
        }
        read.singleNode( storeCursor.neighbourNodeReference(), nodeCursor );
        return nodeCursor.next();
    }

    @Override
    public void closeInternal()
    {
        if ( !isClosed() )
        {
            read = null;
            selection = null;
            mode = null;
            storeCursor.close();
        }
        super.closeInternal();
    }

    @Override
    protected void collectAddedTxStateSnapshot()
    {
        NodeState nodeState = read.txState().getNodeState( originNodeReference );
        addedRelationships = selection.addedRelationship( nodeState );
    }

    @Override
    public boolean isClosed()
    {
        return read == null;
    }

    public void release()
    {
        if ( storeCursor != null )
        {
            storeCursor.close();
        }
        if ( nodeCursor != null )
        {
            nodeCursor.close();
            nodeCursor.release();
        }
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
                    ", " + storeCursor + "]";
        }
    }
}

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

import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.LongSet;

import java.util.Arrays;

import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.collection.PrimitiveLongCollections.mergeToSet;

class DefaultNodeValueIndexCursor extends DefaultEntityValueIndexCursor<DefaultNodeValueIndexCursor> implements NodeValueIndexCursor
{
    private final DefaultNodeCursor securityNodeCursor;
    private int[] propertyIds;

    DefaultNodeValueIndexCursor( CursorPool<DefaultNodeValueIndexCursor> pool, DefaultNodeCursor securityNodeCursor, MemoryTracker memoryTracker )
    {
        super( pool, memoryTracker );
        this.securityNodeCursor = securityNodeCursor;
    }

    /**
     * Check that the user is allowed to access all nodes and properties given by the index descriptor.
     * <p>
     * If the current user is allowed to traverse all labels used in this index and read the properties
     * of all nodes in the index, we can skip checking on every node we get back.
     */
    @Override
    protected boolean canAccessAllDescribedEntities( IndexDescriptor descriptor, AccessMode accessMode )
    {
        propertyIds = descriptor.schema().getPropertyIds();
        final long[] labelIds = Arrays.stream( descriptor.schema().getEntityTokenIds() ).mapToLong( i -> i ).toArray();

        for ( long label : labelIds )
        {
            /*
             * If there can be nodes in the index that that are disallowed to traverse,
             * post-filtering is needed.
             */
            if ( !accessMode.allowsTraverseAllNodesWithLabel( label ) )
            {
                return false;
            }
        }

        for ( int propId : propertyIds )
        {
            /*
             * If reading the property is denied for some label,
             * there can be property values in the index that are disallowed,
             * so post-filtering is needed.
             */
            if ( accessMode.disallowsReadPropertyForSomeLabel( propId ) )
            {
                return false;
            }

            /*
             * If reading the property is not granted for all labels of the the index,
             * there can be property values in the index that are disallowed,
             * so post-filtering is needed.
             */
            for ( long label : labelIds )
            {
                if ( !accessMode.allowsReadNodeProperty( () -> Labels.from( label ), propId ) )
                {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    void traceOnEntity( KernelReadTracer tracer, long entity )
    {
        tracer.onNode( entity );
    }

    @Override
    String implementationName()
    {
        return "NodeValueIndexCursor";
    }

    @Override
    protected boolean allowed( long reference, AccessMode accessMode )
    {
        readEntity( read -> read.singleNode( reference, securityNodeCursor ) );
        if ( !securityNodeCursor.next() )
        {
            // This node is not visible to this security context
            return false;
        }

        boolean allowed = true;
        long[] labels = securityNodeCursor.labelsIgnoringTxStateSetRemove().all();
        for ( int prop : propertyIds )
        {
            allowed &= accessMode.allowsReadNodeProperty( () -> Labels.from( labels ), prop );
        }

        return allowed;
    }

    @Override
    public void node( NodeCursor cursor )
    {
        readEntity( read -> read.singleNode( entityReference(), cursor ) );
    }

    @Override
    public long nodeReference()
    {
        return entityReference();
    }

    @Override
    protected ImmutableLongSet removed( TransactionState txState, LongSet removedFromIndex )
    {
        return mergeToSet( txState.addedAndRemovedNodes().getRemoved(), removedFromIndex ).toImmutable();
    }

    public void release()
    {
        if ( securityNodeCursor != null )
        {
            securityNodeCursor.close();
            securityNodeCursor.release();
        }
    }
}

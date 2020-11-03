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

import org.eclipse.collections.api.set.primitive.LongSet;

import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.txstate.TransactionState;

import static org.neo4j.collection.PrimitiveLongCollections.mergeToSet;

class DefaultNodeLabelIndexCursor extends DefaultEntityTokenIndexCursor<DefaultNodeLabelIndexCursor> implements NodeLabelIndexCursor
{
    private final DefaultNodeCursor securityNodeCursor;

    DefaultNodeLabelIndexCursor( CursorPool<DefaultNodeLabelIndexCursor> pool, DefaultNodeCursor securityNodeCursor )
    {
        super( pool );
        this.securityNodeCursor = securityNodeCursor;
    }

    @Override
    LongSet createAddedInTxState( TransactionState txState, int token )
    {
        return txState.nodesWithLabelChanged( token ).getAdded().freeze();
    }

    @Override
    LongSet createDeletedInTxState( TransactionState txState, int token )
    {
        return mergeToSet( txState.addedAndRemovedNodes().getRemoved(), txState.nodesWithLabelChanged( token ).getRemoved() );
    }

    @Override
    void traceScan( KernelReadTracer tracer, int token )
    {
        tracer.onLabelScan( token );
    }

    @Override
    void traceNext( KernelReadTracer tracer, long entity )
    {
        tracer.onNode( entity );
    }

    @Override
    boolean allowedToSeeAllEntitiesWithToken( AccessMode accessMode, int token )
    {
        return accessMode.allowsTraverseAllNodesWithLabel( token );
    }

    @Override
    boolean allowedToSeeEntity( AccessMode accessMode, long entityReference, TokenSet tokens )
    {
        if ( tokens == null )
        {
            readEntity( read -> read.singleNode( entityReference, securityNodeCursor ) );
            return securityNodeCursor.next();
        }
        return accessMode.allowsTraverseNode( tokens.all() );
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
    public float score()
    {
        return Float.NaN;
    }

    @Override
    public TokenSet labels()
    {
        return tokens();
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "NodeLabelIndexCursor[closed state]";
        }
        else
        {
            return "NodeLabelIndexCursor[node=" + entityReference() + ", labels= " + tokens() + "]";
        }
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

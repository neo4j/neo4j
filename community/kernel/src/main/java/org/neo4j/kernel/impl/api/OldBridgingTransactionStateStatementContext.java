/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.util.Iterator;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.core.TransactionState;

/**
 * This is an intermediary layer in the {@link StatementContext} cake which takes into
 * account what's in the old {@link TransactionState}. This is necessary since the
 * transaction state currently exists in two places: {@link TransactionState} and
 * {@link org.neo4j.kernel.impl.api.state.TxState} (managed by {@link TransactionStateAwareStatementContext}.
 * 
 * Please remove when all NodeImpl/RelationshipImpl/TransactionState functionality lives
 * in the {@link KernelAPI}.
 */
@Deprecated
public class OldBridgingTransactionStateStatementContext extends CompositeStatementContext
{

    // TODO: TxState now knows about node that are deleted, refactor this logic to live inside transaction state aware context instead.

    private final TransactionState oldTransactionState;
    private final StatementContext delegate;

    public OldBridgingTransactionStateStatementContext( StatementContext delegate,
            TransactionState oldTransactionState )
    {
        super( delegate );
        this.oldTransactionState = oldTransactionState;
        this.delegate = delegate;
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        if ( oldTransactionState.nodeIsDeleted( nodeId ) )
            return false;
        return delegate.isLabelSetOnNode( labelId, nodeId );
    }
    
    @Override
    public Iterator<Long> getLabelsForNode( long nodeId )
    {
        if ( oldTransactionState.nodeIsDeleted( nodeId ) )
            return IteratorUtil.emptyIterator();
        return delegate.getLabelsForNode( nodeId );
    }

    @Override
    public Iterator<Long> getNodesWithLabel( long labelId )
    {
        return Iterables.filter( new Predicate<Long>()
        {
            @Override
            public boolean accept( Long nodeId )
            {
                return !oldTransactionState.nodeIsDeleted( nodeId.longValue() );
            }
        }, delegate.getNodesWithLabel( labelId ) );
    }
}

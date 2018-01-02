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
package org.neo4j.kernel;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.TransactionHook;
import org.neo4j.kernel.api.txstate.ReadableTxState;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.TxStateTransactionDataSnapshot;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Handle the collection of transaction event handlers, and fire events as needed.
 *
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public class TransactionEventHandlers
        implements Lifecycle, TransactionHook<TransactionEventHandlers.TransactionHandlerState>
{
    protected final Collection<TransactionEventHandler> transactionEventHandlers = new CopyOnWriteArraySet<>();

    private final NodeProxy.NodeActions nodeActions;
    private final RelationshipProxy.RelationshipActions relationshipActions;
    private final ThreadToStatementContextBridge bridge;

    public TransactionEventHandlers( NodeProxy.NodeActions nodeActions, RelationshipProxy.RelationshipActions
            relationshipActions, ThreadToStatementContextBridge bridge )
    {
        this.nodeActions = nodeActions;
        this.relationshipActions = relationshipActions;
        this.bridge = bridge;
    }

    @Override
    public void init()
            throws Throwable
    {
    }

    @Override
    public void start()
            throws Throwable
    {
    }

    @Override
    public void stop()
            throws Throwable
    {
    }

    @Override
    public void shutdown()
            throws Throwable
    {
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        this.transactionEventHandlers.add( handler );
        return handler;
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return unregisterHandler( this.transactionEventHandlers, handler );
    }

    private <T> T unregisterHandler( Collection<?> setOfHandlers, T handler )
    {
        if ( !setOfHandlers.remove( handler ) )
        {
            throw new IllegalStateException( handler + " isn't registered" );
        }
        return handler;
    }

    @Override
    public TransactionHandlerState beforeCommit( ReadableTxState state, KernelTransaction transaction,
            StoreReadLayer storeReadLayer )
    {
        if ( transactionEventHandlers.isEmpty() )
        {
            return null;
        }

        TransactionData txData = state == null ? EMPTY_DATA :
                new TxStateTransactionDataSnapshot( state, nodeActions, relationshipActions, storeReadLayer );

        TransactionHandlerState handlerStates = new TransactionHandlerState( txData );
        for ( TransactionEventHandler<?> handler : this.transactionEventHandlers )
        {
            try
            {
                handlerStates.add( handler, handler.beforeCommit( txData ) );
            }
            catch ( Throwable t )
            {
                handlerStates.failed( t );
            }
        }

        return handlerStates;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void afterCommit( ReadableTxState state,
            KernelTransaction transaction,
            TransactionHandlerState handlerState )
    {
        if ( transactionEventHandlers.isEmpty() )
        {
            return;
        }

        for ( HandlerAndState handlerAndState : handlerState.states )
        {
            handlerAndState.handler.afterCommit( handlerState.txData, handlerAndState.state );
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void afterRollback( ReadableTxState state,
            KernelTransaction transaction,
            TransactionHandlerState handlerState )
    {
        if ( transactionEventHandlers.isEmpty() )
        {
            return;
        }

        if ( handlerState == null )
        {
            // For legacy reasons, we don't call transaction handlers on implicit rollback.
            return;
        }

        for ( HandlerAndState handlerAndState : handlerState.states )
        {
            handlerAndState.handler.afterRollback( handlerState.txData, handlerAndState.state );
        }
    }

    public static class HandlerAndState
    {
        private final TransactionEventHandler handler;
        private final Object state;

        public HandlerAndState( TransactionEventHandler<?> handler, Object state )
        {
            this.handler = handler;
            this.state = state;
        }
    }

    public static class TransactionHandlerState implements TransactionHook.Outcome
    {
        private final TransactionData txData;
        private final List<HandlerAndState> states = new LinkedList<>();
        private Throwable error;

        public TransactionHandlerState( TransactionData txData )
        {
            this.txData = txData;
        }

        public void failed( Throwable error )
        {
            this.error = error;
        }

        @Override
        public boolean isSuccessful()
        {
            return error == null;
        }

        @Override
        public Throwable failure()
        {
            return error;
        }

        public void add( TransactionEventHandler<?> handler, Object state )
        {
            states.add( new HandlerAndState( handler, state ) );
        }
    }

    private static final TransactionData EMPTY_DATA = new TransactionData()
    {

        @Override
        public Iterable<Node> createdNodes()
        {
            return Iterables.empty();
        }

        @Override
        public Iterable<Node> deletedNodes()
        {
            return Iterables.empty();
        }

        @Override
        public boolean isDeleted( Node node )
        {
            return false;
        }

        @Override
        public Iterable<PropertyEntry<Node>> assignedNodeProperties()
        {
            return Iterables.empty();
        }

        @Override
        public Iterable<PropertyEntry<Node>> removedNodeProperties()
        {
            return Iterables.empty();
        }

        @Override
        public Iterable<LabelEntry> assignedLabels()
        {
            return Iterables.empty();
        }

        @Override
        public Iterable<LabelEntry> removedLabels()
        {
            return Iterables.empty();
        }

        @Override
        public Iterable<Relationship> createdRelationships()
        {
            return Iterables.empty();
        }

        @Override
        public Iterable<Relationship> deletedRelationships()
        {
            return Iterables.empty();
        }

        @Override
        public boolean isDeleted( Relationship relationship )
        {
            return false;
        }

        @Override
        public Iterable<PropertyEntry<Relationship>> assignedRelationshipProperties()
        {
            return Iterables.empty();
        }

        @Override
        public Iterable<PropertyEntry<Relationship>> removedRelationshipProperties()
        {
            return Iterables.empty();
        }
    };
}

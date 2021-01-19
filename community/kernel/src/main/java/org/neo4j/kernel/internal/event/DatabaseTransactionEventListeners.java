/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.internal.event;

import java.util.Collection;

import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.event.TransactionListenersState.ListenerState;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * Handle the collection of transaction event listeners, and fire events as needed.
 */
@SuppressWarnings( {"unchecked", "rawtypes"} )
public class DatabaseTransactionEventListeners
{
    private final GlobalTransactionEventListeners globalTransactionEventListeners;
    private final GraphDatabaseFacade databaseFacade;
    private final String databaseName;

    public DatabaseTransactionEventListeners( GraphDatabaseFacade databaseFacade, GlobalTransactionEventListeners globalTransactionEventListeners,
            NamedDatabaseId namedDatabaseId )
    {
        this.databaseFacade = databaseFacade;
        this.globalTransactionEventListeners = globalTransactionEventListeners;
        this.databaseName = namedDatabaseId.name();
    }

    public void registerTransactionEventListener( TransactionEventListener<?> listener )
    {
        globalTransactionEventListeners.registerTransactionEventListener( databaseName, listener );
    }

    public void unregisterTransactionEventListener( TransactionEventListener<?> listener )
    {
        globalTransactionEventListeners.unregisterTransactionEventListener( databaseName, listener );
    }

    public TransactionListenersState beforeCommit( ReadableTransactionState state, KernelTransaction transaction, StorageReader storageReader )
    {
        // The iterator grabs a snapshot of our list of listenerIterator
        Collection<TransactionEventListener<?>> eventListeners = globalTransactionEventListeners.getDatabaseTransactionEventListeners( databaseName );
        if ( !canInvokeBeforeCommitListeners( eventListeners, state ) )
        {
            // Use 'null' as a signal that no event listenerIterator were registered at beforeCommit time
            return null;
        }

        TransactionData txData = new TxStateTransactionDataSnapshot( state, storageReader, transaction );
        TransactionListenersState listenersStates = new TransactionListenersState( txData );

        boolean hasDataChanges = state.hasDataChanges();
        boolean isSystem = SYSTEM_DATABASE_NAME.equals( databaseName );
        for ( TransactionEventListener<?> listener : eventListeners )
        {
            if ( hasDataChanges || listener instanceof InternalTransactionEventListener || isSystem )
            {
                Object listenerState = null;
                try
                {
                    listenerState = listener.beforeCommit( txData, transaction.internalTransaction(), databaseFacade );
                }
                catch ( Throwable t )
                {
                    listenersStates.failed( t );
                }
                listenersStates.addListenerState( listener, listenerState );
            }
        }

        return listenersStates;
    }

    public void afterCommit( TransactionListenersState listeners )
    {
        if ( listeners == null )
        {
            // As per beforeCommit, 'null' means no listeners were registered in time for this transaction to
            // observe them.
            return;
        }

        TransactionData txData = listeners.getTxData();
        try
        {
            for ( ListenerState listenerState : listeners.getStates() )
            {
                TransactionEventListener listener = listenerState.getListener();
                listener.afterCommit( txData, listenerState.getState(), databaseFacade );
            }
        }
        finally
        {
            if ( txData instanceof TxStateTransactionDataSnapshot )
            {
                ((TxStateTransactionDataSnapshot) txData).close();
            }
            // else if could be EMPTY_DATA as well, and we don't want the user-facing TransactionData interface to have close() on it
        }
    }

    public void afterRollback( TransactionListenersState listenersState )
    {
        if ( listenersState == null )
        {
            // For legacy reasons, we don't call transaction listeners on implicit rollback.
            return;
        }

        TransactionData txData = listenersState.getTxData();
        for ( ListenerState listenerState : listenersState.getStates() )
        {
            TransactionEventListener listener = listenerState.getListener();
            listener.afterRollback( txData, listenerState.getState(), databaseFacade );
        }
    }

    private boolean canInvokeBeforeCommitListeners( Collection<TransactionEventListener<?>> listeners, ReadableTransactionState state )
    {
        return !listeners.isEmpty() && canInvokeListenersWithTransactionState( state );
    }

    private boolean canInvokeListenersWithTransactionState( ReadableTransactionState state )
    {
        return state != null && state.hasChanges();
    }
}

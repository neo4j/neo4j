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
package org.neo4j.kernel.internal.event;

import java.util.Collection;
import java.util.Iterator;

import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.events.GlobalTransactionEventListeners;
import org.neo4j.kernel.internal.event.TransactionListenersState.ListenerState;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static org.neo4j.kernel.internal.event.EmptyTransactionData.EMPTY_DATA;

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
            DatabaseId databaseId )
    {
        this.databaseFacade = databaseFacade;
        this.globalTransactionEventListeners = globalTransactionEventListeners;
        this.databaseName = databaseId.name();
    }

    public TransactionListenersState beforeCommit( ReadableTransactionState state, KernelTransaction transaction, StorageReader storageReader )
    {
        // The iterator grabs a snapshot of our list of listenerIterator
        Collection<TransactionEventListener<?>> eventListeners = globalTransactionEventListeners.getDatabaseTransactionEventListeners( databaseName );
        if ( eventListeners.isEmpty() )
        {
            // Use 'null' as a signal that no event listenerIterator were registered at beforeCommit time
            return null;
        }

        Iterator<TransactionEventListener<?>> listenerIterator = eventListeners.iterator();
        TransactionData txData = state == null ? EMPTY_DATA : new TxStateTransactionDataSnapshot( state, databaseFacade, storageReader, transaction );

        TransactionListenersState listenersStates = new TransactionListenersState( txData );
        while ( listenerIterator.hasNext() )
        {
            TransactionEventListener<?> listener = listenerIterator.next();
            Object listenerState = null;
            try
            {
                listenerState = listener.beforeCommit( txData, databaseFacade );
            }
            catch ( Throwable t )
            {
                listenersStates.failed( t );
            }
            listenersStates.addListenerState( listener, listenerState );
        }

        return listenersStates;
    }

    public void afterCommit( ReadableTransactionState state, KernelTransaction transaction, TransactionListenersState listeners )
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

    public void afterRollback( ReadableTransactionState state, KernelTransaction transaction, TransactionListenersState listenersState )
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
}

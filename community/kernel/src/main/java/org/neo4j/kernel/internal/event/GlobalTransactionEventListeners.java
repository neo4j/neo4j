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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.graphdb.event.TransactionEventListener;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class GlobalTransactionEventListeners
{
    private final ConcurrentHashMap<String,List<TransactionEventListener<?>>> globalTransactionEventListeners = new ConcurrentHashMap<>();

    /**
     * Registers {@code listener} as a listener for transaction events which
     * are generated from different places in the lifecycle of each
     * transaction.
     *
     * @param databaseName name of the database to listener transactions
     * @param listener the listener to receive events about different states
     *                in transaction life cycles.
     */
    public void registerTransactionEventListener( String databaseName, TransactionEventListener<?> listener )
    {
        requireNonNull( databaseName, "Database name is required." );
        requireNonNull( listener, "Transaction event listener is required." );
        globalTransactionEventListeners.compute( databaseName, ( s, transactionEventListeners ) ->
        {
            List<TransactionEventListener<?>> listeners = transactionEventListeners != null ? transactionEventListeners : new CopyOnWriteArrayList<>();
            if ( listeners.contains( listener ) )
            {
                return listeners;
            }
            listeners.add( listener );
            return listeners;
        } );
    }

    /**
     * Unregisters {@code listener} from the list of transaction event listeners.
     * If {@code handler} hasn't been registered with
     * {@link #registerTransactionEventListener(String, TransactionEventListener)} prior
     * to calling this method an {@link IllegalStateException} will be thrown.
     *
     * @param databaseName name of the database to listener transactions
     * @param listener the listener to receive events about different states
     *                in transaction life cycles.
     * @throws IllegalStateException if {@code listener} wasn't registered prior
     *                               to calling this method.
     */
    public void unregisterTransactionEventListener( String databaseName, TransactionEventListener<?> listener )
    {
        requireNonNull( databaseName );
        requireNonNull( listener );
        globalTransactionEventListeners.compute( databaseName, ( s, transactionEventListeners ) ->
        {
            if ( transactionEventListeners == null || !transactionEventListeners.remove( listener ) )
            {
                throw new IllegalStateException(
                        format( "Transaction event listener `%s` is not registered as listener for database `%s`.", listener, databaseName ) );
            }
            if ( transactionEventListeners.isEmpty() )
            {
                return null;
            }
            return transactionEventListeners;
        } );
    }

    public Collection<TransactionEventListener<?>> getDatabaseTransactionEventListeners( String databaseName )
    {
        return globalTransactionEventListeners.getOrDefault( databaseName, Collections.emptyList() );
    }
}

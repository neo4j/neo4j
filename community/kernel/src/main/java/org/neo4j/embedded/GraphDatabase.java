/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.embedded;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

/**
 * A running Neo4j graph database. Provides methods for controlling the database instance (e.g. shutdown,
 * adding event handlers, etc), and also implements {@link GraphDatabaseService} to provides all the methods
 * for working with the graph itself.
 */
public interface GraphDatabase extends GraphDatabaseService
{
    /**
     * Start building a Graph Database.
     *
     * @return a builder for a {@link GraphDatabase}
     */
    static GraphDatabase.Builder build()
    {
        return new Builder();
    }

    /**
     * Start a graph database by opening the specified filesystem directory containing the store.
     *
     * @param storeDir The filesystem location for the store, which will be created if necessary
     * @return The running database
     */
    static GraphDatabase open( File storeDir )
    {
        return build().open( storeDir );
    }

    /**
     * A builder for a {@link GraphDatabase}
     */
    class Builder extends GraphDatabaseBuilder<Builder,GraphDatabase>
    {
        @Override
        protected Builder self()
        {
            return this;
        }

        @Override
        protected GraphDatabase newInstance( File storeDir, Map<String,String> params,
                GraphDatabaseDependencies dependencies, GraphDatabaseFacadeFactory facadeFactory )
        {
            return new GraphDatabaseImpl( facadeFactory, storeDir, params, dependencies );
        }
    }

    /**
     * Use this method to check if the database is currently in a usable state.
     *
     * @param timeout timeout (in milliseconds) to wait for the database to become available.
     * If the database has been shut down {@code false} is returned immediately.
     * @return the state of the database: {@code true} if it is available, otherwise {@code false}
     */
    @Override
    boolean isAvailable( long timeout );

    /**
     * Shuts down Neo4j. After this method has been invoked, it's invalid to
     * invoke any methods in the Neo4j API and all references to this instance
     * of GraphDatabaseService should be discarded.
     */
    @Override
    void shutdown();

    /**
     * Registers {@code handler} as a handler for transaction events which
     * are generated from different places in the lifecycle of each
     * transaction. To guarantee that the handler gets all events properly
     * it shouldn't be registered when the application is running (i.e. in the
     * middle of one or more transactions). If the specified handler instance
     * has already been registered this method will do nothing.
     *
     * @param <T> the type of state object used in the handler, see more
     * documentation about it at {@link TransactionEventHandler}.
     * @param handler the handler to receive events about different states
     * in transaction lifecycles.
     * @return the handler passed in as the argument.
     */
    @Override
    <T> TransactionEventHandler<T> registerTransactionEventHandler( TransactionEventHandler<T> handler );

    /**
     * Unregisters {@code handler} from the list of transaction event handlers.
     * If {@code handler} hasn't been registered with
     * {@link #registerTransactionEventHandler(TransactionEventHandler)} prior
     * to calling this method an {@link IllegalStateException} will be thrown.
     * After a successful call to this method the {@code handler} will no
     * longer receive any transaction events.
     *
     * @param <T> the type of state object used in the handler, see more
     * documentation about it at {@link TransactionEventHandler}.
     * @param handler the handler to receive events about different states
     * in transaction lifecycles.
     * @return the handler passed in as the argument.
     * @throws IllegalStateException if {@code handler} wasn't registered prior
     * to calling this method.
     */
    @Override
    <T> TransactionEventHandler<T> unregisterTransactionEventHandler( TransactionEventHandler<T> handler );

    /**
     * Registers {@code handler} as a handler for kernel events which
     * are generated from different places in the lifecycle of the kernel.
     * To guarantee proper behavior the handler should be registered right
     * after the graph database has been started. If the specified handler
     * instance has already been registered this method will do nothing.
     *
     * @param handler the handler to receive events about different states
     * in the kernel lifecycle.
     * @return the handler passed in as the argument.
     */
    @Override
    KernelEventHandler registerKernelEventHandler( KernelEventHandler handler );

    /**
     * Unregisters {@code handler} from the list of kernel event handlers.
     * If {@code handler} hasn't been registered with
     * {@link #registerKernelEventHandler(KernelEventHandler)} prior to calling
     * this method an {@link IllegalStateException} will be thrown.
     * After a successful call to this method the {@code handler} will no
     * longer receive any kernel events.
     *
     * @param handler the handler to receive events about different states
     * in the kernel lifecycle.
     * @return the handler passed in as the argument.
     * @throws IllegalStateException if {@code handler} wasn't registered prior
     * to calling this method.
     */
    @Override
    KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler );
}

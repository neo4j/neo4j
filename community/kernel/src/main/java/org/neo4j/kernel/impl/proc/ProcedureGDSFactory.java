/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.proc;

import java.net.URL;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.legacyindex.AutoIndexing;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySession;
import org.neo4j.kernel.impl.store.StoreId;

public class ProcedureGDSFactory implements ThrowingFunction<CallableProcedure.Context,GraphDatabaseService,ProcedureException>
{
    private final Config config;
    private final DependencyResolver resolver;
    private final Supplier<StoreId> storeId;
    private final Supplier<QueryExecutionEngine> queryExecutor;
    private final CoreAPIAvailabilityGuard availability;
    private final Function<URL, URL> urlValidator;
    private AutoIndexing autoIndexing = AutoIndexing.UNSUPPORTED;

    public ProcedureGDSFactory( Config config,
                                DependencyResolver resolver,
                                Supplier<StoreId> storeId,
                                Supplier<QueryExecutionEngine> queryExecutor,
                                CoreAPIAvailabilityGuard availability,
                                Function<URL, URL> urlValidator )
    {
        this.config = config;
        this.resolver = resolver;
        this.storeId = storeId;
        this.queryExecutor = queryExecutor;
        this.availability = availability;
        this.urlValidator = urlValidator;
    }

    @Override
    public GraphDatabaseService apply( CallableProcedure.Context context ) throws ProcedureException
    {
        KernelTransaction transaction = context.get( CallableProcedure.Context.KERNEL_TRANSACTION );

        GraphDatabaseFacade facade = new GraphDatabaseFacade();
        facade.init( config, new GraphDatabaseFacade.SPI()
        {
            @Override
            public boolean databaseIsAvailable( long timeout )
            {
                return availability.isAvailable( timeout );
            }

            @Override
            public DependencyResolver resolver()
            {
                return resolver;
            }

            @Override
            public StoreId storeId()
            {
                return storeId.get();
            }

            @Override
            public String storeDir()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public String name()
            {
                return "ProcedureGraphDatabaseService";
            }

            @Override
            public KernelTransaction currentTransaction()
            {
                availability.assertDatabaseAvailable();
                return transaction;
            }

            @Override
            public boolean isInOpenTransaction()
            {
                return transaction.isOpen();
            }

            @Override
            public Statement currentStatement()
            {
                return transaction.acquireStatement();
            }

            @Override
            public Result executeQuery( String query, Map<String,Object> parameters, QuerySession querySession )
            {
                try
                {
                    availability.assertDatabaseAvailable();
                    return queryExecutor.get().executeQuery( query, parameters, querySession );
                }
                catch ( QueryExecutionKernelException e )
                {
                    throw e.asUserException();
                }
            }

            @Override
            public AutoIndexing autoIndexing() {
                return autoIndexing;
            }

            @Override
            public void registerKernelEventHandler( KernelEventHandler handler )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void unregisterKernelEventHandler( KernelEventHandler handler )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> void registerTransactionEventHandler( TransactionEventHandler<T> handler )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> void unregisterTransactionEventHandler( TransactionEventHandler<T> handler )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public URL validateURLAccess( URL url ) throws URLAccessValidationError
            {
                return urlValidator.apply( url );
            }

            @Override
            public void shutdown()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public KernelTransaction beginTransaction()
            {
                throw new UnsupportedOperationException();
            }

        } );

        return facade;
    }
}

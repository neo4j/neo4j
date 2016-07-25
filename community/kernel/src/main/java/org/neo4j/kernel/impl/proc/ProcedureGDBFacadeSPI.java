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

import java.io.File;
import java.net.URL;
import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.legacyindex.AutoIndexing;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySession;
import org.neo4j.kernel.impl.store.StoreId;

class ProcedureGDBFacadeSPI implements GraphDatabaseFacade.SPI
{
    private final Thread transactionThread;
    private final KernelTransaction transaction;
    private final Supplier<QueryExecutionEngine> queryExecutor;

    private final DependencyResolver resolver;
    private final AutoIndexing autoIndexing;
    private final Supplier<StoreId> storeId;
    private final CoreAPIAvailabilityGuard availability;
    private final ThrowingFunction<URL,URL,URLAccessValidationError> urlValidator;
    private final File storeDir;

    public ProcedureGDBFacadeSPI( Thread transactionThread,  KernelTransaction transaction, Supplier<QueryExecutionEngine> queryExecutor,
            File storeDir, DependencyResolver resolver, AutoIndexing autoIndexing,
            Supplier<StoreId> storeId, CoreAPIAvailabilityGuard availability,
            ThrowingFunction<URL,URL,URLAccessValidationError> urlValidator )
    {
        this.transactionThread = transactionThread;
        this.transaction = transaction;
        this.queryExecutor = queryExecutor;
        this.storeDir = storeDir;
        this.resolver = resolver;
        this.autoIndexing = autoIndexing;
        this.storeId = storeId;
        this.availability = availability;
        this.urlValidator = urlValidator;
    }

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
    public File storeDir()
    {
        return storeDir;
    }

    @Override
    public String name()
    {
        return "ProcedureGraphDatabaseService";
    }

    private void assertSameThread()
    {
        if ( transactionThread != Thread.currentThread() )
        {
            throw new UnsupportedOperationException( "Creating new transactions and/or spawning threads are " +
                                                     "not supported operations in store procedures." );
        }
    }

    @Override
    public KernelTransaction currentTransaction()
    {
        availability.assertDatabaseAvailable();
        assertSameThread();
        return transaction;
    }

    @Override
    public boolean isInOpenTransaction()
    {
        assertSameThread();
        return transaction.isOpen();
    }

    @Override
    public Statement currentStatement()
    {
        assertSameThread();
        return transaction.acquireStatement();
    }

    @Override
    public Result executeQuery( String query, Map<String,Object> parameters, QuerySession querySession )
    {
        try
        {
            availability.assertDatabaseAvailable();
            assertSameThread();
            return queryExecutor.get().executeQuery( query, parameters, querySession );
        }
        catch ( QueryExecutionKernelException e )
        {
            throw e.asUserException();
        }
    }

    @Override
    public AutoIndexing autoIndexing()
    {
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
    public GraphDatabaseQueryService queryService()
    {
        return queryExecutor.get().queryService();
    }

    @Override
    public void shutdown()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public KernelTransaction beginTransaction( KernelTransaction.Type type, AccessMode accessMode )
    {
        throw new UnsupportedOperationException();
    }
}

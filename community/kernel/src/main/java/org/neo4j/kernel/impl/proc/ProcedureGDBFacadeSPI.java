/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.DataSourceModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.values.virtual.MapValue;

class ProcedureGDBFacadeSPI implements GraphDatabaseFacade.SPI
{
    private final File storeDir;
    private final DataSourceModule sourceModule;
    private final DependencyResolver resolver;
    private final CoreAPIAvailabilityGuard availability;
    private final ThrowingFunction<URL,URL,URLAccessValidationError> urlValidator;
    private final SecurityContext securityContext;

    ProcedureGDBFacadeSPI( PlatformModule platform, DataSourceModule sourceModule, DependencyResolver resolver,
            CoreAPIAvailabilityGuard availability, ThrowingFunction<URL,URL,URLAccessValidationError> urlValidator,
            SecurityContext securityContext )
    {
        this.storeDir = platform.storeDir;
        this.sourceModule = sourceModule;
        this.resolver = resolver;
        this.availability = availability;
        this.urlValidator = urlValidator;
        this.securityContext = securityContext;
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
        return sourceModule.storeId.get();
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

    @Override
    public KernelTransaction currentTransaction()
    {
        availability.assertDatabaseAvailable();
        KernelTransaction tx = sourceModule.threadToTransactionBridge.getKernelTransactionBoundToThisThread( false );
        if ( tx == null )
        {
            throw new NotInTransactionException();
        }
        return tx;
    }

    @Override
    public boolean isInOpenTransaction()
    {
        return sourceModule.threadToTransactionBridge.hasTransaction();
    }

    @Override
    public Statement currentStatement()
    {
        return sourceModule.threadToTransactionBridge.get();
    }

    @Override
    public Result executeQuery( String query, Map<String,Object> parameters, TransactionalContext tc )
    {
        try
        {
            availability.assertDatabaseAvailable();
            return sourceModule.queryExecutor.get().executeQuery( query, parameters, tc );
        }
        catch ( QueryExecutionKernelException e )
        {
            throw e.asUserException();
        }
    }

    @Override
    public Result executeQuery( String query, MapValue parameters, TransactionalContext tc )
    {
        try
        {
            availability.assertDatabaseAvailable();
            return sourceModule.queryExecutor.get().executeQuery( query, parameters, tc );
        }
        catch ( QueryExecutionKernelException e )
        {
            throw e.asUserException();
        }
    }

    @Override
    public AutoIndexing autoIndexing()
    {
        return sourceModule.autoIndexing;
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
        return resolver.resolveDependency( GraphDatabaseQueryService.class );
    }

    @Override
    public void shutdown()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public KernelTransaction beginTransaction( KernelTransaction.Type type, SecurityContext ignoredSecurityContext, long timeout )
    {
        try
        {
            availability.assertDatabaseAvailable();
            KernelTransaction kernelTx = sourceModule.kernelAPI.get().newTransaction( type, this.securityContext, timeout );
            kernelTx.registerCloseListener(
                    txId -> sourceModule.threadToTransactionBridge.unbindTransactionFromCurrentThread() );
            sourceModule.threadToTransactionBridge.bindTransactionToCurrentThread( kernelTx );
            return kernelTx;
        }
        catch ( TransactionFailureException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( e.getMessage(), e );
        }
    }
}

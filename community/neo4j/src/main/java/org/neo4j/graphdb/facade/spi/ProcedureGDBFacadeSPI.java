/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.graphdb.facade.spi;

import java.net.URL;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.event.DatabaseEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.module.DatabaseModule;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.values.virtual.MapValue;

public class ProcedureGDBFacadeSPI implements GraphDatabaseFacade.SPI
{
    private final DatabaseLayout databaseLayout;
    private final DatabaseModule sourceModule;
    private final DependencyResolver resolver;
    private final CoreAPIAvailabilityGuard availability;
    private final ThrowingFunction<URL,URL,URLAccessValidationError> urlValidator;
    private final SecurityContext securityContext;
    private final ThreadToStatementContextBridge threadToTransactionBridge;

    public ProcedureGDBFacadeSPI( DatabaseModule sourceModule, DependencyResolver resolver, CoreAPIAvailabilityGuard availability,
            ThrowingFunction<URL,URL,URLAccessValidationError> urlValidator, SecurityContext securityContext,
            ThreadToStatementContextBridge threadToTransactionBridge )
    {
        this.databaseLayout = sourceModule.database.getDatabaseLayout();
        this.sourceModule = sourceModule;
        this.resolver = resolver;
        this.availability = availability;
        this.urlValidator = urlValidator;
        this.securityContext = securityContext;
        this.threadToTransactionBridge = threadToTransactionBridge;
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
    public DatabaseLayout databaseLayout()
    {
        return databaseLayout;
    }

    @Override
    public String name()
    {
        return "ProcedureGraphDatabaseService";
    }

    @Override
    public Result executeQuery( String query, MapValue parameters, TransactionalContext tc )
    {
        try
        {
            availability.assertDatabaseAvailable();
            return sourceModule.database.getExecutionEngine().executeQuery( query, parameters, tc, false );
        }
        catch ( QueryExecutionKernelException e )
        {
            throw e.asUserException();
        }
    }

    @Override
    public void registerDatabaseEventHandler( DatabaseEventHandler handler )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unregisterDatabaseEventHandler( DatabaseEventHandler handler )
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
    public KernelTransaction beginTransaction( KernelTransaction.Type type, LoginContext ignored, ClientConnectionInfo connectionInfo, long timeout )
    {
        try
        {
            availability.assertDatabaseAvailable();
            KernelTransaction kernelTx = sourceModule.kernelAPI.get().beginTransaction( type, this.securityContext, connectionInfo, timeout );
            kernelTx.registerCloseListener(
                    txId -> threadToTransactionBridge.unbindTransactionFromCurrentThread() );
            threadToTransactionBridge.bindTransactionToCurrentThread( kernelTx );
            return kernelTx;
        }
        catch ( TransactionFailureException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( e.getMessage(), e );
        }
    }
}

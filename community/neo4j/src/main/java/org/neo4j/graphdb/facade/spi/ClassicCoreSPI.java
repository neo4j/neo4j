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
package org.neo4j.graphdb.facade.spi;

import java.net.URL;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.module.DataSourceModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.Logger;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.values.virtual.MapValue;

/**
 * This implements the backend for the "classic" Core API - meaning the surface-layer-of-the-database, thread bound API.
 * It's a thin veneer to wire the various components the kernel and related utilities expose in a way that
 * {@link GraphDatabaseFacade} likes.
 * @see org.neo4j.kernel.impl.factory.GraphDatabaseFacade.SPI
 */
public class ClassicCoreSPI implements GraphDatabaseFacade.SPI
{
    private final PlatformModule platform;
    private final DataSourceModule dataSource;
    private final Logger msgLog;
    private final CoreAPIAvailabilityGuard availability;
    private final ThreadToStatementContextBridge threadToTransactionBridge;

    public ClassicCoreSPI( PlatformModule platform, DataSourceModule dataSource, Logger msgLog, CoreAPIAvailabilityGuard availability,
            ThreadToStatementContextBridge threadToTransactionBridge )
    {
        this.platform = platform;
        this.dataSource = dataSource;
        this.msgLog = msgLog;
        this.availability = availability;
        this.threadToTransactionBridge = threadToTransactionBridge;
    }

    @Override
    public boolean databaseIsAvailable( long timeout )
    {
        return dataSource.neoStoreDataSource.getDatabaseAvailabilityGuard().isAvailable( timeout );
    }

    @Override
    public Result executeQuery( String query, MapValue parameters, TransactionalContext transactionalContext )
    {
        try
        {
            availability.assertDatabaseAvailable();
            return dataSource.neoStoreDataSource.getExecutionEngine().executeQuery( query, parameters, transactionalContext );
        }
        catch ( QueryExecutionKernelException e )
        {
            throw e.asUserException();
        }
    }

    @Override
    public AutoIndexing autoIndexing()
    {
        return dataSource.neoStoreDataSource.getAutoIndexing();
    }

    @Override
    public DependencyResolver resolver()
    {
        return dataSource.neoStoreDataSource.getDependencyResolver();
    }

    @Override
    public void registerKernelEventHandler( KernelEventHandler handler )
    {
        platform.eventHandlers.registerKernelEventHandler( handler );
    }

    @Override
    public void unregisterKernelEventHandler( KernelEventHandler handler )
    {
        platform.eventHandlers.unregisterKernelEventHandler( handler );
    }

    @Override
    public <T> void registerTransactionEventHandler( TransactionEventHandler<T> handler )
    {
        dataSource.neoStoreDataSource.getTransactionEventHandlers().registerTransactionEventHandler( handler );
    }

    @Override
    public <T> void unregisterTransactionEventHandler( TransactionEventHandler<T> handler )
    {
        dataSource.neoStoreDataSource.getTransactionEventHandlers().unregisterTransactionEventHandler( handler );
    }

    @Override
    public StoreId storeId()
    {
        return dataSource.storeId.get();
    }

    @Override
    public DatabaseLayout databaseLayout()
    {
        return dataSource.neoStoreDataSource.getDatabaseLayout();
    }

    @Override
    public URL validateURLAccess( URL url ) throws URLAccessValidationError
    {
        return platform.urlAccessRule.validate( platform.config, url );
    }

    @Override
    public GraphDatabaseQueryService queryService()
    {
        return resolver().resolveDependency( GraphDatabaseQueryService.class );
    }

    @Override
    public Kernel kernel()
    {
        return resolver().resolveDependency( Kernel.class );
    }

    @Override
    public String name()
    {
        return platform.databaseInfo.toString();
    }

    @Override
    public void shutdown()
    {
        try
        {
            msgLog.log( "Shutdown started" );
            dataSource.neoStoreDataSource.getDatabaseAvailabilityGuard().shutdown();
            platform.life.shutdown();
        }
        catch ( LifecycleException throwable )
        {
            msgLog.log( "Shutdown failed", throwable );
            throw throwable;
        }
    }

    @Override
    public KernelTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, long timeout )
    {
        try
        {
            availability.assertDatabaseAvailable();
            KernelTransaction kernelTx = dataSource.kernelAPI.get().beginTransaction( type, loginContext, timeout );
            kernelTx.registerCloseListener( txId -> threadToTransactionBridge.unbindTransactionFromCurrentThread() );
            threadToTransactionBridge.bindTransactionToCurrentThread( kernelTx );
            return kernelTx;
        }
        catch ( TransactionFailureException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( e.getMessage(), e );
        }
    }
}

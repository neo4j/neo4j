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

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
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
    private final DatabaseInfo databaseInfo;
    private final Database database;
    private final CoreAPIAvailabilityGuard availability;
    private final ThreadToStatementContextBridge threadToTransactionBridge;

    public ClassicCoreSPI( DatabaseInfo databaseInfo, Database database, CoreAPIAvailabilityGuard availability,
            ThreadToStatementContextBridge threadToTransactionBridge )
    {
        this.databaseInfo = databaseInfo;
        this.database = database;
        this.availability = availability;
        this.threadToTransactionBridge = threadToTransactionBridge;
    }

    @Override
    public boolean databaseIsAvailable( long timeout )
    {
        return database.getDatabaseAvailabilityGuard().isAvailable( timeout );
    }

    @Override
    public Result executeQuery( String query, MapValue parameters, TransactionalContext transactionalContext )
    {
        try
        {
            availability.assertDatabaseAvailable();
            return database.getExecutionEngine().executeQuery( query, parameters, transactionalContext, false );
        }
        catch ( QueryExecutionKernelException e )
        {
            throw e.asUserException();
        }
    }

    @Override
    public DependencyResolver resolver()
    {
        return database.getDependencyResolver();
    }

    @Override
    public StoreId storeId()
    {
        return database.getStoreId();
    }

    @Override
    public DatabaseLayout databaseLayout()
    {
        return database.getDatabaseLayout();
    }

    @Override
    public GraphDatabaseQueryService queryService()
    {
        return resolver().resolveDependency( GraphDatabaseQueryService.class );
    }

    @Override
    public String name()
    {
        return databaseInfo.toString();
    }

    @Override
    public KernelTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo, long timeout )
    {
        try
        {
            availability.assertDatabaseAvailable();
            KernelTransaction kernelTx = database.getKernel().beginTransaction( type, loginContext, connectionInfo, timeout );
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

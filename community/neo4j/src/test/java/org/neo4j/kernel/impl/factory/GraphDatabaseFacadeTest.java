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
package org.neo4j.kernel.impl.factory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

class GraphDatabaseFacadeTest
{
    private final GraphDatabaseFacade graphDatabaseFacade = new GraphDatabaseFacade();
    private GraphDatabaseQueryService queryService;
    private InwardKernel inwardKernel;

    @BeforeEach
    void setUp()
    {
        queryService = mock( GraphDatabaseQueryService.class );
        Database database = mock( Database.class, RETURNS_MOCKS );
        Dependencies resolver = mock( Dependencies.class );
        inwardKernel = mock( InwardKernel.class, RETURNS_MOCKS );
        when( database.getKernel() ).thenReturn( inwardKernel );
        when( database.getDependencyResolver() ).thenReturn( resolver );
        Statement statement = mock( Statement.class, RETURNS_DEEP_STUBS );
        ThreadToStatementContextBridge contextBridge = mock( ThreadToStatementContextBridge.class );

        when( resolver.resolveDependency( ThreadToStatementContextBridge.class ) ).thenReturn( contextBridge );
        when( resolver.resolveDependency( GraphDatabaseQueryService.class ) ).thenReturn( queryService );
        when( contextBridge.get() ).thenReturn( statement );
        Config config = Config.defaults();
        when( resolver.resolveDependency( Config.class ) ).thenReturn( config );

        graphDatabaseFacade.init( database, contextBridge, config, DatabaseInfo.COMMUNITY, mock( CoreAPIAvailabilityGuard.class ) );
    }

    @Test
    void beginTransactionWithCustomTimeout() throws TransactionFailureException
    {
        graphDatabaseFacade.beginTx( 10, TimeUnit.MILLISECONDS );

        verify( inwardKernel ).beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED, EMBEDDED_CONNECTION, 10L );
    }

    @Test
    void beginTransaction() throws TransactionFailureException
    {
        graphDatabaseFacade.beginTx();

        long timeout = Config.defaults().get( GraphDatabaseSettings.transaction_timeout ).toMillis();
        verify( inwardKernel ).beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED, EMBEDDED_CONNECTION, timeout );
    }

    @Test
    void executeQueryWithCustomTimeoutShouldStartTransactionWithRequestedTimeout() throws TransactionFailureException
    {
        graphDatabaseFacade.execute( "create (n)", 157L, TimeUnit.SECONDS );
        verify( inwardKernel ).beginTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED, EMBEDDED_CONNECTION,
            TimeUnit.SECONDS.toMillis( 157L ) );

        graphDatabaseFacade.execute( "create (n)", new HashMap<>(), 247L, MINUTES );
        verify( inwardKernel ).beginTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED, EMBEDDED_CONNECTION,
            MINUTES.toMillis( 247L ) );
    }

    @Test
    void executeQueryStartDefaultTransaction() throws TransactionFailureException
    {
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        InternalTransaction transaction = new TopLevelTransaction( kernelTransaction );

        when( queryService.beginTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED, EMBEDDED_CONNECTION) )
            .thenReturn( transaction );

        graphDatabaseFacade.execute( "create (n)" );
        graphDatabaseFacade.execute( "create (n)", new HashMap<>() );

        long timeout = Config.defaults().get( GraphDatabaseSettings.transaction_timeout ).toMillis();
        verify( inwardKernel, times( 2 ) ).beginTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED, EMBEDDED_CONNECTION, timeout );
    }
}

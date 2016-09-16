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
package org.neo4j.kernel.impl.factory;


import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GraphDatabaseFacadeTest
{
    private GraphDatabaseFacade.SPI spi = Mockito.mock( GraphDatabaseFacade.SPI.class, Mockito.RETURNS_DEEP_STUBS );
    private GraphDatabaseFacade graphDatabaseFacade = new GraphDatabaseFacade();
    private GraphDatabaseQueryService queryService;
    private DependencyResolver dependencyResolver;
    private Config defaultConfig;
    private Statement statement;

    @Before
    public void setUp()
    {
        queryService = mock( GraphDatabaseQueryService.class );
        dependencyResolver = mock( DependencyResolver.class );
        statement = mock( Statement.class, Mockito.RETURNS_DEEP_STUBS );
        ThreadToStatementContextBridge contextBridge = mock( ThreadToStatementContextBridge.class );

        when( spi.queryService() ).thenReturn( queryService );
        when( queryService.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class ) )
                .thenReturn( contextBridge );
        when( contextBridge.get() ).thenReturn( statement );
        defaultConfig = Config.defaults();

        graphDatabaseFacade.init( spi, defaultConfig );
        graphDatabaseFacade.initTransactionalContextFactoryFromSPI();
    }

    @Test
    public void beginTransactionWithCustomTimeout() throws Exception
    {
        graphDatabaseFacade.beginTx( 10, TimeUnit.MILLISECONDS );

        verify( spi ).beginTransaction( KernelTransaction.Type.explicit, AccessMode.Static.FULL, 10L );
    }

    @Test
    public void beginTransaction()
    {
        graphDatabaseFacade.beginTx();

        long timeout = defaultConfig.get( GraphDatabaseSettings.transaction_timeout );
        verify( spi ).beginTransaction( KernelTransaction.Type.explicit, AccessMode.Static.FULL, timeout );
    }

    @Test
    public void executeQueryWithCustomTimeoutShouldStartTransactionWithRequestedTimeout()
    {
        graphDatabaseFacade.execute( "create (n)", 157L, TimeUnit.SECONDS );
        verify( spi ).beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL,
                TimeUnit.SECONDS.toMillis( 157L ) );

        graphDatabaseFacade.execute( "create (n)", new HashMap<>(), 247L, TimeUnit.MINUTES );
        verify( spi ).beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL,
                TimeUnit.MINUTES.toMillis( 247L ) );
    }

    @Test
    public void executeQueryStartDefaultTransaction()
    {
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        InternalTransaction transaction = new TopLevelTransaction( kernelTransaction, null );

        when( queryService.beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL ) )
                .thenReturn( transaction );

        graphDatabaseFacade.execute( "create (n)" );
        graphDatabaseFacade.execute( "create (n)", new HashMap<>() );

        long timeout = defaultConfig.get( GraphDatabaseSettings.transaction_timeout );
        verify( spi, times( 2 ) ).beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL, timeout );
    }
}

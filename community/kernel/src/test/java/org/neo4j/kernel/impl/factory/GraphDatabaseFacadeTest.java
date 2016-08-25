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

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

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

    @Before
    public void setUp()
    {
        queryService = mock( GraphDatabaseQueryService.class );
        dependencyResolver = mock( DependencyResolver.class );
        ThreadToStatementContextBridge contextBridge = mock( ThreadToStatementContextBridge.class );

        when( spi.queryService() ).thenReturn( queryService );
        when( queryService.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class ) ).thenReturn( contextBridge );

        graphDatabaseFacade.init( spi );
    }

    @Test
    public void beginTransactionWithCustomTimeout() throws Exception
    {
        graphDatabaseFacade.beginTx( 10L );

        verify( spi ).beginTransaction( KernelTransaction.Type.explicit, AccessMode.Static.FULL, 10L );
    }

    @Test
    public void beginTransaction()
    {
        graphDatabaseFacade.beginTx();

        verify( spi ).beginTransaction( KernelTransaction.Type.explicit, AccessMode.Static.FULL );
    }

    @Test
    public void executeQueryWithCustomTimeoutShouldStartTransactionWithRequestedTimeout()
    {
        graphDatabaseFacade.execute( "create (n)", 157L );
        verify( spi ).beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL, 157L );

        graphDatabaseFacade.execute( "create (n)", new HashMap<>(), 247L );
        verify( spi ).beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL, 247L );
    }

    @Test
    public void executeQueryStartDefaultTransaction()
    {
        graphDatabaseFacade.execute( "create (n)" );
        graphDatabaseFacade.execute( "create (n)", new HashMap<>() );

        verify( spi, times( 2 ) ).beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL );
    }
}

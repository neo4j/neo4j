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
package org.neo4j.server.rest.transactional;

import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransitionalPeriodTransactionMessContainerTest
{

    private GraphDatabaseFacade databaseFacade = mock( GraphDatabaseFacade.class );
    private GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
    private HttpServletRequest request = mock( HttpServletRequest.class );
    private DependencyResolver dependencyResolver = mock( DependencyResolver.class );
    private ThreadToStatementContextBridge bridge = mock( ThreadToStatementContextBridge.class );
    private InternalTransaction internalTransaction = mock( InternalTransaction.class );
    private KernelTransaction.Type type = KernelTransaction.Type.implicit;
    private AccessMode.Static accessMode = AccessMode.Static.FULL;

    @Before
    public void setUp()
    {
        when( internalTransaction.transactionType() ).thenReturn( type );
        when( internalTransaction.mode() ).thenReturn( accessMode );
        when( databaseFacade.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( queryService.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class ) ).thenReturn( bridge );
    }

    @Test
    public void startTransactionWithCustomTimeout() throws Exception
    {
        when( databaseFacade.beginTransaction( type, accessMode, 10 ) ).thenReturn( internalTransaction );

        TransitionalPeriodTransactionMessContainer transactionMessContainer =
                new TransitionalPeriodTransactionMessContainer( databaseFacade );
        transactionMessContainer.create( queryService, type, accessMode, 10, request );

        verify( databaseFacade ).beginTransaction( type, accessMode, 10 );
    }

    @Test
    public void startDefaultTransactionWhenTimeoutNotSpecified()
    {
        when( databaseFacade.beginTransaction( type, accessMode ) ).thenReturn( internalTransaction );

        TransitionalPeriodTransactionMessContainer transactionMessContainer =
                new TransitionalPeriodTransactionMessContainer( databaseFacade );
        transactionMessContainer.create( queryService, type, accessMode, 0, request );

        verify( databaseFacade ).beginTransaction( type, accessMode );
    }
}

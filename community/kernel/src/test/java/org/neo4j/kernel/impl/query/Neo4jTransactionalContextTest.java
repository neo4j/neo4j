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
package org.neo4j.kernel.impl.query;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class Neo4jTransactionalContextTest
{

    private GraphDatabaseQueryService databaseQueryService;
    private DependencyResolver dependencyResolver;
    private ThreadToStatementContextBridge statementContextBridge;
    private Guard guard;
    private KernelStatement statement;
    private PropertyContainerLocker propertyContainerLocker;
    private TopLevelTransaction transaction;

    @Before
    public void setUp()
    {
        setUpMocks();
    }

    @Test
    public void checkKernelStatementOnCheck() throws Exception
    {
        Neo4jTransactionalContext transactionalContext =
                new Neo4jTransactionalContext( databaseQueryService, transaction, statement, propertyContainerLocker );

        transactionalContext.check();

        verify( guard ).check( statement );
    }

    private void setUpMocks()
    {
        databaseQueryService = mock( GraphDatabaseQueryService.class );
        dependencyResolver = mock( DependencyResolver.class );
        statementContextBridge = mock( ThreadToStatementContextBridge.class );
        guard = mock( Guard.class );
        statement = mock( KernelStatement.class );
        propertyContainerLocker = mock( PropertyContainerLocker.class );
        transaction = new TopLevelTransaction( mock( KernelTransaction.class ), () -> statement );

        when( databaseQueryService.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class ) ).thenReturn( statementContextBridge );
        when( dependencyResolver.resolveDependency( Guard.class ) ).thenReturn( guard );
    }
}

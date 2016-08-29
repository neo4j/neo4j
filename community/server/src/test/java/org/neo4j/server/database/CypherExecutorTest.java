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
package org.neo4j.server.database;

import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;

import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.web.HttpHeaderUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CypherExecutorTest
{

    private final static long CUSTOM_TRANSACTION_TIMEOUT = 1000L;

    private Database database;
    private GraphDatabaseFacade databaseFacade;
    private DependencyResolver dependencyResolver;
    private QueryExecutionEngine executionEngine;
    private ThreadToStatementContextBridge statementBridge;
    private GraphDatabaseQueryService databaseQueryService;
    private KernelTransaction kernelTransaction;
    private Statement statement;
    private HttpServletRequest request;
    private AssertableLogProvider logProvider;

    @Before
    public void setUp()
    {
        setUpMocks();
        initLogProvider();
    }

    @Test
    public void startDefaultTransaction() throws Throwable
    {
        Config config = getGuardEnabledConfig();
        CypherExecutor cypherExecutor = new CypherExecutor( database, config, logProvider );
        cypherExecutor.start();

        cypherExecutor.createSession( request );

        verify( databaseQueryService ).beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void startTransactionWithCustomTimeout() throws Throwable
    {
        when( request.getHeader( HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER ) )
                .thenReturn( String.valueOf( CUSTOM_TRANSACTION_TIMEOUT ) );

        Config config = getGuardEnabledConfig();
        CypherExecutor cypherExecutor = new CypherExecutor( database, config, logProvider );
        cypherExecutor.start();

        cypherExecutor.createSession( request );

        verify( databaseQueryService ).beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL,
                CUSTOM_TRANSACTION_TIMEOUT );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void startDefaultTransactionWhenHeaderHasIncorrectValue() throws Throwable
    {
        when( request.getHeader( HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER ) )
                .thenReturn( "not a number" );

        Config config = getGuardEnabledConfig();
        CypherExecutor cypherExecutor = new CypherExecutor( database, config, logProvider );
        cypherExecutor.start();

        cypherExecutor.createSession( request );

        verify( databaseQueryService ).beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL );
        logProvider.assertContainsMessageContaining( "Fail to parse `max-execution-time` header with value: 'not a " +
                                                     "number'. Should be a positive number." );
    }

    @Test
    public void startDefaultTransactionIfTimeoutIsNegative() throws Throwable
    {
        when( request.getHeader( HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER ) )
                .thenReturn( "-2" );

        Config config = getGuardEnabledConfig();
        CypherExecutor cypherExecutor = new CypherExecutor( database, config, logProvider );
        cypherExecutor.start();

        cypherExecutor.createSession( request );

        verify( databaseQueryService ).beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void startDefaultTransactionIfExecutionGuardDisabled() throws Throwable
    {
        when( request.getHeader( HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER ) )
                .thenReturn( String.valueOf( CUSTOM_TRANSACTION_TIMEOUT ) );

        CypherExecutor cypherExecutor = new CypherExecutor( database, Config.defaults(), logProvider );
        cypherExecutor.start();

        cypherExecutor.createSession( request );

        verify( databaseQueryService ).beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL );
        logProvider.assertNoLoggingOccurred();
    }

    private void initLogProvider()
    {
        logProvider = new AssertableLogProvider( true );
    }

    private Config getGuardEnabledConfig()
    {
        return new Config( MapUtil.stringMap( GraphDatabaseSettings.execution_guard_enabled.name(), "true" ) );
    }

    private void setUpMocks()
    {
        database = mock( Database.class );
        databaseFacade = mock( GraphDatabaseFacade.class );
        dependencyResolver = mock( DependencyResolver.class );
        executionEngine = mock( ExecutionEngine.class );
        statementBridge = mock( ThreadToStatementContextBridge.class );
        databaseQueryService = mock( GraphDatabaseQueryService.class );
        kernelTransaction = mock( KernelTransaction.class );
        statement = mock( Statement.class );
        request = mock( HttpServletRequest.class );

        InternalTransaction transaction = new TopLevelTransaction( kernelTransaction, () -> statement );

        AccessMode.Static accessMode = AccessMode.Static.FULL;
        KernelTransaction.Type type = KernelTransaction.Type.implicit;
        when( kernelTransaction.mode() ).thenReturn( accessMode );
        when( kernelTransaction.transactionType() ).thenReturn( type  );
        when( database.getGraph() ).thenReturn( databaseFacade );
        when( databaseFacade.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( QueryExecutionEngine.class ) ).thenReturn( executionEngine );
        when( dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class ) ).thenReturn(
                statementBridge );
        when( executionEngine.queryService() ).thenReturn( databaseQueryService );
        when( databaseQueryService.beginTransaction( type, accessMode ) )
                .thenReturn( transaction );
        when( databaseQueryService.beginTransaction( type, accessMode,
                CUSTOM_TRANSACTION_TIMEOUT ) ).thenReturn( transaction );
        when( databaseQueryService.getDependencyResolver() ).thenReturn( dependencyResolver );
    }

}

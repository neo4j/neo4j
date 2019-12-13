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
package org.neo4j.server.http.cypher;

import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HttpTransactionManagerTest
{
    @Test
    void shouldSetupJobScheduler()
    {
        var managementService = mock( DatabaseManagementService.class );
        JobScheduler jobScheduler = mock( JobScheduler.class );
        AssertableLogProvider logProvider = new AssertableLogProvider( true );

        new HttpTransactionManager( managementService, jobScheduler, Clocks.systemClock(), Duration.ofMinutes( 1 ), logProvider );

        long runEvery = Math.round( Duration.ofMinutes( 1 ).toMillis() / 2.0 );
        verify( jobScheduler ).scheduleRecurring( eq( Group.SERVER_TRANSACTION_TIMEOUT ), any(), eq( runEvery ), eq( TimeUnit.MILLISECONDS ) );
    }

    @Test
    void shouldCreateTransactionHandleRegistry()
    {
        var managementService = mock( DatabaseManagementService.class );
        JobScheduler jobScheduler = mock( JobScheduler.class );
        AssertableLogProvider logProvider = new AssertableLogProvider( true );

        HttpTransactionManager manager =
                new HttpTransactionManager( managementService, jobScheduler, Clocks.systemClock(), Duration.ofMinutes( 1 ), logProvider );

        assertNotNull( manager.getTransactionHandleRegistry() );
    }

    @Test
    void shouldGetEmptyTransactionFacadeOfDatabaseData()
    {
        DatabaseManagementService managementService = mock( DatabaseManagementService.class );
        HttpTransactionManager manager = newTransactionManager( managementService );
        final Optional<GraphDatabaseFacade> graphDatabaseFacade = manager.getGraphDatabaseFacade( "data" );

        assertFalse( graphDatabaseFacade.isPresent() );

        verify( managementService ).database( "data" );
    }

    @Test
    void shouldGetTransactionFacadeOfDatabaseWithSpecifiedName()
    {
        DatabaseManagementService managementService = mock( DatabaseManagementService.class );
        HttpTransactionManager manager = newTransactionManager( managementService );
        Optional<GraphDatabaseFacade> transactionFacade = manager.getGraphDatabaseFacade( "neo4j" );

        assertTrue( transactionFacade.isPresent() );

        verify( managementService ).database( "neo4j" );
    }

    @Test
    void shouldGetEmptyTransactionFacadeForUnknownDatabase()
    {
        DatabaseManagementService managementService = mock( DatabaseManagementService.class );
        HttpTransactionManager manager = newTransactionManager( managementService );
        Optional<GraphDatabaseFacade> transactionFacade = manager.getGraphDatabaseFacade( "foo" );

        assertFalse( transactionFacade.isPresent() );

        verify( managementService ).database( "foo" );
    }

    private HttpTransactionManager newTransactionManager( DatabaseManagementService managementService )
    {
        JobScheduler jobScheduler = mock( JobScheduler.class );
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        var defaultDatabase = "neo4j";
        when( managementService.database( any( String.class ) ) ).thenAnswer( invocation -> {
            Object[] args = invocation.getArguments();
            String db = (String) args[0];

            if ( db.equals( defaultDatabase ) || db.equals( "system" ) )
            {
                return graphWithName( db );
            }
            else
            {
                throw new DatabaseNotFoundException( "Not found db named " + db );
            }

        } );
        return new HttpTransactionManager( managementService, jobScheduler, Clocks.systemClock(), Duration.ofMinutes( 1 ), logProvider );
    }

    private GraphDatabaseFacade graphWithName( String name )
    {
        GraphDatabaseFacade graph = mock( GraphDatabaseFacade.class );
        when( graph.databaseName() ).thenReturn( name );
        when( graph.getDependencyResolver() ).thenReturn( mock( DependencyResolver.class, Answers.RETURNS_SMART_NULLS ) );
        return graph;
    }
}

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
package org.neo4j.bolt.v4.runtime;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPIProvider;
import org.neo4j.bolt.txtracking.SimpleReconciledTransactionTracker;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.SystemNanoClock;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TransactionStateMachineSPIProviderV4Test
{
    @Test
    void shouldReturnTransactionStateMachineSPIIfDatabaseExists() throws Throwable
    {
        String databaseName = "database";
        DatabaseManagementService managementService = managementService( databaseName );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( managementService );

        TransactionStateMachineSPI spi = spiProvider.getTransactionStateMachineSPI( databaseName, mock( StatementProcessorReleaseManager.class ) );
        assertThat( spi, instanceOf( TransactionStateMachineV4SPI.class ) );
    }

    @Test
    void shouldReturnDefaultTransactionStateMachineSPIWithEmptyDatabasename() throws Throwable
    {
        String databaseName = "neo4j";
        DatabaseManagementService managementService = managementService( databaseName );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( managementService );

        TransactionStateMachineSPI spi = spiProvider.getTransactionStateMachineSPI( "", mock( StatementProcessorReleaseManager.class ) );
        assertThat( spi, instanceOf( TransactionStateMachineV4SPI.class ) );
    }

    @Test
    void shouldErrorIfDatabaseNotFound()
    {
        DatabaseManagementService managementService = mock( DatabaseManagementService.class );
        var databaseName = "database";
        when( managementService.database( databaseName ) ).thenThrow( new DatabaseNotFoundException( databaseName ) );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( managementService );

        BoltIOException error = assertThrows( BoltIOException.class, () ->
                spiProvider.getTransactionStateMachineSPI( databaseName, mock( StatementProcessorReleaseManager.class ) ) );
        assertThat( error.status(), equalTo( Status.Database.DatabaseNotFound ) );
        assertThat( error.getMessage(), containsString( "Database does not exist. Database name: 'database'." ) );
    }

    private DatabaseManagementService managementService( String databaseName )
    {
        DatabaseManagementService managementService = mock( DatabaseManagementService.class );
        GraphDatabaseFacade databaseFacade = mock( GraphDatabaseFacade.class );
        final DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );

        when( databaseFacade.isAvailable( anyLong() ) ).thenReturn( true );
        when( managementService.database( databaseName ) ).thenReturn( databaseFacade );
        when( databaseFacade.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( GraphDatabaseQueryService.class ) ).thenReturn( queryService );
        when( dependencyResolver.resolveDependency( Database.class ) ).thenReturn( mock( Database.class ) );
        when( queryService.getDependencyResolver() ).thenReturn( dependencyResolver );

        return managementService;
    }

    private TransactionStateMachineSPIProvider newSpiProvider( DatabaseManagementService managementService )
    {
        var clock = mock( SystemNanoClock.class );
        var reconciledTxTracker = new SimpleReconciledTransactionTracker( managementService, NullLogService.getInstance() );
        var dbProvider = new BoltKernelDatabaseManagementServiceProvider( managementService, reconciledTxTracker, new Monitors(), clock, Duration.ZERO );
        return new TransactionStateMachineSPIProviderV4( dbProvider, "neo4j", mock( BoltChannel.class ), clock );
    }
}

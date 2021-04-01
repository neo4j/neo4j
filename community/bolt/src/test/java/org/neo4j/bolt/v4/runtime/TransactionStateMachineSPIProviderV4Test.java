/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.bolt.dbapi.impl.BoltKernelGraphDatabaseServiceProvider;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPIProvider;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.SystemNanoClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class TransactionStateMachineSPIProviderV4Test
{
    private BoltChannel mockBoltChannel = mock( BoltChannel.class );

    @Test
    void shouldReturnTransactionStateMachineSPIIfDatabaseExists() throws Throwable
    {
        String databaseName = "database";
        DatabaseManagementService managementService = managementService( databaseName );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( managementService );

        TransactionStateMachineSPI spi = spiProvider.getTransactionStateMachineSPI( databaseName, mock( StatementProcessorReleaseManager.class ) );
        assertThat( spi ).isInstanceOf( TransactionStateMachineV4SPI.class );
    }

    @Test
    void shouldReturnDefaultTransactionStateMachineSPIWithEmptyDatabasename() throws Throwable
    {
        String databaseName = "neo4j";
        DatabaseManagementService managementService = managementService( databaseName );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( managementService );
        when( mockBoltChannel.defaultDatabase() ).thenReturn( "neo4j" );

        TransactionStateMachineSPI spi = spiProvider.getTransactionStateMachineSPI( "", mock( StatementProcessorReleaseManager.class, RETURNS_MOCKS ) );
        assertThat( spi ).isInstanceOf( TransactionStateMachineV4SPI.class );
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
        assertThat( error.status() ).isEqualTo( Status.Database.DatabaseNotFound );
        assertThat( error.getMessage() ).contains( "Database does not exist. Database name: 'database'." );
    }

    @Test
    void shouldAllocateMemoryForTransactionStateMachineSPI() throws BoltProtocolBreachFatality, BoltIOException
    {
        String databaseName = "neo4j";
        var clock = mock( SystemNanoClock.class );

        DatabaseManagementService managementService = managementService( databaseName );
        var memoryTracker = mock( MemoryTracker.class );
        var scopedMemoryTracker = mock( MemoryTracker.class, RETURNS_MOCKS );

        when( mockBoltChannel.defaultDatabase() ).thenReturn( "neo4j" );
        when( memoryTracker.getScopedMemoryTracker() ).thenReturn( scopedMemoryTracker );

        var dbProvider = new BoltKernelDatabaseManagementServiceProvider( managementService, new Monitors(), clock, Duration.ZERO );
        var spiProvider = new TransactionStateMachineSPIProviderV4( dbProvider, mockBoltChannel, clock, memoryTracker );

        spiProvider.getTransactionStateMachineSPI( "", mock( StatementProcessorReleaseManager.class ) );

        verify( memoryTracker ).getScopedMemoryTracker();
        verify( scopedMemoryTracker ).allocateHeap( TransactionStateMachineV4SPI.SHALLOW_SIZE );
        verify( scopedMemoryTracker ).allocateHeap( BoltKernelGraphDatabaseServiceProvider.SHALLOW_SIZE );
        verify( scopedMemoryTracker ).getScopedMemoryTracker();
        verifyNoMoreInteractions( memoryTracker );
        verifyNoMoreInteractions( scopedMemoryTracker );
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
        var dbProvider = new BoltKernelDatabaseManagementServiceProvider( managementService, new Monitors(), clock, Duration.ZERO );
        return new TransactionStateMachineSPIProviderV4( dbProvider, mockBoltChannel, clock, mock( MemoryTracker.class, RETURNS_MOCKS ) );
    }
}

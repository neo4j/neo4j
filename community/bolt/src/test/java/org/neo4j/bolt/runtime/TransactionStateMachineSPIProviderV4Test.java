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
package org.neo4j.bolt.runtime;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.v1.runtime.StatementProcessorReleaseManager;
import org.neo4j.bolt.v4.runtime.TransactionStateMachineV4SPI;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseId;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TransactionStateMachineSPIProviderV4Test
{
    @Test
    void shouldReturnTransactionStateMachineSPIIfDatabaseExists() throws Throwable
    {
        DatabaseId databaseId = new DatabaseId( "database" );
        DatabaseManager<?> databaseManager = databaseManager( databaseId );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( databaseManager );

        TransactionStateMachineSPI spi = spiProvider.getTransactionStateMachineSPI( databaseId, mock( StatementProcessorReleaseManager.class ) );
        assertThat( spi, instanceOf( TransactionStateMachineV4SPI.class ) );
    }

    @Test
    void shouldReturnDefaultTransactionStateMachineSPIWithEmptyDatabasename() throws Throwable
    {
        DatabaseId databaseId = new DatabaseId( "neo4j" );
        DatabaseManager<?> databaseManager = databaseManager( databaseId );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( databaseManager );

        TransactionStateMachineSPI spi = spiProvider.getTransactionStateMachineSPI( new DatabaseId( "" ), mock( StatementProcessorReleaseManager.class ) );
        assertThat( spi, instanceOf( TransactionStateMachineV4SPI.class ) );
    }

    @Test
    void shouldErrorIfDatabaseNotFound() throws Throwable
    {
        DatabaseManager<?> databaseManager = mock( DatabaseManager.class );
        var databaseId = new DatabaseId( "database" );
        when( databaseManager.getDatabaseContext( databaseId ) ).thenReturn( Optional.empty() );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( databaseManager );

        BoltIOException error = assertThrows( BoltIOException.class, () ->
                spiProvider.getTransactionStateMachineSPI( databaseId, mock( StatementProcessorReleaseManager.class ) ) );
        assertThat( error.status(), equalTo( Status.Database.DatabaseNotFound ) );
        assertThat( error.getMessage(), containsString( "The database requested does not exist. Requested database name: 'database'." ) );
    }

    private DatabaseManager<StandaloneDatabaseContext> databaseManager( DatabaseId databaseId )
    {
        @SuppressWarnings( "unchecked" )
        DatabaseManager<StandaloneDatabaseContext> databaseManager = mock( DatabaseManager.class );
        StandaloneDatabaseContext databaseContext = mock( StandaloneDatabaseContext.class );
        Dependencies dependencies = mock( Dependencies.class );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );

        when( databaseManager.getDatabaseContext( databaseId ) ).thenReturn( Optional.of( databaseContext ) );
        when( databaseContext.dependencies() ).thenReturn( dependencies );
        when( dependencies.resolveDependency( GraphDatabaseQueryService.class ) ).thenReturn( queryService );
        when( queryService.getDependencyResolver() ).thenReturn( mock( DependencyResolver.class ) );

        return databaseManager;
    }

    private TransactionStateMachineSPIProvider newSpiProvider( DatabaseManager<?> databaseManager )
    {
        return new TransactionStateMachineSPIProviderV4( databaseManager, new DatabaseId( "neo4j" ),
                mock( BoltChannel.class ), Duration.ZERO, mock( Clock.class ) );
    }
}

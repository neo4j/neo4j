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
import org.neo4j.bolt.v1.runtime.TransactionStateMachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.v4.runtime.TransactionStateMachineV4SPI;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.Dependencies;

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
        DatabaseManager databaseManager = databaseManager( "database" );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( databaseManager );

        TransactionStateMachineSPI spi = spiProvider.getTransactionStateMachineSPI( "database", mock( StatementProcessorReleaseManager.class ) );
        assertThat( spi, instanceOf( TransactionStateMachineV4SPI.class ) );
    }

    @Test
    void shouldReturnDefaultTransactionStateMachineSPIWithEmptyDatabasename() throws Throwable
    {
        DatabaseManager databaseManager = databaseManager( "neo4j" );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( databaseManager );

        TransactionStateMachineSPI spi = spiProvider.getTransactionStateMachineSPI( "", mock( StatementProcessorReleaseManager.class ) );
        assertThat( spi, instanceOf( TransactionStateMachineV4SPI.class ) );
    }

    @Test
    void shouldErrorIfDatabaseNotFound() throws Throwable
    {
        DatabaseManager databaseManager = mock( DatabaseManager.class );
        when( databaseManager.getDatabaseContext( "database" ) ).thenReturn( Optional.empty() );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( databaseManager );

        BoltIOException error = assertThrows( BoltIOException.class, () ->
                spiProvider.getTransactionStateMachineSPI( "database", mock( StatementProcessorReleaseManager.class ) ) );
        assertThat( error.status(), equalTo( Status.Request.Invalid ) );
        assertThat( error.getMessage(), containsString( "The database requested does not exists. Requested database name: 'database'." ) );
    }

    private DatabaseManager databaseManager( String databaseName )
    {
        DatabaseManager databaseManager = mock( DatabaseManager.class );
        DatabaseContext databaseContext = mock( DatabaseContext.class );
        Dependencies dependencies = mock( Dependencies.class );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );

        when( databaseManager.getDatabaseContext( databaseName ) ).thenReturn( Optional.of( databaseContext ) );
        when( databaseContext.getDependencies() ).thenReturn( dependencies );
        when( dependencies.resolveDependency( GraphDatabaseQueryService.class ) ).thenReturn( queryService );
        when( queryService.getDependencyResolver() ).thenReturn( mock( DependencyResolver.class ) );

        return databaseManager;
    }

    private TransactionStateMachineSPIProvider newSpiProvider( DatabaseManager databaseManager )
    {
        return new TransactionStateMachineSPIProviderV4( databaseManager, "neo4j",
                mock( BoltChannel.class ), Duration.ZERO, mock( Clock.class ) );
    }
}

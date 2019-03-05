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
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultDatabaseTransactionStateMachineSPIProviderTest
{
    @Test
    void shouldReturnDefaultTransactionStateMachineSPIWithEmptyDatabaseName() throws Throwable
    {
        DatabaseManager databaseManager = databaseManagerWithDatabase( "neo4j" );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( databaseManager );

        TransactionStateMachineSPI spi = spiProvider.getTransactionStateMachineSPI( "" );
        assertThat( spi, instanceOf( TransactionStateMachineSPI.class ) );
    }

    @Test
    void shouldErrorIfDatabaseNotFound() throws Throwable
    {
        DatabaseManager databaseManager = databaseManagerWithDatabase( "database" );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( databaseManager );

        BoltProtocolBreachFatality error = assertThrows( BoltProtocolBreachFatality.class, () -> spiProvider.getTransactionStateMachineSPI( "database" ) );
        assertThat( error.getMessage(), containsString( "Database selection by name not supported by Bolt protocol version lower than BoltV4." ) );
    }

    private DatabaseManager databaseManagerWithDatabase( String databaseName )
    {
        DatabaseManager databaseManager = mock( DatabaseManager.class );
        DatabaseContext databaseContext = mock( DatabaseContext.class );
        when( databaseManager.getDatabaseContext( databaseName ) ).thenReturn( Optional.of( databaseContext ) );
        return databaseManager;
    }

    private TransactionStateMachineSPIProvider newSpiProvider( DatabaseManager databaseManager )
    {
        return new TransactionStateMachineSPIProvider.DefaultDatabaseTransactionStatementSPIProvider( databaseManager, "neo4j", mock( BoltChannel.class ),
                Duration.ZERO, mock( Clock.class ) )
        {
            @Override
            protected TransactionStateMachineSPI newTransactionStateMachineSPI( DatabaseContext activeDatabase )
            {
                return mock( TransactionStateMachineSPI.class );
            }
        };
    }
}

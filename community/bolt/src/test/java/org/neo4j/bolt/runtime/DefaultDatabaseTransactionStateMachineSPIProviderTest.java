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

import java.time.Duration;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.v1.runtime.StatementProcessorReleaseManager;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.time.SystemNanoClock;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;

class DefaultDatabaseTransactionStateMachineSPIProviderTest
{
    @Test
    void shouldReturnDefaultTransactionStateMachineSPIWithEmptyDatabaseName() throws Throwable
    {
        DatabaseManagementService managementService = managementServiceWithDatabase( "neo4j" );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( managementService );

        TransactionStateMachineSPI spi = spiProvider.getTransactionStateMachineSPI( ABSENT_DB_NAME, mock( StatementProcessorReleaseManager.class ) );
        assertThat( spi, instanceOf( TransactionStateMachineSPI.class ) );
    }

    @Test
    void shouldErrorIfDatabaseNotFound()
    {
        DatabaseManagementService managementService = managementServiceWithDatabase( "database" );
        TransactionStateMachineSPIProvider spiProvider = newSpiProvider( managementService );

        BoltProtocolBreachFatality error = assertThrows( BoltProtocolBreachFatality.class, () ->
                spiProvider.getTransactionStateMachineSPI( "database", mock( StatementProcessorReleaseManager.class ) ) );
        assertThat( error.getMessage(), containsString( "Database selection by name not supported by Bolt protocol version lower than BoltV4." ) );
    }

    private DatabaseManagementService managementServiceWithDatabase( String databaseName )
    {
        DatabaseManagementService managementService = mock( DatabaseManagementService.class );
        GraphDatabaseFacade databaseContext = mock( GraphDatabaseFacade.class );
        when( managementService.database( databaseName ) ).thenReturn( databaseContext );
        return managementService;
    }

    private TransactionStateMachineSPIProvider newSpiProvider( DatabaseManagementService managementService )
    {
        return new AbstractTransactionStatementSPIProvider( managementService, "neo4j", mock( BoltChannel.class ),
                Duration.ZERO, mock( SystemNanoClock.class ) )
        {
            @Override
            protected TransactionStateMachineSPI newTransactionStateMachineSPI( GraphDatabaseFacade activeDatabase,
                    StatementProcessorReleaseManager resourceReleaseManger )
            {
                return mock( TransactionStateMachineSPI.class );
            }
        };
    }
}

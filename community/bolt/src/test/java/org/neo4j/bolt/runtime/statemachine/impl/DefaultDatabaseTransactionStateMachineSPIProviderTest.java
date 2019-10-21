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
package org.neo4j.bolt.runtime.statemachine.impl;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPIProvider;
import org.neo4j.bolt.txtracking.SimpleReconciledTransactionTracker;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.SystemNanoClock;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
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
        GraphDatabaseFacade databaseFacade = mock( GraphDatabaseFacade.class );
        when( databaseFacade.isAvailable( anyLong() ) ).thenReturn( true );
        when( managementService.database( databaseName ) ).thenReturn( databaseFacade );
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        when( databaseFacade.getDependencyResolver() ).thenReturn( dependencyResolver );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        when( dependencyResolver.resolveDependency( GraphDatabaseQueryService.class ) ).thenReturn( queryService );
        when( queryService.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( Database.class ) ).thenReturn( mock( Database.class ) );

        return managementService;
    }

    private TransactionStateMachineSPIProvider newSpiProvider( DatabaseManagementService managementService )
    {
        var clock = mock( SystemNanoClock.class );
        var reconciledTxTracker = new SimpleReconciledTransactionTracker( managementService, NullLogService.getInstance() );
        var dbProvider = new BoltKernelDatabaseManagementServiceProvider( managementService, reconciledTxTracker, new Monitors(), clock, Duration.ZERO );
        return new AbstractTransactionStatementSPIProvider( dbProvider, "neo4j", mock( BoltChannel.class ), clock )
        {
            @Override
            protected TransactionStateMachineSPI newTransactionStateMachineSPI( BoltGraphDatabaseServiceSPI activeBoltGraphDatabaseServiceSPI,
                    StatementProcessorReleaseManager resourceReleaseManger )
            {
                return mock( TransactionStateMachineSPI.class );
            }
        };
    }
}

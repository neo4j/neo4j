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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.testing.BoltTestUtil;
import org.neo4j.bolt.txtracking.DefaultReconciledTransactionTracker;
import org.neo4j.bolt.v3.BoltStateMachineV3;
import org.neo4j.bolt.v4.BoltStateMachineV4;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BoltStateMachineFactoryImplTest
{
    private static final String CUSTOM_DB_NAME = "customDbName";
    private static final SystemNanoClock CLOCK = Clocks.nanoClock();
    private static final BoltChannel CHANNEL = BoltTestUtil.newTestBoltChannel();

    @Test
    void shouldCreateBoltStateMachinesV3()
    {
        BoltStateMachineFactoryImpl factory = newBoltFactory();

        BoltStateMachine boltStateMachine = factory.newStateMachine( 3L, CHANNEL );

        assertNotNull( boltStateMachine );
        assertThat( boltStateMachine, instanceOf( BoltStateMachineV3.class ) );
    }

    @Test
    void shouldCreateBoltStateMachinesV4()
    {
        BoltStateMachineFactoryImpl factory = newBoltFactory();

        BoltStateMachine boltStateMachine = factory.newStateMachine( 4L, CHANNEL );

        assertNotNull( boltStateMachine );
        assertThat( boltStateMachine, instanceOf( BoltStateMachineV4.class ) );
    }

    @ParameterizedTest( name = "V{0}" )
    @ValueSource( longs = {999, -1, 1, 2} )
    void shouldThrowExceptionIfVersionIsUnknown( long protocolVersion )
    {
        BoltStateMachineFactoryImpl factory = newBoltFactory();

        IllegalArgumentException error = assertThrows( IllegalArgumentException.class, () -> factory.newStateMachine( protocolVersion, CHANNEL ) );
        assertThat( error.getMessage(), startsWith( "Failed to create a state machine for protocol version" ) );
    }

    private static BoltStateMachineFactoryImpl newBoltFactory()
    {
        return newBoltFactory( newDbMock() );
    }

    private static BoltStateMachineFactoryImpl newBoltFactory( DatabaseManagementService managementService )
    {
        var config = Config.defaults( GraphDatabaseSettings.default_database, CUSTOM_DB_NAME );
        var reconciledTxTracker = new DefaultReconciledTransactionTracker( NullLogService.getInstance() );
        var dbProvider = new BoltKernelDatabaseManagementServiceProvider( managementService, reconciledTxTracker, new Monitors(), CLOCK, Duration.ZERO );
        return new BoltStateMachineFactoryImpl( dbProvider, mock( Authentication.class ), CLOCK, config, NullLogService.getInstance() );
    }

    private static DatabaseManagementService newDbMock()
    {
        GraphDatabaseFacade db = mock( GraphDatabaseFacade.class );
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        when( db.getDependencyResolver() ).thenReturn( dependencyResolver );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        when( queryService.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( GraphDatabaseQueryService.class ) ).thenReturn( queryService );
        DatabaseManagementService managementService = mock( DatabaseManagementService.class );
        when( managementService.database( CUSTOM_DB_NAME ) ).thenReturn( db );
        return managementService;
    }
}

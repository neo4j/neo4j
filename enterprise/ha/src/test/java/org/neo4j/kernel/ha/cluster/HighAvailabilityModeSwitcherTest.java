/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha.cluster;

import java.net.URI;

import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.BindingNotifier;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.util.Monitors;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class HighAvailabilityModeSwitcherTest
{
    @Test
    public void shouldBroadcastMasterIsAvailableIfMasterAndReceiveMasterIsElected() throws Exception
    {
        // Given
        ClusterMemberAvailability availability = mock( ClusterMemberAvailability.class );
        HighAvailabilityModeSwitcher toTest = new HighAvailabilityModeSwitcher( mock( BindingNotifier.class ),
                mock( DelegateInvocationHandler.class ), availability,
                mock( HighAvailabilityMemberStateMachine.class),  mock( GraphDatabaseAPI.class ),
                mock( HaIdGeneratorFactory.class ), mock( Config.class ), mock( Logging.class ), mock(
                UpdateableSchemaState.class), Iterables.<KernelExtensionFactory<?>>empty(), new Monitors( StringLogger.DEV_NULL) );

        // When
        toTest.masterIsElected( new HighAvailabilityMemberChangeEvent( HighAvailabilityMemberState.MASTER,
                HighAvailabilityMemberState.MASTER, new InstanceId( 2 ), URI.create( "ha://someone" ) ) );

        // Then
          /*
           * The second argument to memberIsAvailable below is null because it has not been set yet. This would require
           * a switch to master which we don't do here.
           */
        verify( availability, times( 1 ) ).memberIsAvailable( HighAvailabilityModeSwitcher.MASTER, null );
    }

    @Test
    public void shouldBroadcastSlaveIsAvailableIfSlaveAndReceivesMasterIsAvailable() throws Exception
    {

        // Given
        ClusterMemberAvailability availability = mock( ClusterMemberAvailability.class );
        HighAvailabilityModeSwitcher toTest = new HighAvailabilityModeSwitcher( mock( BindingNotifier.class ),
                mock( DelegateInvocationHandler.class ), availability,
                mock( HighAvailabilityMemberStateMachine.class),  mock( GraphDatabaseAPI.class ),
                mock( HaIdGeneratorFactory.class ), mock( Config.class ), mock( Logging.class ), mock(
                UpdateableSchemaState.class), Iterables.<KernelExtensionFactory<?>>empty(), new Monitors( StringLogger.DEV_NULL) );

        // When
        toTest.masterIsAvailable( new HighAvailabilityMemberChangeEvent( HighAvailabilityMemberState.SLAVE,
                HighAvailabilityMemberState.SLAVE, new InstanceId( 2 ), URI.create( "ha://someone" ) ) );

        // Then
          /*
           * The second argument to memberIsAvailable below is null because it has not been set yet. This would require
           * a switch to master which we don't do here.
           */
        verify( availability, times( 1 ) ).memberIsAvailable( HighAvailabilityModeSwitcher.SLAVE, null );
    }

    @Test
    public void shouldNotBroadcastIfSlaveAndReceivesMasterIsElected() throws Exception
    {

        // Given
        ClusterMemberAvailability availability = mock( ClusterMemberAvailability.class );
        HighAvailabilityModeSwitcher toTest = new HighAvailabilityModeSwitcher( mock( BindingNotifier.class ),
                mock( DelegateInvocationHandler.class ), availability,
                mock( HighAvailabilityMemberStateMachine.class),  mock( GraphDatabaseAPI.class ),
                mock( HaIdGeneratorFactory.class ), mock( Config.class ), mock( Logging.class ), mock(
                UpdateableSchemaState.class), Iterables.<KernelExtensionFactory<?>>empty(), new Monitors( StringLogger.DEV_NULL) );

        // When
        toTest.masterIsElected( new HighAvailabilityMemberChangeEvent( HighAvailabilityMemberState.SLAVE,
                HighAvailabilityMemberState.SLAVE, new InstanceId( 2 ), URI.create( "ha://someone" ) ) );

        // Then
          /*
           * The second argument to memberIsAvailable below is null because it has not been set yet. This would require
           * a switch to master which we don't do here.
           */
        verifyZeroInteractions(  availability );
    }

    @Test
    public void shouldNotBroadcastIfMasterAndReceivesSlaveIsAvailable() throws Exception
    {

        // Given
        ClusterMemberAvailability availability = mock( ClusterMemberAvailability.class );
        HighAvailabilityModeSwitcher toTest = new HighAvailabilityModeSwitcher( mock( BindingNotifier.class ),
                mock( DelegateInvocationHandler.class ), availability,
                mock( HighAvailabilityMemberStateMachine.class),  mock( GraphDatabaseAPI.class ),
                mock( HaIdGeneratorFactory.class ), mock( Config.class ), mock( Logging.class ), mock(
                UpdateableSchemaState.class), Iterables.<KernelExtensionFactory<?>>empty(), new Monitors( StringLogger.DEV_NULL) );

        // When
        toTest.slaveIsAvailable( new HighAvailabilityMemberChangeEvent( HighAvailabilityMemberState.MASTER,
                HighAvailabilityMemberState.MASTER, new InstanceId( 2 ), URI.create( "ha://someone" ) ) );

        // Then
          /*
           * The second argument to memberIsAvailable below is null because it has not been set yet. This would require
           * a switch to master which we don't do here.
           */
        verifyZeroInteractions(  availability );
    }

}

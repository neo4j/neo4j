/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.PENDING;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.TO_SLAVE;

public class HighAvailabilityModeSwitcherTest
{
    @Test
    public void shouldBroadcastMasterIsAvailableIfMasterAndReceiveMasterIsElected() throws Exception
    {
        // Given
        ClusterMemberAvailability availability = mock( ClusterMemberAvailability.class );
        HighAvailabilityModeSwitcher toTest = new HighAvailabilityModeSwitcher( mock( SwitchToSlave.class ),
                mock(SwitchToMaster.class),
                mock( Election.class ),
                availability,
                StringLogger.DEV_NULL );

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
        HighAvailabilityModeSwitcher toTest = new HighAvailabilityModeSwitcher( mock( SwitchToSlave.class ),
                mock(SwitchToMaster.class),
                mock( Election.class ),
                availability,
                StringLogger.DEV_NULL );

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
        HighAvailabilityModeSwitcher toTest = new HighAvailabilityModeSwitcher( mock( SwitchToSlave.class ),
                mock(SwitchToMaster.class),
                mock( Election.class ),
                availability,
                StringLogger.DEV_NULL );

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
        HighAvailabilityModeSwitcher toTest = new HighAvailabilityModeSwitcher( mock( SwitchToSlave.class ),
                mock(SwitchToMaster.class),
                mock( Election.class ),
                availability,
                StringLogger.DEV_NULL );

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

    @Test
    public void shouldReswitchToSlaveIfNewMasterBecameAvailableDuringSwitch() throws Throwable
    {
        // Given
        final CountDownLatch switching = new CountDownLatch( 1 );
        final CountDownLatch latch = new CountDownLatch( 1 );
        final CountDownLatch slaveAvailable = new CountDownLatch( 2 );
        ClusterMemberAvailability availability = mock( ClusterMemberAvailability.class );
        SwitchToSlave switchToSlave = mock( SwitchToSlave.class );
        SwitchToMaster switchToMaster = mock( SwitchToMaster.class );

        when( switchToSlave.switchToSlave(any( LifeSupport.class), any(URI.class), any(URI.class))).thenAnswer( new Answer<URI>()
                {
                    @Override
                    public URI answer( InvocationOnMock invocationOnMock ) throws Throwable
                    {
                        switching.countDown();
                        latch.await();
                        slaveAvailable.countDown();
                        return URI.create("ha://slave");
                    }
                } );

        Logging logging = mock( Logging.class );
        doReturn( new ConsoleLogger( StringLogger.DEV_NULL) ).when( logging ).getConsoleLog( HighAvailabilityModeSwitcher.class );

        HighAvailabilityModeSwitcher toTest = new HighAvailabilityModeSwitcher( switchToSlave,
                switchToMaster,
                mock( Election.class ),
                availability,
                StringLogger.DEV_NULL );
        toTest.init();
        toTest.start();

        // When
        toTest.masterIsAvailable( new HighAvailabilityMemberChangeEvent( PENDING,
                TO_SLAVE, new InstanceId( 1 ), URI.create( "ha://server1" ) ) );
        switching.await();
        toTest.masterIsElected( new HighAvailabilityMemberChangeEvent( TO_SLAVE, PENDING, new InstanceId( 2 ),
                URI.create( "ha://server2" ) ) );
        toTest.masterIsAvailable( new HighAvailabilityMemberChangeEvent( PENDING, TO_SLAVE, new InstanceId( 2 ), URI.create( "ha://server2" ) ) );
        latch.countDown();

        // Then
        slaveAvailable.await();
    }

}

/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
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
                dependencyResolverMock(),
                new DevNullLoggingService() );

        // When
        toTest.masterIsElected( new HighAvailabilityMemberChangeEvent( HighAvailabilityMemberState.MASTER,
                HighAvailabilityMemberState.MASTER, new InstanceId( 2 ), URI.create( "ha://someone" ) ) );

        // Then
          /*
           * The second argument to memberIsAvailable below is null because it has not been set yet. This would require
           * a switch to master which we don't do here.
           */
        verify( availability ).memberIsAvailable( HighAvailabilityModeSwitcher.MASTER, null, StoreId.DEFAULT );
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
                dependencyResolverMock(),
                new DevNullLoggingService() );

        // When
        toTest.masterIsAvailable( new HighAvailabilityMemberChangeEvent( HighAvailabilityMemberState.SLAVE,
                HighAvailabilityMemberState.SLAVE, new InstanceId( 2 ), URI.create( "ha://someone" ) ) );

        // Then
          /*
           * The second argument to memberIsAvailable below is null because it has not been set yet. This would require
           * a switch to master which we don't do here.
           */
        verify( availability ).memberIsAvailable( HighAvailabilityModeSwitcher.SLAVE, null, StoreId.DEFAULT );
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
                dependencyResolverMock(),
                new DevNullLoggingService() );

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
                dependencyResolverMock(),
                new DevNullLoggingService() );

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
        final CountDownLatch slaveAvailable = new CountDownLatch( 2 );
        final AtomicBoolean firstSwitch = new AtomicBoolean( true );
        ClusterMemberAvailability availability = mock( ClusterMemberAvailability.class );
        SwitchToSlave switchToSlave = mock( SwitchToSlave.class );
        SwitchToMaster switchToMaster = mock( SwitchToMaster.class );

        when( switchToSlave.switchToSlave( any( LifeSupport.class ), any( URI.class ), any( URI.class ),
                any( CancellationRequest.class ) ) ).thenAnswer( new Answer<URI>()
        {
            @Override
            public URI answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                        switching.countDown();
                        CancellationRequest cancel = (CancellationRequest) invocationOnMock.getArguments()[3];
                        if ( firstSwitch.get() )
                        {
                            while ( !cancel.cancellationRequested() )
                            {
                                Thread.sleep( 1 );
                            }
                            firstSwitch.set( false );
                        }
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
                dependencyResolverMock(),
                new DevNullLoggingService() );
        toTest.init();
        toTest.start();
        toTest.listeningAt( URI.create("ha://server3?serverId=3") );

        // When
        // This will start a switch to slave
        toTest.masterIsAvailable( new HighAvailabilityMemberChangeEvent( PENDING,
                TO_SLAVE, new InstanceId( 1 ), URI.create( "ha://server1" ) ) );
        // Wait until it starts and blocks on the cancellation request
        switching.await();
        // change the elected master, moving to pending, cancelling the previous change. This will block until the
        // previous switch is aborted
        toTest.masterIsElected( new HighAvailabilityMemberChangeEvent( TO_SLAVE, PENDING, new InstanceId( 2 ),
                URI.create( "ha://server2" ) ) );
        // Now move to the new master by switching to TO_SLAVE
        toTest.masterIsAvailable( new HighAvailabilityMemberChangeEvent( PENDING, TO_SLAVE, new InstanceId( 2 ), URI.create( "ha://server2" ) ) );

        // Then
        // The second switch must happen and this test won't block
        slaveAvailable.await();
    }

    @Test
    public void shouldTakeNoActionIfSwitchingToSlaveForItselfAsMaster() throws Throwable
    {
        // Given
            // A HAMS
        SwitchToSlave switchToSlave = mock( SwitchToSlave.class );
        Logging logging = mock( Logging.class );
        StringLogger msgLog = mock( StringLogger.class );
        when( logging.getMessagesLog( HighAvailabilityModeSwitcher.class ) ).thenReturn( msgLog );
        HighAvailabilityModeSwitcher toTest = new HighAvailabilityModeSwitcher( switchToSlave,
                mock( SwitchToMaster.class ), mock( Election.class ), mock( ClusterMemberAvailability.class ),
                dependencyResolverMock(), logging );
            // That is properly started
        toTest.init();
        toTest.start();
            // And listens to id 2
        URI serverHaUri = URI.create( "ha://server2?serverId=2" );
        toTest.listeningAt( serverHaUri );

        // When
            // The HAMS tries to switch to slave for a master that is itself
        toTest.masterIsAvailable( new HighAvailabilityMemberChangeEvent( PENDING, TO_SLAVE, new InstanceId( 2 ), serverHaUri ) );

        // Then
            // No switching to slave must happen
        verifyZeroInteractions( switchToSlave );
            // And an error must be logged
        verify( msgLog, times( 1 ) ).error( anyString() );
    }

    @Test
    public void shouldPerformForcedElections()
    {
        // Given
        ClusterMemberAvailability memberAvailability = mock( ClusterMemberAvailability.class );
        Election election = mock( Election.class );

        HighAvailabilityModeSwitcher modeSwitcher = new HighAvailabilityModeSwitcher( mock( SwitchToSlave.class ),
                mock( SwitchToMaster.class ), election, memberAvailability, dependencyResolverMock(),
                new DevNullLoggingService() );

        // When
        modeSwitcher.forceElections();

        // Then
        InOrder inOrder = inOrder( memberAvailability, election );
        inOrder.verify( memberAvailability ).memberIsUnavailable( HighAvailabilityModeSwitcher.SLAVE );
        inOrder.verify( election ).performRoleElections();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldPerformForcedElectionsOnlyOnce()
    {
        // Given: HAMS
        ClusterMemberAvailability memberAvailability = mock( ClusterMemberAvailability.class );
        Election election = mock( Election.class );

        HighAvailabilityModeSwitcher modeSwitcher = new HighAvailabilityModeSwitcher( mock( SwitchToSlave.class ),
                mock( SwitchToMaster.class ), election, memberAvailability, dependencyResolverMock(),
                new DevNullLoggingService() );

        // When: reelections are forced multiple times
        modeSwitcher.forceElections();
        modeSwitcher.forceElections();
        modeSwitcher.forceElections();

        // Then: instance sens out memberIsUnavailable and asks for elections and does this only once
        InOrder inOrder = inOrder( memberAvailability, election );
        inOrder.verify( memberAvailability ).memberIsUnavailable( HighAvailabilityModeSwitcher.SLAVE );
        inOrder.verify( election ).performRoleElections();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldAllowForcedElectionsAfterModeSwitch() throws Throwable
    {
        // Given
        SwitchToSlave switchToSlave = mock( SwitchToSlave.class );
        when( switchToSlave.switchToSlave( any( LifeSupport.class ), any( URI.class ), any( URI.class ),
                any( CancellationRequest.class ) ) ).thenReturn( URI.create( "http://localhost" ) );
        ClusterMemberAvailability memberAvailability = mock( ClusterMemberAvailability.class );
        Election election = mock( Election.class );

        final CountDownLatch modeSwitchHappened = new CountDownLatch( 1 );

        HighAvailabilityModeSwitcher modeSwitcher = new HighAvailabilityModeSwitcher( switchToSlave,
                mock( SwitchToMaster.class ), election, memberAvailability, dependencyResolverMock(),
                new DevNullLoggingService() )
        {
            @Override
            ScheduledExecutorService createExecutor()
            {
                ScheduledExecutorService executor = mock( ScheduledExecutorService.class );

                doAnswer( new Answer()
                {
                    @Override
                    public Object answer( InvocationOnMock invocation ) throws Throwable
                    {
                        ((Runnable) invocation.getArguments()[0]).run();
                        modeSwitchHappened.countDown();
                        return mock( Future.class );
                    }
                } ).when( executor ).submit( any( Runnable.class ) );

                return executor;
            }
        };

        modeSwitcher.init();
        modeSwitcher.start();

        modeSwitcher.forceElections();
        reset( memberAvailability, election );

        // When
        modeSwitcher.masterIsAvailable( new HighAvailabilityMemberChangeEvent( PENDING, TO_SLAVE, new InstanceId( 1 ),
                URI.create( "http://localhost:9090?serverId=42" ) ) );
        modeSwitchHappened.await();
        modeSwitcher.forceElections();

        // Then
        InOrder inOrder = inOrder( memberAvailability, election );
        inOrder.verify( memberAvailability ).memberIsUnavailable( HighAvailabilityModeSwitcher.SLAVE );
        inOrder.verify( election ).performRoleElections();
        inOrder.verifyNoMoreInteractions();
    }

    private static DependencyResolver dependencyResolverMock()
    {
        DependencyResolver resolver = mock( DependencyResolver.class );
        when( resolver.resolveDependency( eq( StoreId.class ) ) ).thenReturn( StoreId.DEFAULT );
        return resolver;
    }
}

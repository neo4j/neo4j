/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.com.ComException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
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
        verifyZeroInteractions( availability );
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
    public void shouldReswitchToSlaveIfNewMasterBecameElectedAndAvailableDuringSwitch() throws Throwable
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
    public void shouldRecognizeNewMasterIfNewMasterBecameAvailableDuringSwitch() throws Throwable
    {
        // When messages coming in the following ordering, the slave should detect that the master id has changed
        // M1: Get masterIsAvailable for instance 1 at PENDING state, changing PENDING -> TO_SLAVE
        // M2: Get masterIsAvailable for instance 2 at TO_SLAVE state, changing TO_SLAVE -> TO_SLAVE

        System.gc();
        // Given
        final CountDownLatch firstMasterAvailableHandled = new CountDownLatch( 1 );
        final CountDownLatch secondMasterAvailableComes = new CountDownLatch( 1 );
        final CountDownLatch secondMasterAvailableHandled = new CountDownLatch( 1 );

        SwitchToSlave switchToSlave = mock( SwitchToSlave.class );

        Logging mock = mock( Logging.class );
        when( mock.getMessagesLog( any( Class.class ) ) ).thenReturn( mock( StringLogger.class ) );
        when( mock.getConsoleLog( any( Class.class ) ) ).thenReturn( mock( ConsoleLogger.class ) );
        HighAvailabilityModeSwitcher toTest = new HighAvailabilityModeSwitcher( switchToSlave,
                mock( SwitchToMaster.class ), mock( Election.class ), mock( ClusterMemberAvailability.class ),
                mock( DependencyResolver.class ), mock )
        {
            @Override
            ScheduledExecutorService createExecutor()
            {
                final ScheduledExecutorService executor = mock( ScheduledExecutorService.class );
                final ExecutorService realExecutor = Executors.newSingleThreadExecutor();

                when( executor.submit( any( Runnable.class ) ) ).thenAnswer( new Answer<Future<?>>()
                {
                    @Override
                    public Future<?> answer( final InvocationOnMock invocation ) throws Throwable
                    {
                        return realExecutor.submit( new Runnable() {
                            @Override
                            public void run()
                            {
                                ((Runnable) invocation.getArguments()[0]).run();
                            }
                        });
                    }
                } );

                when( executor.schedule( any( Runnable.class ), anyLong(), any( TimeUnit.class ) ) ).thenAnswer(
                        new Answer<Future<?>>()
                        {
                            @Override
                            public Future<?> answer( final InvocationOnMock invocation ) throws Throwable
                            {
                                realExecutor.submit( new Callable<Void>()
                                {
                                    @Override
                                    public Void call() throws Exception
                                    {
                                        firstMasterAvailableHandled.countDown();

                                        // wait until the second masterIsAvailable comes and then call switchToSlave method
                                        secondMasterAvailableComes.await();
                                        ((Runnable) invocation.getArguments()[0]).run();
                                        secondMasterAvailableHandled.countDown();
                                        return null;
                                    };
                                } );
                                return mock( ScheduledFuture.class );
                            }
                        } );
                return executor;
            }
        };
        toTest.init();
        toTest.start();
        toTest.listeningAt( URI.create( "ha://server3?serverId=3" ) );

        // When

        // masterIsAvailable for instance 1
        URI uri1 = URI.create( "ha://server1" );
        // The first masterIsAvailable should fail so that the slave instance stops at TO_SLAVE state
        doThrow( new ComException( "Fail to switch to slave and reschedule to retry" ) )
                .when( switchToSlave )
                .switchToSlave( any( LifeSupport.class ), any( URI.class ), eq( uri1 ), any( CancellationRequest.class ) );

        toTest.masterIsAvailable( new HighAvailabilityMemberChangeEvent( PENDING, TO_SLAVE, new InstanceId( 1 ), uri1 ) );
        firstMasterAvailableHandled.await(); // wait until the first masterIsAvailable triggers the exception handling process
        verify( switchToSlave ).switchToSlave( any( LifeSupport.class ), any( URI.class ), eq( uri1 ),
                any( CancellationRequest.class ) );


        // masterIsAvailable for instance 2
        URI uri2 = URI.create( "ha://server2" );
        toTest.masterIsAvailable( new HighAvailabilityMemberChangeEvent( TO_SLAVE, TO_SLAVE, new InstanceId( 2 ), uri2 ) );
        secondMasterAvailableComes.countDown();
        secondMasterAvailableHandled.await(); // wait until switchToSlave method is invoked again

        // Then
        // switchToSlave should be retried with new master id
        verify( switchToSlave ).switchToSlave( any( LifeSupport.class ), any( URI.class ), eq( uri2 ),
                any( CancellationRequest.class ) );
    }

    @Test
    public void shouldNotResetAvailableMasterURIIfElectionResultReceived() throws Throwable
    {
        /*
         * It is possible that a masterIsElected nulls out the current available master URI in the HAMS. That can
         * be a problem if handing the mIE event is concurrent with an ongoing switch which re-runs because
         * the store was incompatible or a log was missing. In such a case it will find a null master URI on
         * rerun and it will fail.
         */

        // Given
        SwitchToSlave switchToSlave = mock( SwitchToSlave.class );
        // The fist run through switchToSlave
        final CountDownLatch firstCallMade = new CountDownLatch( 1 );
        // The second run through switchToSlave
        final CountDownLatch secondCallMade = new CountDownLatch( 1 );
        // The latch for waiting for the masterIsElected to come through
        final CountDownLatch waitForSecondMessage = new CountDownLatch( 1 );

        HighAvailabilityModeSwitcher toTest = new HighAvailabilityModeSwitcher( switchToSlave,
                mock( SwitchToMaster.class ), mock( Election.class ), mock( ClusterMemberAvailability.class ),
                dependencyResolverMock(), new DevNullLoggingService() );
        URI uri1 = URI.create( "ha://server1" );
        toTest.init();
        toTest.start();
        toTest.listeningAt( URI.create( "ha://server3?serverId=3" ) );

        when( switchToSlave.switchToSlave( any( LifeSupport.class ), any( URI.class ), any( URI.class ), any( CancellationRequest.class ) ) ).thenAnswer( new Answer<URI>()

        {
            // The first time around it must "fail" so as to cause a rerun, then wait for the mIE to come through
            @Override
            public URI answer( InvocationOnMock invocation ) throws Throwable
            {
                firstCallMade.countDown();
                waitForSecondMessage.await();
                throw new NoSuchLogVersionException( 1 );
            }
        } ).thenAnswer( new Answer<URI>()
        {
            // The second time around it can finish normally, it doesn't really matter. Just let the test continue.
            @Override
            public URI answer( InvocationOnMock invocation ) throws Throwable
            {
                secondCallMade.countDown();
                return URI.create( "ha://server3" );
            }
        } );

        // When

        // The first message goes through, start the first run
        toTest.masterIsAvailable(
                new HighAvailabilityMemberChangeEvent( PENDING, TO_SLAVE, new InstanceId( 1 ), uri1 ) );
        // Wait for it to be processed but get just before the exception
        firstCallMade.await();
        // It is just about to throw the exception, i.e. rerun. Send in the event
        toTest.masterIsElected(
                new HighAvailabilityMemberChangeEvent( TO_SLAVE, TO_SLAVE, new InstanceId( 1 ), null ) );
        // Allow to continue and do the second run
        waitForSecondMessage.countDown();
        // Wait for the call to finish
        secondCallMade.await();

        // Then
        verify( switchToSlave, times( 2 ) ).switchToSlave( any( LifeSupport.class ), any( URI.class ), eq( uri1 ), any(
                CancellationRequest.class ) );
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

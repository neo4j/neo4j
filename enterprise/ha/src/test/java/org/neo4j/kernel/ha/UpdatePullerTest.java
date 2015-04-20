/*
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
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.UpdatePuller.Condition;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberContext;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.InvalidEpochException;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.BufferingLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.OnDemandJobScheduler;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.ha.UpdatePuller.NEXT_TICKET;

public class UpdatePullerTest
{
    private final InstanceId myId = new InstanceId( 1 );
    private final CapturingHighAvailabilityMemberStateMachine stateMachine =
            new CapturingHighAvailabilityMemberStateMachine( myId );

    private final OnDemandJobScheduler scheduler = new OnDemandJobScheduler();
    private final Config config = mock( Config.class );
    private final AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
    private final LastUpdateTime lastUpdateTime = mock( LastUpdateTime.class );
    private final Master master = mock( Master.class );
    private final ErrorTrackingLogging logging = new ErrorTrackingLogging();
    private final RequestContextFactory requestContextFactory = mock( RequestContextFactory.class );
    private final InvalidEpochExceptionHandler invalidEpochHandler = mock( InvalidEpochExceptionHandler.class );
    private final UpdatePuller updatePuller = new UpdatePuller( stateMachine, requestContextFactory,
            master, lastUpdateTime, logging, myId, invalidEpochHandler );

    public final @Rule CleanupRule cleanup = new CleanupRule();

    @Before
    public void setup() throws Throwable
    {
        when( config.get( HaSettings.pull_interval ) ).thenReturn( 1000l );
        when( config.get( ClusterSettings.server_id ) ).thenReturn( myId );
        when( availabilityGuard.isAvailable( anyLong() ) ).thenReturn( true );
        updatePuller.init();
        updatePuller.start();
    }

    @Test
    public void shouldNotStartPullingUpdatesUntilStartIsCalled() throws Throwable
    {
        // GIVEN
        final UpdatePullerClient puller = new UpdatePullerClient(
                1,
                scheduler,
                logging,
                updatePuller,
                availabilityGuard );

        // WHEN
        puller.init();

        // THEN
        // Asserts the puller set the job
        assertNotNull( scheduler.getJob() );
        scheduler.runJob();
        verifyZeroInteractions( lastUpdateTime, availabilityGuard );
    }

    @Test
    public void shouldStartAndStopPullingUpdatesWhenStartAndStopIsCalled() throws Throwable
    {
        // GIVEN
        final UpdatePullerClient puller = new UpdatePullerClient(
                1,
                scheduler,
                logging,
                updatePuller,
                availabilityGuard );

        // WHEN
        puller.init();

        // THEN
        // Asserts the puller set the job
        assertNotNull( scheduler.getJob() );

        puller.start();
        updatePuller.unpause();
        scheduler.runJob();

        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        updatePuller.stop();
        scheduler.runJob();

        verifyNoMoreInteractions( lastUpdateTime, availabilityGuard );
    }

    @Test
    public void shouldStopPullingUpdatesWhenThisInstanceBecomesTheMaster() throws Throwable
    {
        // GIVEN
        final UpdatePullerClient puller = new UpdatePullerClient(
                1,
                scheduler,
                logging,
                updatePuller,
                availabilityGuard );

        // WHEN
        puller.init();
        puller.start();
        updatePuller.unpause();
        scheduler.runJob();

        // THEN
        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        stateMachine.masterIsElected(); // pauses the update puller

        scheduler.runJob();

        verifyNoMoreInteractions( lastUpdateTime, availabilityGuard );
    }

    @Test
    public void shouldKeepPullingUpdatesWhenThisInstanceBecomesASlave() throws Throwable
    {
        // GIVEN
        final UpdatePullerClient puller = new UpdatePullerClient(
                1,
                scheduler,
                logging,
                updatePuller,
                availabilityGuard );

        // WHEN
        puller.init();
        puller.start();
        updatePuller.unpause();
        scheduler.runJob();

        // THEN
        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        scheduler.runJob();

        verify( lastUpdateTime, times( 2 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 2 ) ).isAvailable( anyLong() );
        verify( master, times( 2 ) ).pullUpdates( Matchers.<RequestContext>any() );
    }

    @Test
    public void shouldResumePullingUpdatesWhenThisInstanceSwitchesFromMasterToSlave() throws Throwable
    {
        // GIVEN
        final UpdatePullerClient puller = new UpdatePullerClient(
                1,
                scheduler,
                logging,
                updatePuller,
                availabilityGuard );

        // WHEN
        puller.init();
        puller.start();
        updatePuller.unpause();
        scheduler.runJob();

        // THEN
        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        stateMachine.masterIsElected(); // pauses the update puller

        // This job should be ignored, since I'm now master
        scheduler.runJob();

        updatePuller.unpause();

        scheduler.runJob();

        verify( lastUpdateTime, times( 2 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 2 ) ).isAvailable( anyLong() );
        verify( master, times( 2 ) ).pullUpdates( Matchers.<RequestContext>any() );
    }

    @Test
    public void shouldResumePullingUpdatesWhenThisInstanceSwitchesFromSlaveToMaster() throws Throwable
    {
        final UpdatePullerClient puller = new UpdatePullerClient(
                1,
                scheduler,
                logging,
                updatePuller,
                availabilityGuard );

        puller.init();
        puller.start();
        updatePuller.unpause();
        scheduler.runJob();

        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        scheduler.runJob();

        verify( lastUpdateTime, times( 2 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 2 ) ).isAvailable( anyLong() );
        verify( master, times( 2 ) ).pullUpdates( Matchers.<RequestContext>any() );

        stateMachine.masterIsElected(); // pauses the update puller

        verifyNoMoreInteractions( lastUpdateTime, availabilityGuard );
    }

    @Test
    public void shouldReturnFalseIfPullerInitiallyInactiveNonStrict() throws Exception
    {
        // GIVEN
        Condition condition = mock( Condition.class );
        updatePuller.pause();

        // WHEN
        boolean result = updatePuller.await( condition, false );

        // THEN
        assertFalse( result );
        verifyNoMoreInteractions( condition );
    }

    @Test
    public void shouldReturnFalseIfPullerBecomesInactiveWhileWaitingNonStrict() throws Exception
    {
        // GIVEN
        Condition condition = mock( Condition.class );
        updatePuller.unpause();
        when( condition.evaluate( anyInt(), anyInt() ) ).thenAnswer( new Answer<Boolean>()
        {
            @Override
            public Boolean answer( InvocationOnMock invocation ) throws Throwable
            {
                updatePuller.pause();
                return false;
            }
        } );

        // WHEN
        boolean result = updatePuller.await( condition, false );

        // THEN
        assertFalse( result );
        verify( condition, times( 1 ) ).evaluate( anyInt(), anyInt() );
    }

    @Test
    public void shouldThrowIfPullerInitiallyInactiveStrict() throws Exception
    {
        // GIVEN
        Condition condition = mock( Condition.class );
        updatePuller.pause();

        // WHEN
        try
        {
            updatePuller.await( condition, true );
            fail( "Should have thrown" );
        }
        catch ( IllegalStateException e )
        {   // THEN Good
            verifyNoMoreInteractions( condition );
        }
    }

    @Test
    public void shouldThrowIfPullerBecomesInactiveWhileWaitingStrict() throws Exception
    {
        // GIVEN
        Condition condition = mock( Condition.class );
        updatePuller.unpause();
        when( condition.evaluate( anyInt(), anyInt() ) ).thenAnswer( new Answer<Boolean>()
        {
            @Override
            public Boolean answer( InvocationOnMock invocation ) throws Throwable
            {
                updatePuller.pause();
                return false;
            }
        } );

        // WHEN
        try
        {
            updatePuller.await( condition, true );
            fail( "Should have thrown" );
        }
        catch ( IllegalStateException e )
        {   // THEN Good
            verify( condition, times( 1 ) ).evaluate( anyInt(), anyInt() );
        }
    }

    @Test
    public void shouldHandleInvalidEpochByNotifyingItsHandler() throws Exception
    {
        // GIVEN
        doThrow( InvalidEpochException.class ).when( master ).pullUpdates( any( RequestContext.class ) );
        updatePuller.unpause();

        // WHEN
        updatePuller.await( NEXT_TICKET, true );

        // THEN
        verify( invalidEpochHandler ).handle();
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldCopeWithHardExceptionsLikeOutOfMemory() throws Exception
    {
        // GIVEN
        when( master.pullUpdates( any( RequestContext.class ) ) )
                .thenThrow( OutOfMemoryError.class )
                .thenReturn( Response.EMPTY );
        updatePuller.unpause();

        // WHEN making the first pull
        updatePuller.await( NEXT_TICKET, true );

        // THEN the OOM should be caught and logged
        assertTrue( logging.hasSeenError( OutOfMemoryError.class ) );

        // WHEN that has passed THEN we should still be making pull attempts.
        updatePuller.await( NEXT_TICKET, true );
    }

    private static class CapturingHighAvailabilityMemberStateMachine extends HighAvailabilityMemberStateMachine
    {
        private final InstanceId myId;
        private final URI uri;
        private final List<HighAvailabilityMemberListener> listeners = new ArrayList<>();

        public CapturingHighAvailabilityMemberStateMachine( InstanceId myId )
        {
            super( mock( HighAvailabilityMemberContext.class ), mock( AvailabilityGuard.class ),
                    mock( ClusterMembers.class ), mock( ClusterMemberEvents.class ), mock( Election.class ),
                    mock( StringLogger.class ) );
            this.myId = myId;
            this.uri = URI.create( "ha://me" );
        }

        @Override
        public void addHighAvailabilityMemberListener( HighAvailabilityMemberListener toAdd )
        {
            listeners.add( toAdd );
        }

        public void masterIsElected()
        {
            for ( HighAvailabilityMemberListener listener : listeners )
            {
                listener.masterIsElected( new HighAvailabilityMemberChangeEvent(
                        HighAvailabilityMemberState.PENDING, HighAvailabilityMemberState.TO_MASTER, myId, uri ) );
            }
        }
    }

    private static class ErrorTrackingLogging extends LifecycleAdapter implements Logging
    {
        private final List<Throwable> errors = new ArrayList<>();
        private final StringLogger logger = new ErrorTrackingLogger( errors );

        @Override
        public StringLogger getMessagesLog( Class loggingClass )
        {
            return logger;
        }

        @Override
        public ConsoleLogger getConsoleLog( Class loggingClass )
        {
            throw new UnsupportedOperationException( "Shouldn't be required" );
        }

        boolean hasSeenError( Class<?> cls )
        {
            for ( Throwable throwable : errors )
            {
                if ( throwable.getClass().equals( cls ) )
                {
                    return true;
                }
            }
            return false;
        }
    }

    private static class ErrorTrackingLogger extends BufferingLogger
    {
        private final List<Throwable> errors;

        public ErrorTrackingLogger( List<Throwable> errors )
        {
            this.errors = errors;
        }

        @Override
        public synchronized void logMessage( String msg, Throwable cause )
        {
            errors.add( cause );
            super.logMessage( msg, cause );
        }

        @Override
        public synchronized void logMessage( String msg, Throwable cause, boolean flush )
        {
            errors.add( cause );
            super.logMessage( msg, cause, flush );
        }
    }
}

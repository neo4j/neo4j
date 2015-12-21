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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.InvalidEpochException;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.impl.util.CountingJobScheduler;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.BufferingLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.LogMarker;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.test.CleanupRule;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
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
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.ha.SlaveUpdatePuller.Condition;

public class SlaveUpdatePullerTest
{
    private final AtomicInteger scheduledJobs = new AtomicInteger();
    private final InstanceId instanceId = new InstanceId( 1 );
    private final Config config = mock( Config.class );
    private final AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
    private final LastUpdateTime lastUpdateTime = mock( LastUpdateTime.class );
    private final Master master = mock( Master.class );
    private final ErrorTrackingLogging logging = new ErrorTrackingLogging();
    private final RequestContextFactory requestContextFactory = mock( RequestContextFactory.class );
    private final InvalidEpochExceptionHandler invalidEpochHandler = mock( InvalidEpochExceptionHandler.class );
    private final JobScheduler jobScheduler = new CountingJobScheduler(
            scheduledJobs, new Neo4jJobScheduler( "SlaveUpdatePullerTest" ) );
    private final SlaveUpdatePuller updatePuller = new SlaveUpdatePuller( requestContextFactory,
            master, lastUpdateTime, logging, instanceId, availabilityGuard, invalidEpochHandler, jobScheduler );

    @Rule
    public final CleanupRule cleanup = new CleanupRule();

    @Before
    public void setUp() throws Throwable
    {
        when( config.get( HaSettings.pull_interval ) ).thenReturn( 1000L );
        when( config.get( ClusterSettings.server_id ) ).thenReturn( instanceId );
        when( availabilityGuard.isAvailable( anyLong() ) ).thenReturn( true );
        jobScheduler.init();
        jobScheduler.start();
        updatePuller.init();
        updatePuller.start();
    }

    @After
    public void tearDown() throws Throwable
    {
        updatePuller.stop();
        updatePuller.shutdown();
        jobScheduler.stop();
        jobScheduler.shutdown();
    }

    @Test
    public void initialisationMustBeIdempotent() throws Throwable
    {
        updatePuller.init();
        updatePuller.start();
        updatePuller.init();
        updatePuller.start();
        updatePuller.init();
        updatePuller.start();
        assertThat( scheduledJobs.get(), is( 1 ) );
    }

    @Test
    public void shouldStopPullingAfterStop() throws Throwable
    {
        // WHEN
        updatePuller.pullUpdates();

        // THEN
        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        // WHEN
        updatePuller.shutdown();
        updatePuller.pullUpdates();

        // THEN
        verifyNoMoreInteractions( lastUpdateTime, availabilityGuard );
    }

    @Test
    public void keepPullingUpdatesOnConsecutiveCalls() throws Throwable
    {
        // WHEN
        updatePuller.pullUpdates();

        // THEN
        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        // WHEN
        updatePuller.pullUpdates();

        // THEN
        verify( lastUpdateTime, times( 2 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 2 ) ).isAvailable( anyLong() );
        verify( master, times( 2 ) ).pullUpdates( Matchers.<RequestContext>any() );
    }

    @Test
    public void falseOnTryPullUpdatesOnInactivePuller() throws Throwable
    {
        // GIVEN
        updatePuller.shutdown();

        // WHEN
        boolean result = updatePuller.tryPullUpdates();

        // THEN
        assertFalse( result );
    }

    @Test
    public void shouldThrowIfPullerInitiallyInactiveStrict() throws Throwable
    {
        // GIVEN
        Condition condition = mock( Condition.class );
        updatePuller.shutdown();

        // WHEN
        try
        {
            updatePuller.pullUpdates( condition, true );
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

        when( condition.evaluate( anyInt(), anyInt() ) ).thenAnswer( new Answer<Boolean>()
        {
            @Override
            public Boolean answer( InvocationOnMock invocation ) throws Throwable
            {
                updatePuller.shutdown();
                return false;
            }
        } );

        // WHEN
        try
        {
            updatePuller.pullUpdates( condition, true );
            fail( "Should have thrown" );
        }
        catch ( IllegalStateException e )
        {   // THEN Good
            verify( condition ).evaluate( anyInt(), anyInt() );
        }
    }

    @Test
    public void shouldHandleInvalidEpochByNotifyingItsHandler() throws Exception
    {
        // GIVEN
        doThrow( InvalidEpochException.class ).when( master ).pullUpdates( any( RequestContext.class ) );

        // WHEN
        updatePuller.pullUpdates();

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


        // WHEN making the first pull
        updatePuller.pullUpdates();

        // THEN the OOM should be caught and logged
        assertTrue( logging.hasSeenError( OutOfMemoryError.class ) );

        // WHEN that has passed THEN we should still be making pull attempts.
        updatePuller.pullUpdates();
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
        public synchronized void error( String msg, Throwable cause, boolean flush, LogMarker logMarker )
        {
            errors.add( cause );
            super.error( msg, cause, flush, logMarker );
        }
    }
}

/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.OngoingStubbing;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.com.ComException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.UpdatePuller.Condition;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.InvalidEpochException;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.impl.util.CountingJobScheduler;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.rule.CleanupRule;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class SlaveUpdatePullerTest
{
    private final AtomicInteger scheduledJobs = new AtomicInteger();
    private final InstanceId instanceId = new InstanceId( 1 );
    private final Config config = mock( Config.class );
    private final AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
    private final LastUpdateTime lastUpdateTime = mock( LastUpdateTime.class );
    private final Master master = mock( Master.class, RETURNS_MOCKS );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final RequestContextFactory requestContextFactory = mock( RequestContextFactory.class );
    private final InvalidEpochExceptionHandler invalidEpochHandler = mock( InvalidEpochExceptionHandler.class );
    private final SlaveUpdatePuller.Monitor monitor = mock( SlaveUpdatePuller.Monitor.class );
    private final JobScheduler jobScheduler = new CountingJobScheduler( scheduledJobs, new CentralJobScheduler() );
    private final SlaveUpdatePuller updatePuller = new SlaveUpdatePuller( requestContextFactory, master,
            lastUpdateTime, logProvider, instanceId, availabilityGuard, invalidEpochHandler, jobScheduler, monitor );

    @Rule
    public final CleanupRule cleanup = new CleanupRule();

    @Before
    public void setUp() throws Throwable
    {
        when( requestContextFactory.newRequestContext() ).thenReturn( new RequestContext( 42, 42, 42, 42, 42 ) );
        when( config.get( HaSettings.pull_interval ) ).thenReturn( Duration.ofSeconds( 1 ) );
        when( config.get( ClusterSettings.server_id ) ).thenReturn( instanceId );
        when( availabilityGuard.isAvailable( anyLong() ) ).thenReturn( true );
        jobScheduler.init();
        jobScheduler.start();
        updatePuller.start();
    }

    @After
    public void tearDown() throws Throwable
    {
        updatePuller.stop();
        jobScheduler.stop();
        jobScheduler.shutdown();
    }

    @Test
    public void initialisationMustBeIdempotent()
    {
        updatePuller.start();
        updatePuller.start();
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
        verify( master, times( 1 ) ).pullUpdates( ArgumentMatchers.any() );
        verify( monitor, times( 1 ) ).pulledUpdates( anyLong() );

        // WHEN
        updatePuller.stop();
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
        verify( master, times( 1 ) ).pullUpdates( ArgumentMatchers.any() );
        verify( monitor, times( 1 ) ).pulledUpdates( anyLong() );

        // WHEN
        updatePuller.pullUpdates();

        // THEN
        verify( lastUpdateTime, times( 2 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 2 ) ).isAvailable( anyLong() );
        verify( master, times( 2 ) ).pullUpdates( ArgumentMatchers.any() );
        verify( monitor, times( 2 ) ).pulledUpdates( anyLong() );
    }

    @Test
    public void falseOnTryPullUpdatesOnInactivePuller() throws Throwable
    {
        // GIVEN
        updatePuller.stop();

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
        updatePuller.stop();

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

        when( condition.evaluate( anyInt(), anyInt() ) ).thenAnswer( invocation ->
        {
            updatePuller.stop();
            return false;
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
        OutOfMemoryError oom = new OutOfMemoryError();
        when( master.pullUpdates( any( RequestContext.class ) ) )
                .thenThrow( oom )
                .thenReturn( Response.empty() );

        // WHEN making the first pull
        updatePuller.pullUpdates();

        // THEN the OOM should be caught and logged
        logProvider.assertAtLeastOnce(
                inLog( SlaveUpdatePuller.class ).error( org.hamcrest.Matchers.any( String.class ), sameInstance( oom ) )
        );

        // WHEN that has passed THEN we should still be making pull attempts.
        updatePuller.pullUpdates();
    }

    @Test
    public void shouldCapExcessiveComExceptionLogging() throws Exception
    {
        OngoingStubbing<Response<Void>> updatePullStubbing = when( master.pullUpdates( any( RequestContext.class ) ) );
        updatePullStubbing.thenThrow( new ComException() );

        for ( int i = 0; i < SlaveUpdatePuller.LOG_CAP + 20; i++ )
        {
            updatePuller.pullUpdates();
        }

        logProvider.assertContainsThrowablesMatching( 0, repeat( new ComException(), SlaveUpdatePuller.LOG_CAP ) );

        // And we should be able to recover afterwards
        updatePullStubbing.thenReturn( Response.empty() ).thenThrow( new ComException() );

        updatePuller.pullUpdates(); // This one will succeed and unlock the circuit breaker
        updatePuller.pullUpdates(); // And then we log another exception

        logProvider.assertContainsThrowablesMatching( 0, repeat( new ComException(), SlaveUpdatePuller.LOG_CAP + 1 ) );
    }

    private Throwable[] repeat( Throwable throwable, int count )
    {
        Throwable[] throwables = new Throwable[count];
        for ( int i = 0; i < count; i++ )
        {
            throwables[i] = throwable;
        }
        return throwables;
    }

    @Test
    public void shouldCapExcessiveInvalidEpochExceptionLogging() throws Exception
    {
        OngoingStubbing<Response<Void>> updatePullStubbing = when( master.pullUpdates( any( RequestContext.class ) ) );
        updatePullStubbing.thenThrow( new InvalidEpochException( 2, 1 ) );

        for ( int i = 0; i < SlaveUpdatePuller.LOG_CAP + 20; i++ )
        {
            updatePuller.pullUpdates();
        }

        logProvider.assertContainsThrowablesMatching( 0,
                repeat( new InvalidEpochException( 2, 1 ), SlaveUpdatePuller.LOG_CAP ) );

        // And we should be able to recover afterwards
        updatePullStubbing.thenReturn( Response.empty() ).thenThrow( new InvalidEpochException( 2, 1 ) );

        updatePuller.pullUpdates(); // This one will succeed and unlock the circuit breaker
        updatePuller.pullUpdates(); // And then we log another exception

        logProvider.assertContainsThrowablesMatching( 0,
                repeat( new InvalidEpochException( 2, 1 ), SlaveUpdatePuller.LOG_CAP + 1 ) );
    }

}

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
package org.neo4j.kernel.ha;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.com.RequestContext;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberContext;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class UpdatePullerTest
{
    private final InstanceId myId = new InstanceId( 1 );
    private final CapturingHighAvailabilityMemberStateMachine stateMachine =
            new CapturingHighAvailabilityMemberStateMachine( myId );

    private final OnDemandCallScheduler scheduler = new OnDemandCallScheduler();
    private final Config config = mock( Config.class );
    private final AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
    private final LastUpdateTime lastUpdateTime = mock( LastUpdateTime.class );
    private final Master master = mock( Master.class );
    private final Logging logging = new DevNullLoggingService();
    private final RequestContextFactory requestContextFactory = mock( RequestContextFactory.class );
    private final DependencyResolver resolver = mock( DependencyResolver.class );
    private final UpdatePuller updatePuller = new UpdatePuller( stateMachine, availabilityGuard,
            requestContextFactory, master, lastUpdateTime, logging, myId );

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
                updatePuller );

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
                updatePuller );

        // WHEN
        puller.init();

        // THEN
        // Asserts the puller set the job
        assertNotNull( scheduler.getJob() );

        puller.start();
        stateMachine.switchInstanceToSlave();
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
                updatePuller );

        // WHEN
        puller.init();
        puller.start();
        stateMachine.switchInstanceToSlave();
        scheduler.runJob();

        // THEN
        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        stateMachine.switchInstanceToMaster();

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
                updatePuller );

        // WHEN
        puller.init();
        puller.start();
        stateMachine.switchInstanceToSlave();
        scheduler.runJob();

        // THEN
        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        stateMachine.switchInstanceToSlave();

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
                updatePuller );

        // WHEN
        puller.init();
        puller.start();
        stateMachine.switchInstanceToSlave();
        scheduler.runJob();

        // THEN
        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        stateMachine.switchInstanceToMaster();

        // This job should be ignored, since I'm now master
        scheduler.runJob();

        stateMachine.switchInstanceToSlave();

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
                updatePuller );

        puller.init();
        puller.start();
        stateMachine.switchInstanceToSlave();
        scheduler.runJob();

        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        stateMachine.switchInstanceToSlave();

        scheduler.runJob();

        verify( lastUpdateTime, times( 2 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 2 ) ).isAvailable( anyLong() );
        verify( master, times( 2 ) ).pullUpdates( Matchers.<RequestContext>any() );

        stateMachine.switchInstanceToMaster();

        verifyNoMoreInteractions( lastUpdateTime, availabilityGuard );
    }

    private static class OnDemandCallScheduler extends LifecycleAdapter implements JobScheduler
    {
        private Runnable job;

        @Override
        public JobHandle schedule( Group group, Runnable job )
        {
            this.job = job;
            return new OnDemandJobHandle();
        }

        @Override
        public JobHandle scheduleRecurring( Group group, Runnable runnable, long period, TimeUnit timeUnit )
        {
            this.job = runnable;
            return new OnDemandJobHandle();
        }

        @Override
        public JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period, TimeUnit
                timeUnit )
        {
            this.job = runnable;
            return new OnDemandJobHandle();
        }

        public Runnable getJob()
        {
            return job;
        }

        public void runJob()
        {
            job.run();
        }

        private class OnDemandJobHandle implements JobHandle
        {
            @Override
            public void cancel( boolean mayInterruptIfRunning )
            {
                job = null;
            }
        }
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

        public void switchInstanceToSlave()
        {
            for ( HighAvailabilityMemberListener listener : listeners )
            {
                listener.slaveIsAvailable(
                        new HighAvailabilityMemberChangeEvent(
                                HighAvailabilityMemberState.TO_SLAVE, HighAvailabilityMemberState.SLAVE, myId, uri ) );
            }
        }

        public void switchInstanceToMaster()
        {
            for ( HighAvailabilityMemberListener listener : listeners )
            {
                listener.masterIsAvailable(
                        new HighAvailabilityMemberChangeEvent(
                                HighAvailabilityMemberState.TO_MASTER, HighAvailabilityMemberState.MASTER, myId, uri ) );
            }
        }
    }
}

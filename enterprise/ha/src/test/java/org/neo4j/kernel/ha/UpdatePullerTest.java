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
package org.neo4j.kernel.ha;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Matchers;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
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
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class UpdatePullerTest
{
    @Test
    public void shouldNotStartPullingUpdatesUntilStartIsCalled() throws Throwable
    {
        OnDemandCallScheduler scheduler = new OnDemandCallScheduler();
        Config config = mock( Config.class );
        InstanceId myId = new InstanceId( 1 );
        when( config.get( HaSettings.pull_interval ) ).thenReturn( 1000l );
        when( config.get( ClusterSettings.server_id ) ).thenReturn( myId );

        LastUpdateTime lastUpdateTime = mock( LastUpdateTime.class );
        AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
        when( availabilityGuard.isAvailable( anyLong() ) ).thenReturn( true );
        HaXaDataSourceManager dataSourceManager = mock( HaXaDataSourceManager.class );
        Master master = mock( Master.class );

        UpdatePuller puller = new UpdatePuller( mock( HighAvailabilityMemberStateMachine.class),  dataSourceManager,
                master, mock( RequestContextFactory.class ), mock( AbstractTransactionManager.class ),
                availabilityGuard, lastUpdateTime, config,
                scheduler, mock( StringLogger.class ) );

        puller.init();

        // Asserts the puller set the job
        assertNotNull( scheduler.getJob() );

        scheduler.runJob();

        verifyZeroInteractions( lastUpdateTime, availabilityGuard, dataSourceManager );
    }

    @Test
    public void shouldStartAndStopPullingUpdatesWhenStartAndStopIsCalled() throws Throwable
    {
        OnDemandCallScheduler scheduler = new OnDemandCallScheduler();
        Config config = mock( Config.class );
        InstanceId myId = new InstanceId( 1 );
        when( config.get( HaSettings.pull_interval ) ).thenReturn( 1000l );
        when( config.get( ClusterSettings.server_id ) ).thenReturn( myId );

        LastUpdateTime lastUpdateTime = mock( LastUpdateTime.class );
        AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
        when( availabilityGuard.isAvailable( anyLong() ) ).thenReturn( true );
        HaXaDataSourceManager dataSourceManager = mock( HaXaDataSourceManager.class );
        Master master = mock( Master.class );

        UpdatePuller puller = new UpdatePuller( mock( HighAvailabilityMemberStateMachine.class), dataSourceManager,
                master, mock( RequestContextFactory.class ), mock( AbstractTransactionManager.class ),
                availabilityGuard, lastUpdateTime, config,
                scheduler, mock( StringLogger.class ) );

        puller.init();

        // Asserts the puller set the job
        assertNotNull( scheduler.getJob() );

        puller.start();
        scheduler.runJob();

        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( dataSourceManager, times( 1 ) ).applyTransactions( Matchers.<Response>any() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        puller.stop();
        scheduler.runJob();

        verifyNoMoreInteractions( lastUpdateTime, availabilityGuard, dataSourceManager );
    }

    @Test
    public void shouldStopPullingUpdatesWhenThisInstanceBecomesTheMaster() throws Throwable
    {
        OnDemandCallScheduler scheduler = new OnDemandCallScheduler();
        Config config = mock( Config.class );
        InstanceId myId = new InstanceId( 1 );
        when( config.get( HaSettings.pull_interval ) ).thenReturn( 1000l );
        when( config.get( ClusterSettings.server_id ) ).thenReturn( myId );

        LastUpdateTime lastUpdateTime = mock( LastUpdateTime.class );
        AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
        when( availabilityGuard.isAvailable( anyLong() ) ).thenReturn( true );
        HaXaDataSourceManager dataSourceManager = mock( HaXaDataSourceManager.class );
        Master master = mock( Master.class );

        CapturingHighAvailabilityMemberStateMachine memberStateMachine = new
                CapturingHighAvailabilityMemberStateMachine( myId );
        UpdatePuller puller = new UpdatePuller( memberStateMachine, dataSourceManager,
                master, mock( RequestContextFactory.class ), mock( AbstractTransactionManager.class ),
                availabilityGuard, lastUpdateTime, config,
                scheduler, mock( StringLogger.class ) );

        puller.init();
        puller.start();
        scheduler.runJob();

        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( dataSourceManager, times( 1 ) ).applyTransactions( Matchers.<Response>any() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        memberStateMachine.switchInstanceToMaster();

        scheduler.runJob();

        verifyNoMoreInteractions( lastUpdateTime, availabilityGuard, dataSourceManager );
    }

    @Test
    public void shouldKeepPullingUpdatesWhenThisInstanceBecomesASlave() throws Throwable
    {
        OnDemandCallScheduler scheduler = new OnDemandCallScheduler();
        Config config = mock( Config.class );
        InstanceId myId = new InstanceId( 1 );
        when( config.get( HaSettings.pull_interval ) ).thenReturn( 1000l );
        when( config.get( ClusterSettings.server_id ) ).thenReturn( myId );

        LastUpdateTime lastUpdateTime = mock( LastUpdateTime.class );
        AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
        when( availabilityGuard.isAvailable( anyLong() ) ).thenReturn( true );
        HaXaDataSourceManager dataSourceManager = mock( HaXaDataSourceManager.class );
        Master master = mock( Master.class );

        CapturingHighAvailabilityMemberStateMachine memberStateMachine = new
                CapturingHighAvailabilityMemberStateMachine( myId );
        UpdatePuller puller = new UpdatePuller( memberStateMachine, dataSourceManager,
                master, mock( RequestContextFactory.class ), mock( AbstractTransactionManager.class ),
                availabilityGuard, lastUpdateTime, config,
                scheduler, mock( StringLogger.class ) );

        puller.init();
        puller.start();
        scheduler.runJob();

        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( dataSourceManager, times( 1 ) ).applyTransactions( Matchers.<Response>any() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        memberStateMachine.switchInstanceToSlave();

        scheduler.runJob();

        verify( lastUpdateTime, times( 2 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 2 ) ).isAvailable( anyLong() );
        verify( dataSourceManager, times( 2 ) ).applyTransactions( Matchers.<Response>any() );
        verify( master, times( 2 ) ).pullUpdates( Matchers.<RequestContext>any() );
    }

    @Test
    public void shouldResumePullingUpdatesWhenThisInstanceSwitchesFromMasterToSlave() throws Throwable
    {
        OnDemandCallScheduler scheduler = new OnDemandCallScheduler();
        Config config = mock( Config.class );
        InstanceId myId = new InstanceId( 1 );
        when( config.get( HaSettings.pull_interval ) ).thenReturn( 1000l );
        when( config.get( ClusterSettings.server_id ) ).thenReturn( myId );

        LastUpdateTime lastUpdateTime = mock( LastUpdateTime.class );
        AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
        when( availabilityGuard.isAvailable( anyLong() ) ).thenReturn( true );
        HaXaDataSourceManager dataSourceManager = mock( HaXaDataSourceManager.class );
        Master master = mock( Master.class );

        CapturingHighAvailabilityMemberStateMachine memberStateMachine = new
                CapturingHighAvailabilityMemberStateMachine( myId );
        UpdatePuller puller = new UpdatePuller( memberStateMachine, dataSourceManager,
                master, mock( RequestContextFactory.class ), mock( AbstractTransactionManager.class ),
                availabilityGuard, lastUpdateTime, config,
                scheduler, mock( StringLogger.class ) );

        puller.init();
        puller.start();
        scheduler.runJob();

        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( dataSourceManager, times( 1 ) ).applyTransactions( Matchers.<Response>any() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        memberStateMachine.switchInstanceToMaster();

        scheduler.runJob();

        memberStateMachine.switchInstanceToSlave();

        scheduler.runJob();

        verify( lastUpdateTime, times( 2 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 2 ) ).isAvailable( anyLong() );
        verify( dataSourceManager, times( 2 ) ).applyTransactions( Matchers.<Response>any() );
        verify( master, times( 2 ) ).pullUpdates( Matchers.<RequestContext>any() );
    }

    @Test
    public void shouldResumePullingUpdatesWhenThisInstanceSwitchesFromSlaveToMaster() throws Throwable
    {
        OnDemandCallScheduler scheduler = new OnDemandCallScheduler();
        Config config = mock( Config.class );
        InstanceId myId = new InstanceId( 1 );
        when( config.get( HaSettings.pull_interval ) ).thenReturn( 1000l );
        when( config.get( ClusterSettings.server_id ) ).thenReturn( myId );

        LastUpdateTime lastUpdateTime = mock( LastUpdateTime.class );
        AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );
        when( availabilityGuard.isAvailable( anyLong() ) ).thenReturn( true );
        HaXaDataSourceManager dataSourceManager = mock( HaXaDataSourceManager.class );
        Master master = mock( Master.class );

        CapturingHighAvailabilityMemberStateMachine memberStateMachine = new
                CapturingHighAvailabilityMemberStateMachine( myId );
        UpdatePuller puller = new UpdatePuller( memberStateMachine, dataSourceManager,
                master, mock( RequestContextFactory.class ), mock( AbstractTransactionManager.class ),
                availabilityGuard, lastUpdateTime, config,
                scheduler, mock( StringLogger.class ) );

        puller.init();
        puller.start();
        scheduler.runJob();

        verify( lastUpdateTime, times( 1 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 1 ) ).isAvailable( anyLong() );
        verify( dataSourceManager, times( 1 ) ).applyTransactions( Matchers.<Response>any() );
        verify( master, times( 1 ) ).pullUpdates( Matchers.<RequestContext>any() );

        memberStateMachine.switchInstanceToSlave();

        scheduler.runJob();

        verify( lastUpdateTime, times( 2 ) ).setLastUpdateTime( anyLong() );
        verify( availabilityGuard, times( 2 ) ).isAvailable( anyLong() );
        verify( dataSourceManager, times( 2 ) ).applyTransactions( Matchers.<Response>any() );
        verify( master, times( 2 ) ).pullUpdates( Matchers.<RequestContext>any() );

        memberStateMachine.switchInstanceToMaster();

        verifyNoMoreInteractions( lastUpdateTime, availabilityGuard, dataSourceManager );
    }

    private static class OnDemandCallScheduler extends LifecycleAdapter implements JobScheduler
    {
        private Runnable job;

        @Override
        public void schedule( Group group, Runnable job )
        {
            this.job = job;
        }

        @Override
        public void scheduleRecurring( Group group, Runnable runnable, long period, TimeUnit timeUnit )
        {
            this.job = runnable;
        }

        @Override
        public void scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period, TimeUnit
                timeUnit )
        {
            this.job = runnable;
        }

        @Override
        public void cancelRecurring( Group group, Runnable runnable )
        {
            this.job = null;
        }

        public Runnable getJob()
        {
            return job;
        }

        public void runJob()
        {
            job.run();
        }
    }

    private static class CapturingHighAvailabilityMemberStateMachine extends HighAvailabilityMemberStateMachine
    {
        private final InstanceId myId;
        private final URI uri;
        private HighAvailabilityMemberListener listener;

        public CapturingHighAvailabilityMemberStateMachine( InstanceId myId )
        {
            super( mock( HighAvailabilityMemberContext.class ), mock(AvailabilityGuard.class ),
                    mock(ClusterMembers.class ), mock(ClusterMemberEvents.class ), mock(Election.class ),
                    mock(StringLogger.class ) );
            this.myId = myId;
            this.uri = URI.create( "ha://me" );
        }

        @Override
        public void addHighAvailabilityMemberListener( HighAvailabilityMemberListener toAdd )
        {
            listener = toAdd;
        }

        public void switchInstanceToSlave()
        {
             listener.slaveIsAvailable(
                     new HighAvailabilityMemberChangeEvent(
                             HighAvailabilityMemberState.TO_SLAVE, HighAvailabilityMemberState.SLAVE, myId, uri ) );
        }

        public void switchInstanceToMaster()
        {
            listener.masterIsAvailable(
                    new HighAvailabilityMemberChangeEvent(
                            HighAvailabilityMemberState.TO_MASTER, HighAvailabilityMemberState.MASTER, myId, uri ) );
        }
    }
}
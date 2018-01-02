/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.function.Function;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.AvailabilityGuard.AvailabilityRequirement;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.MasterClient214;
import org.neo4j.kernel.ha.PullerFactory;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.member.ObservedClusterMembers;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.SLAVE;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcherTest.neoStoreDataSourceSupplierMock;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcherTest.storeSupplierMock;

public class HighAvailabilityMemberStateMachineTest
{
    @Test
    public void shouldStartFromPending() throws Exception
    {
        // Given
        HighAvailabilityMemberStateMachine memberStateMachine = buildMockedStateMachine();
        // Then
        assertThat( memberStateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
    }

    @Test
    public void shouldMoveToToMasterFromPendingOnMasterElectedForItself() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );
        ClusterMemberListenerContainer memberListenerContainer = mockAddClusterMemberListener( events );

        HighAvailabilityMemberStateMachine stateMachine = buildMockedStateMachine( context, events );
        stateMachine.init();
        ClusterMemberListener memberListener = memberListenerContainer.get();

        // When
        memberListener.coordinatorIsElected( me );

        // Then
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_MASTER ) );
    }

    @Test
    public void shouldRemainToPendingOnMasterElectedForSomeoneElse() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );
        ClusterMemberListenerContainer memberListenerContainer = mockAddClusterMemberListener( events );

        HighAvailabilityMemberStateMachine stateMachine = buildMockedStateMachine( context, events );
        stateMachine.init();
        ClusterMemberListener memberListener = memberListenerContainer.get();

        // When
        memberListener.coordinatorIsElected( new InstanceId( 2 ) );

        // Then
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
    }

    @Test
    public void shouldSwitchToToSlaveOnMasterAvailableForSomeoneElse() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );
        ClusterMemberListenerContainer memberListenerContainer = mockAddClusterMemberListener( events );

        HighAvailabilityMemberStateMachine stateMachine = buildMockedStateMachine( context, events );

        stateMachine.init();
        ClusterMemberListener memberListener = memberListenerContainer.get();
        HAStateChangeListener probe = new HAStateChangeListener();
        stateMachine.addHighAvailabilityMemberListener( probe );

        // When
        memberListener.memberIsAvailable( MASTER, new InstanceId( 2 ), URI.create( "ha://whatever" ), StoreId.DEFAULT );

        // Then
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_SLAVE ) );
        assertThat( probe.masterIsAvailable, is( true ) );
    }

    @Test
    public void whenInMasterStateLosingQuorumShouldPutInPending() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId other = new InstanceId( 2 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );

        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ObservedClusterMembers members = mockClusterMembers( me, Collections.<InstanceId>emptyList(), singletonList( other ) );

        ClusterMemberEvents events = mock( ClusterMemberEvents.class );
        ClusterMemberListenerContainer memberListenerContainer = mockAddClusterMemberListener( events );

        HighAvailabilityMemberStateMachine stateMachine = buildMockedStateMachine( context, events, members, guard );

        stateMachine.init();
        ClusterMemberListener memberListener = memberListenerContainer.get();
        HAStateChangeListener probe = new HAStateChangeListener();
        stateMachine.addHighAvailabilityMemberListener( probe );

        // Send it to MASTER
        memberListener.coordinatorIsElected( me );
        memberListener.memberIsAvailable( MASTER, me, URI.create( "ha://whatever" ), StoreId.DEFAULT );

        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.MASTER ) );

        // When
        memberListener.memberIsFailed( new InstanceId( 2 ) );

        // Then
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
        assertThat( probe.instanceStops, is( false ) );
        assertThat( probe.instanceDetached, is( true ) );
        verify( guard, times( 1 ) ).require( any( AvailabilityRequirement.class ) );
    }

    @Test
    public void whenInSlaveStateLosingOtherSlaveShouldNotPutInPending() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId master = new InstanceId( 2 );
        InstanceId otherSlave = new InstanceId( 3 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ObservedClusterMembers members = mockClusterMembers( me, singletonList( master ), singletonList( otherSlave ) );

        ClusterMemberEvents events = mock( ClusterMemberEvents.class );
        ClusterMemberListenerContainer memberListenerContainer = mockAddClusterMemberListener( events );

        HighAvailabilityMemberStateMachine stateMachine = buildMockedStateMachine( context, events, members, guard );

        stateMachine.init();
        ClusterMemberListener memberListener = memberListenerContainer.get();
        HAStateChangeListener probe = new HAStateChangeListener();
        stateMachine.addHighAvailabilityMemberListener( probe );

        // Send it to MASTER
        memberListener.memberIsAvailable( MASTER, master, URI.create( "ha://whatever" ), StoreId.DEFAULT );
        memberListener.memberIsAvailable( SLAVE, me, URI.create( "ha://whatever3" ), StoreId.DEFAULT );
        memberListener.memberIsAvailable( SLAVE, otherSlave, URI.create( "ha://whatever2" ), StoreId.DEFAULT );

        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.SLAVE ) );

        // When
        memberListener.memberIsFailed( otherSlave );

        // Then
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.SLAVE ) );
        assertThat( probe.instanceStops, is( false ) );
    }


    @Test
    public void whenInSlaveStateLosingMasterShouldPutInPending() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId master = new InstanceId( 2 );
        InstanceId otherSlave = new InstanceId( 3 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ObservedClusterMembers members = mockClusterMembers( me, singletonList( otherSlave ), singletonList( master ) );

        ClusterMemberEvents events = mock( ClusterMemberEvents.class );
        ClusterMemberListenerContainer memberListenerContainer = mockAddClusterMemberListener( events );

        HighAvailabilityMemberStateMachine stateMachine = buildMockedStateMachine( context, events, members, guard );

        stateMachine.init();
        ClusterMemberListener memberListener = memberListenerContainer.get();
        HAStateChangeListener probe = new HAStateChangeListener();
        stateMachine.addHighAvailabilityMemberListener( probe );

        // Send it to MASTER
        memberListener.coordinatorIsElected( master );
        memberListener.memberIsAvailable( MASTER, master, URI.create( "ha://whatever" ), StoreId.DEFAULT );
        memberListener.memberIsAvailable( SLAVE, me, URI.create( "ha://whatever3" ), StoreId.DEFAULT );
        memberListener.memberIsAvailable( SLAVE, otherSlave, URI.create( "ha://whatever2" ), StoreId.DEFAULT );

        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.SLAVE ) );

        // When
        memberListener.memberIsFailed( master );

        // Then
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
        assertThat( probe.instanceStops, is( false ) );
        assertThat( probe.instanceDetached, is( true ) );
        verify( guard, times( 1 ) ).require( any( AvailabilityRequirement.class ) );
    }

    @Test
    public void whenInToMasterStateLosingQuorumShouldPutInPending() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId other = new InstanceId( 2 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ObservedClusterMembers members = mockClusterMembers( me, Collections.<InstanceId>emptyList(), singletonList( other ) );

        ClusterMemberEvents events = mock( ClusterMemberEvents.class );
        ClusterMemberListenerContainer memberListenerContainer = mockAddClusterMemberListener( events );

        HighAvailabilityMemberStateMachine stateMachine = buildMockedStateMachine( context, events, members, guard );

        stateMachine.init();
        ClusterMemberListener memberListener = memberListenerContainer.get();
        HAStateChangeListener probe = new HAStateChangeListener();
        stateMachine.addHighAvailabilityMemberListener( probe );

        // Send it to MASTER
        memberListener.coordinatorIsElected( me );

        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_MASTER ) );

        // When
        memberListener.memberIsFailed( new InstanceId( 2 ) );

        // Then
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
        assertThat( probe.instanceStops, is( false ) );
        assertThat( probe.instanceDetached, is( true ) );
        verify( guard, times( 1 ) ).require( any( AvailabilityRequirement.class ) );
    }

    @Test
    public void whenInToSlaveStateLosingQuorumShouldPutInPending() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId other = new InstanceId( 2 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ObservedClusterMembers members = mockClusterMembers( me, Collections.<InstanceId>emptyList(), singletonList( other ) );

        ClusterMemberEvents events = mock( ClusterMemberEvents.class );
        ClusterMemberListenerContainer memberListenerContainer = mockAddClusterMemberListener( events );

        HighAvailabilityMemberStateMachine stateMachine = buildMockedStateMachine( context, events, members, guard );
        stateMachine.init();
        ClusterMemberListener memberListener = memberListenerContainer.get();
        HAStateChangeListener probe = new HAStateChangeListener();
        stateMachine.addHighAvailabilityMemberListener( probe );

        // Send it to MASTER
        memberListener.memberIsAvailable( MASTER, other, URI.create( "ha://whatever" ), StoreId.DEFAULT );

        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_SLAVE ) );

        // When
        memberListener.memberIsFailed( new InstanceId( 2 ) );

        // Then
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
        assertThat( probe.instanceStops, is( false ) );
        assertThat( probe.instanceDetached, is( true ) );
        verify( guard, times( 1 ) ).require( any( AvailabilityRequirement.class ) );
    }

    @Test
    public void whenSlaveOnlyIsElectedStayInPending() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, true );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );
        ClusterMemberListenerContainer memberListenerContainer = mockAddClusterMemberListener( events );

        HighAvailabilityMemberStateMachine stateMachine = buildMockedStateMachine( context, events );

        stateMachine.init();

        ClusterMemberListener memberListener = memberListenerContainer.get();

        // When
        memberListener.coordinatorIsElected( me );

        // Then
        assertThat( stateMachine.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
    }


    @Test
    public void whenHAModeSwitcherSwitchesToSlaveTheOtherModeSwitcherDoNotGetTheOldMasterClient() throws Throwable
    {
        InstanceId me = new InstanceId( 1 );
        StoreId storeId = new StoreId();
        HighAvailabilityMemberContext context = mock( HighAvailabilityMemberContext.class );
        when( context.getMyId() ).thenReturn( me );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ObservedClusterMembers members = mock( ObservedClusterMembers.class );
        ClusterMember masterMember = mock( ClusterMember.class );
        when( masterMember.getHARole() ).thenReturn( "master" );
        when( masterMember.hasRole( "master" ) ).thenReturn( true );
        when( masterMember.getInstanceId() ).thenReturn( new InstanceId( 2 ) );
        when( masterMember.getStoreId() ).thenReturn( storeId );
        ClusterMember self = new ClusterMember( me );
        when( members.getMembers() ).thenReturn( Arrays.asList( self, masterMember ) );
        when( members.getCurrentMember() ).thenReturn( self );
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        when( fs.fileExists( any( File.class ) ) ).thenReturn( true );
        when( dependencyResolver.resolveDependency( FileSystemAbstraction.class ) ).thenReturn( fs );
        when( dependencyResolver.resolveDependency( Monitors.class ) ).thenReturn( new Monitors() );
        NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );
        when( dataSource.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dataSource.getStoreId() ).thenReturn( storeId );
        when( dependencyResolver.resolveDependency( NeoStoreDataSource.class ) ).thenReturn( dataSource );
        when( dependencyResolver.resolveDependency( TransactionIdStore.class ) ).
                thenReturn( new DeadSimpleTransactionIdStore() );
        when( dependencyResolver.resolveDependency( ObservedClusterMembers.class ) ).thenReturn( members );
        UpdatePuller updatePuller = mock( UpdatePuller.class );
        when( updatePuller.tryPullUpdates() ).thenReturn( true );
        when( dependencyResolver.resolveDependency( UpdatePuller.class ) ).thenReturn( updatePuller );

        ClusterMemberAvailability clusterMemberAvailability = mock( ClusterMemberAvailability.class );
        final TriggerableClusterMemberEvents events = new TriggerableClusterMemberEvents();

        Election election = mock( Election.class );
        HighAvailabilityMemberStateMachine stateMachine =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election,
                        NullLogProvider.getInstance() );

        ClusterMembers clusterMembers = new ClusterMembers( members, stateMachine );
        when( dependencyResolver.resolveDependency( ClusterMembers.class ) ).thenReturn( clusterMembers );

        stateMachine.init();
        stateMachine.start();

        final DelegateInvocationHandler<Master> handler = new DelegateInvocationHandler<>( Master.class );

        MasterClientResolver masterClientResolver = mock( MasterClientResolver.class );
        MasterClient masterClient = mock( MasterClient.class );
        when( masterClient.getProtocolVersion() ).thenReturn( MasterClient214.PROTOCOL_VERSION );
        when( masterClient.handshake( anyLong(), any( StoreId.class ) ) ).thenReturn(
                new Response<HandshakeResult>( new HandshakeResult( 0, 42 ), storeId, mock( ResourceReleaser.class ) )
                {
                    @Override
                    public void accept( Handler handler ) throws IOException
                    {
                    }

                    @Override
                    public boolean hasTransactionsToBeApplied()
                    {
                        return false;
                    }
                } );
        when( masterClient.toString() ).thenReturn( "TheExpectedMasterClient!" );
        when( masterClientResolver.instantiate( anyString(), anyInt(), anyString(), any( Monitors.class ),
                any( StoreId.class ), any( LifeSupport.class ) ) ).thenReturn( masterClient );

        final CountDownLatch latch = new CountDownLatch( 2 );
        final AtomicBoolean switchedSuccessfully = new AtomicBoolean();

        SwitchToSlave.Monitor monitor = new SwitchToSlave.Monitor()
        {
            @Override
            public void switchToSlaveStarted()
            {
            }

            @Override
            public void switchToSlaveCompleted( boolean wasSuccessful )
            {
                switchedSuccessfully.set( wasSuccessful );
                latch.countDown();
            }

            @Override
            public void storeCopyStarted()
            {
            }

            @Override
            public void storeCopyCompleted( boolean wasSuccessful )
            {

            }

            @Override
            public void catchupStarted()
            {

            }

            @Override
            public void catchupCompleted()
            {

            }
        };

        Config config = new Config( Collections.singletonMap( ClusterSettings.server_id.name(), me.toString() ) );

        TransactionCounters transactionCounters = mock( TransactionCounters.class );
        when( transactionCounters.getNumberOfActiveTransactions() ).thenReturn( 0l );

        PageCache pageCacheMock = mock( PageCache.class );
        PagedFile pagedFileMock = mock( PagedFile.class );
        when( pagedFileMock.getLastPageId() ).thenReturn( 1l );
        when( pageCacheMock.map( any( File.class ), anyInt() ) ).thenReturn( pagedFileMock );

        TransactionIdStore transactionIdStoreMock = mock( TransactionIdStore.class );
        when( transactionIdStoreMock.getLastCommittedTransaction() ).thenReturn( new TransactionId( 0, 0, 0 ) );
        SwitchToSlave switchToSlave = new SwitchToSlave( new File( "" ), NullLogService.getInstance(),
                mock( FileSystemAbstraction.class ),
                config, dependencyResolver,
                mock( HaIdGeneratorFactory.class ),
                handler,
                mock( ClusterMemberAvailability.class ), mock( RequestContextFactory.class ),
                mock( PullerFactory.class, RETURNS_MOCKS ),
                Iterables.<KernelExtensionFactory<?>>empty(), masterClientResolver,
                monitor,
                new StoreCopyClient.Monitor.Adapter(),
                Suppliers.singleton( dataSource ),
                Suppliers.singleton( transactionIdStoreMock ),
                new Function<Slave,SlaveServer>()
                {
                    @Override
                    public SlaveServer apply( Slave slave ) throws RuntimeException
                    {
                        SlaveServer mock = mock( SlaveServer.class );
                        when( mock.getSocketAddress() ).thenReturn( new InetSocketAddress( "localhost", 123 ) );
                        return mock;
                    }
                }, updatePuller, pageCacheMock, mock( Monitors.class ), transactionCounters );

        HighAvailabilityModeSwitcher haModeSwitcher = new HighAvailabilityModeSwitcher( switchToSlave,
                mock( SwitchToMaster.class ), election, clusterMemberAvailability, mock( ClusterClient.class ),
                storeSupplierMock(), me, NullLogService.getInstance(), neoStoreDataSourceSupplierMock() );
        haModeSwitcher.init();
        haModeSwitcher.start();
        haModeSwitcher.listeningAt( URI.create( "http://localhost:12345" ) );

        stateMachine.addHighAvailabilityMemberListener( haModeSwitcher );

        final AtomicReference<Master> ref = new AtomicReference<>( null );

        //noinspection unchecked
        AbstractModeSwitcher<Object> otherModeSwitcher = new AbstractModeSwitcher<Object>( haModeSwitcher, mock(
                DelegateInvocationHandler.class ) )
        {
            @Override
            protected Object getSlaveImpl( LifeSupport life )
            {
                Master master = handler.cement();
                ref.set( master );
                latch.countDown();
                return null;
            }

            @Override
            protected Object getMasterImpl( LifeSupport life )
            {
                return null;
            }
        };
        otherModeSwitcher.init();
        otherModeSwitcher.start();

        // When
        events.switchToSlave( me );

        // Then
        latch.await();
        assertTrue( "mode switch failed", switchedSuccessfully.get() );
        Master actual = ref.get();
        // let's test the toString()s since there are too many wrappers of proxies
        assertEquals( masterClient.toString(), actual.toString() );

        stateMachine.stop();
        stateMachine.shutdown();
        haModeSwitcher.stop();
        haModeSwitcher.shutdown();
        otherModeSwitcher.stop();
        otherModeSwitcher.shutdown();
    }

    private ObservedClusterMembers mockClusterMembers( InstanceId me, List<InstanceId> alive, List<InstanceId> failed )
    {
        ObservedClusterMembers members = mock( ObservedClusterMembers.class );

        // we cannot set outside of the package the isAlive to return false. So do it with a mock
        List<ClusterMember> aliveMembers = new ArrayList<>( alive.size() );
        List<ClusterMember> failedMembers = new ArrayList<>( failed.size() );
        for ( InstanceId instanceId : alive )
        {
            ClusterMember otherMember = mock( ClusterMember.class );
            when( otherMember.getInstanceId() ).thenReturn( instanceId );
            // the failed argument tells us which instance should be marked as failed
            when( otherMember.isAlive() ).thenReturn( true );
            aliveMembers.add( otherMember );
        }

        for ( InstanceId instanceId : failed )
        {
            ClusterMember otherMember = mock( ClusterMember.class );
            when( otherMember.getInstanceId() ).thenReturn( instanceId );
            // the failed argument tells us which instance should be marked as failed
            when( otherMember.isAlive() ).thenReturn( false );
            failedMembers.add( otherMember );
        }

        ClusterMember thisMember = new ClusterMember( me );
        aliveMembers.add( thisMember );

        List<ClusterMember> allMembers = new ArrayList<>();
        allMembers.addAll( aliveMembers ); // thisMember is in aliveMembers
        allMembers.addAll( failedMembers );
        when( members.getMembers() ).thenReturn( allMembers );
        when( members.getAliveMembers() ).thenReturn( aliveMembers );

        return members;
    }

    static ClusterMemberListenerContainer mockAddClusterMemberListener( ClusterMemberEvents events )
    {
        final ClusterMemberListenerContainer listenerContainer = new ClusterMemberListenerContainer();
        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                listenerContainer.set( (ClusterMemberListener) invocation.getArguments()[0] );
                return null;
            }

        } ).when( events ).addClusterMemberListener( Matchers.<ClusterMemberListener>any() );
        return listenerContainer;
    }

    private HighAvailabilityMemberStateMachine buildMockedStateMachine()
    {
        return new StateMachineBuilder().build();
    }

    private HighAvailabilityMemberStateMachine buildMockedStateMachine ( HighAvailabilityMemberContext context,
            ClusterMemberEvents events )
    {
        return new StateMachineBuilder().withContext( context ).withEvents( events ).build();
    }

    private HighAvailabilityMemberStateMachine buildMockedStateMachine( HighAvailabilityMemberContext context,
            ClusterMemberEvents events, ObservedClusterMembers clusterMembers, AvailabilityGuard guard )
    {
        return new StateMachineBuilder().withContext( context ).withEvents( events ).withClusterMembers(
                clusterMembers ).withGuard( guard ).build();
    }

    static class StateMachineBuilder
    {
        HighAvailabilityMemberContext context = mock( HighAvailabilityMemberContext.class );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );
        ObservedClusterMembers clusterMembers = mock( ObservedClusterMembers.class );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        Election election = mock( Election.class );

        public StateMachineBuilder withContext( HighAvailabilityMemberContext context )
        {
            this.context = context;
            return this;
        }

        public StateMachineBuilder withEvents(ClusterMemberEvents events)
        {
            this.events = events;
            return this;
        }

        public StateMachineBuilder withClusterMembers(ObservedClusterMembers clusterMember)
        {
            this.clusterMembers = clusterMember;
            return this;
        }

        public StateMachineBuilder withGuard(AvailabilityGuard guard)
        {
            this.guard = guard;
            return this;
        }

        public StateMachineBuilder withElection(Election election)
        {
            this.election = election;
            return this;
        }

        public HighAvailabilityMemberStateMachine build()
        {
            return new HighAvailabilityMemberStateMachine( context, guard, clusterMembers, events, election,
                    NullLogProvider.getInstance() );
        }
    }

    static class ClusterMemberListenerContainer
    {
        private ClusterMemberListener clusterMemberListener;

        public ClusterMemberListener get()
        {
            return clusterMemberListener;
        }

        public void set( ClusterMemberListener clusterMemberListener )
        {
            if ( this.clusterMemberListener != null )
            {
                throw new IllegalStateException( "Expected to have only 1 listener, but have more. " +
                                                 "Defined listener: " + this.clusterMemberListener +
                                                 ". Newly added listener:" + clusterMemberListener );
            }
            this.clusterMemberListener = clusterMemberListener;
        }
    }

    static final class HAStateChangeListener implements HighAvailabilityMemberListener
    {
        boolean masterIsElected = false;
        boolean masterIsAvailable = false;
        boolean slaveIsAvailable = false;
        boolean instanceStops = false;
        boolean instanceDetached = false;
        HighAvailabilityMemberChangeEvent lastEvent = null;

        @Override
        public void masterIsElected( HighAvailabilityMemberChangeEvent event )
        {
            masterIsElected = true;
            masterIsAvailable = false;
            slaveIsAvailable = false;
            instanceStops = false;
            instanceDetached = false;
            lastEvent = event;
        }

        @Override
        public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            masterIsElected = false;
            masterIsAvailable = true;
            slaveIsAvailable = false;
            instanceStops = false;
            instanceDetached = false;
            lastEvent = event;
        }

        @Override
        public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            masterIsElected = false;
            masterIsAvailable = false;
            slaveIsAvailable = true;
            instanceStops = false;
            instanceDetached = false;
            lastEvent = event;
        }

        @Override
        public void instanceStops( HighAvailabilityMemberChangeEvent event )
        {
            masterIsElected = false;
            masterIsAvailable = false;
            slaveIsAvailable = false;
            instanceStops = true;
            instanceDetached = false;
            lastEvent = event;
        }

        @Override
        public void instanceDetached( HighAvailabilityMemberChangeEvent event )
        {
            masterIsElected = false;
            masterIsAvailable = false;
            slaveIsAvailable = false;
            instanceStops = false;
            instanceDetached = true;
            lastEvent = event;
        }
    }

    private static class TriggerableClusterMemberEvents implements ClusterMemberEvents
    {
        private ClusterMemberListener listener;

        @Override
        public void addClusterMemberListener( ClusterMemberListener listener )
        {
            this.listener = listener;
        }

        @Override
        public void removeClusterMemberListener( ClusterMemberListener listener )
        {
        }

        public void switchToSlave( InstanceId me )
        {
            InstanceId someOneElseThanMyself = new InstanceId( me.toIntegerIndex() + 1 );
            listener.memberIsAvailable( "master", someOneElseThanMyself, URI.create( "cluster://127.0.0.1:2390?serverId=2" ), null );
            listener.memberIsAvailable( "slave", me, null, null );
        }
    }
}

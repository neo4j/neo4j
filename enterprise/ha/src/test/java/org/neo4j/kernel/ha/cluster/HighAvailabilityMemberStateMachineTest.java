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
package org.neo4j.kernel.ha.cluster;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcherTest.storeSupplierMock;

public class HighAvailabilityMemberStateMachineTest
{
    @Test
    public void shouldStartFromPending() throws Exception
    {
        // Given
        HighAvailabilityMemberContext context = mock( HighAvailabilityMemberContext.class );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ClusterMembers members = mock( ClusterMembers.class );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );
        Election election = mock( Election.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, NullLogProvider.getInstance() );

        // Then
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
    }

    @Test
    public void shouldMoveToToMasterFromPendingOnMasterElectedForItself() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ClusterMembers members = mock( ClusterMembers.class );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );

        final Set<ClusterMemberListener> listener = new HashSet<>();

        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                listener.add( (ClusterMemberListener) invocation.getArguments()[0] );
                return null;
            }

        } ).when( events ).addClusterMemberListener( Matchers.<ClusterMemberListener>any() );

        Election election = mock( Election.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, NullLogProvider.getInstance() );
        toTest.init();
        ClusterMemberListener theListener = listener.iterator().next();

        // When
        theListener.coordinatorIsElected( me );

        // Then
        assertThat( listener.size(), equalTo( 1 ) ); // Sanity check.
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_MASTER ) );
    }

    @Test
    public void shouldRemainToPendingOnMasterElectedForSomeoneElse() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ClusterMembers members = mock( ClusterMembers.class );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );

        final Set<ClusterMemberListener> listener = new HashSet<>();

        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                listener.add( (ClusterMemberListener) invocation.getArguments()[0] );
                return null;
            }

        } ).when( events ).addClusterMemberListener( Matchers.<ClusterMemberListener>any() );

        Election election = mock( Election.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, NullLogProvider.getInstance() );
        toTest.init();
        ClusterMemberListener theListener = listener.iterator().next();

        // When
        theListener.coordinatorIsElected( new InstanceId( 2 ) );

        // Then
        assertThat( listener.size(), equalTo( 1 ) ); // Sanity check.
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
    }

    @Test
    public void shouldSwitchToToSlaveOnMasterAvailableForSomeoneElse() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ClusterMembers members = mock( ClusterMembers.class );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );

        final Set<ClusterMemberListener> listener = new HashSet<>();

        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                listener.add( (ClusterMemberListener) invocation.getArguments()[0] );
                return null;
            }

        } ).when( events ).addClusterMemberListener( Matchers.<ClusterMemberListener>any() );

        Election election = mock( Election.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, NullLogProvider.getInstance() );
        toTest.init();
        ClusterMemberListener theListener = listener.iterator().next();
        HAStateChangeListener probe = new HAStateChangeListener();
        toTest.addHighAvailabilityMemberListener( probe );

        // When
        theListener.memberIsAvailable( MASTER, new InstanceId( 2 ), URI.create( "ha://whatever" ), StoreId.DEFAULT );

        // Then
        assertThat( listener.size(), equalTo( 1 ) ); // Sanity check.
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_SLAVE ) );
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
        ClusterMembers members = mock( ClusterMembers.class );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );

        List<ClusterMember> membersList = new LinkedList<>();
        // we cannot set outside of the package the isAlive to return false. So do it with a mock
        ClusterMember otherMemberMock = mock( ClusterMember.class );
        when( otherMemberMock.getInstanceId() ).thenReturn( other );
        when( otherMemberMock.isAlive() ).thenReturn( false );
        membersList.add( otherMemberMock );

        membersList.add( new ClusterMember( me ) );
        when( members.getMembers() ).thenReturn( membersList );

        final Set<ClusterMemberListener> listener = new HashSet<>();

        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                listener.add( (ClusterMemberListener) invocation.getArguments()[0] );
                return null;
            }

        } ).when( events ).addClusterMemberListener( Matchers.<ClusterMemberListener>any() );

        Election election = mock( Election.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, NullLogProvider.getInstance() );
        toTest.init();
        ClusterMemberListener theListener = listener.iterator().next();
        HAStateChangeListener probe = new HAStateChangeListener();
        toTest.addHighAvailabilityMemberListener( probe );

        // Send it to MASTER
        theListener.coordinatorIsElected( me );
        theListener.memberIsAvailable( MASTER, me, URI.create( "ha://whatever" ), StoreId.DEFAULT );

        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.MASTER ) );

        // When
        theListener.memberIsFailed( new InstanceId( 2 ) );

        // Then
        assertThat( listener.size(), equalTo( 1 ) ); // Sanity check.
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
        assertThat( probe.instanceStops, is( true ) );
        verify( guard, times( 2 ) ).require( any( AvailabilityRequirement.class ) );
    }

    @Test
    public void whenInSlaveStateLosingQuorumShouldPutInPending() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId other = new InstanceId( 2 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ClusterMembers members = mock( ClusterMembers.class );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );

        List<ClusterMember> membersList = new LinkedList<>();
        // we cannot set outside of the package the isAlive to return false. So do it with a mock
        ClusterMember otherMemberMock = mock( ClusterMember.class );
        when( otherMemberMock.getInstanceId() ).thenReturn( other );
        when( otherMemberMock.isAlive() ).thenReturn( false );
        membersList.add( otherMemberMock );

        membersList.add( new ClusterMember( me ) );
        when( members.getMembers() ).thenReturn( membersList );

        final Set<ClusterMemberListener> listener = new HashSet<>();

        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                listener.add( (ClusterMemberListener) invocation.getArguments()[0] );
                return null;
            }

        } ).when( events ).addClusterMemberListener( Matchers.<ClusterMemberListener>any() );

        Election election = mock( Election.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, NullLogProvider.getInstance() );
        toTest.init();
        ClusterMemberListener theListener = listener.iterator().next();
        HAStateChangeListener probe = new HAStateChangeListener();
        toTest.addHighAvailabilityMemberListener( probe );

        // Send it to MASTER
        theListener.memberIsAvailable( MASTER, other, URI.create( "ha://whatever" ), StoreId.DEFAULT );
        theListener.memberIsAvailable( SLAVE, me, URI.create( "ha://whatever2" ), StoreId.DEFAULT );

        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.SLAVE ) );

        // When
        theListener.memberIsFailed( new InstanceId( 2 ) );

        // Then
        assertThat( listener.size(), equalTo( 1 ) ); // Sanity check.
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
        assertThat( probe.instanceStops, is( true ) );
        verify( guard, times( 2 ) ).require( any( AvailabilityRequirement.class ) );
    }

    @Test
    public void whenInToMasterStateLosingQuorumShouldPutInPending() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId other = new InstanceId( 2 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, false );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ClusterMembers members = mock( ClusterMembers.class );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );

        List<ClusterMember> membersList = new LinkedList<>();
        // we cannot set outside of the package the isAlive to return false. So do it with a mock
        ClusterMember otherMemberMock = mock( ClusterMember.class );
        when( otherMemberMock.getInstanceId() ).thenReturn( other );
        when( otherMemberMock.isAlive() ).thenReturn( false );
        membersList.add( otherMemberMock );

        membersList.add( new ClusterMember( me ) );
        when( members.getMembers() ).thenReturn( membersList );

        final Set<ClusterMemberListener> listener = new HashSet<>();

        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                listener.add( (ClusterMemberListener) invocation.getArguments()[0] );
                return null;
            }

        } ).when( events ).addClusterMemberListener( Matchers.<ClusterMemberListener>any() );

        Election election = mock( Election.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, NullLogProvider.getInstance() );
        toTest.init();
        ClusterMemberListener theListener = listener.iterator().next();
        HAStateChangeListener probe = new HAStateChangeListener();
        toTest.addHighAvailabilityMemberListener( probe );

        // Send it to MASTER
        theListener.coordinatorIsElected( me );

        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_MASTER ) );

        // When
        theListener.memberIsFailed( new InstanceId( 2 ) );

        // Then
        assertThat( listener.size(), equalTo( 1 ) ); // Sanity check.
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
        assertThat( probe.instanceStops, is( true ) );
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
        ClusterMembers members = mock( ClusterMembers.class );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );

        List<ClusterMember> membersList = new LinkedList<>();
        // we cannot set outside of the package the isAlive to return false. So do it with a mock
        ClusterMember otherMemberMock = mock( ClusterMember.class );
        when( otherMemberMock.getInstanceId() ).thenReturn( other );
        when( otherMemberMock.isAlive() ).thenReturn( false );
        membersList.add( otherMemberMock );

        membersList.add( new ClusterMember( me ) );
        when( members.getMembers() ).thenReturn( membersList );

        final Set<ClusterMemberListener> listener = new HashSet<>();

        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                listener.add( (ClusterMemberListener) invocation.getArguments()[0] );
                return null;
            }

        } ).when( events ).addClusterMemberListener( Matchers.<ClusterMemberListener>any() );

        Election election = mock( Election.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, NullLogProvider.getInstance() );
        toTest.init();
        ClusterMemberListener theListener = listener.iterator().next();
        HAStateChangeListener probe = new HAStateChangeListener();
        toTest.addHighAvailabilityMemberListener( probe );

        // Send it to MASTER
        theListener.memberIsAvailable( MASTER, other, URI.create( "ha://whatever" ), StoreId.DEFAULT );

        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_SLAVE ) );

        // When
        theListener.memberIsFailed( new InstanceId( 2 ) );

        // Then
        assertThat( listener.size(), equalTo( 1 ) ); // Sanity check.
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
        assertThat( probe.instanceStops, is( true ) );
        verify( guard, times( 1 ) ).require( any( AvailabilityRequirement.class ) );
    }

    @Test
    public void whenSlaveOnlyIsElectedStayInPending() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        HighAvailabilityMemberContext context = new SimpleHighAvailabilityMemberContext( me, true );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ClusterMembers members = mock( ClusterMembers.class );
        ClusterMemberEvents events = mock( ClusterMemberEvents.class );

        final Set<ClusterMemberListener> listener = new HashSet<>();

        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                listener.add( (ClusterMemberListener) invocation.getArguments()[0] );
                return null;
            }

        } ).when( events ).addClusterMemberListener( Matchers.<ClusterMemberListener>any() );

        Election election = mock( Election.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, NullLogProvider.getInstance() );

        toTest.init();

        ClusterMemberListener theListener = listener.iterator().next();

        // When
        theListener.coordinatorIsElected( me );

        // Then
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );

    }

    @Test
    public void whenHAModeSwitcherSwitchesToSlaveTheOtherModeSwitcherDoNotGetTheOldMasterClient() throws Throwable
    {
        InstanceId me = new InstanceId( 1 );
        StoreId storeId = new StoreId();
        HighAvailabilityMemberContext context = mock( HighAvailabilityMemberContext.class );
        when( context.getMyId() ).thenReturn( me );
        AvailabilityGuard guard = mock( AvailabilityGuard.class );
        ClusterMembers members = mock( ClusterMembers.class );
        ClusterMember masterMember = mock( ClusterMember.class );
        when( masterMember.getHARole() ).thenReturn( "master" );
        when( masterMember.hasRole( "master" ) ).thenReturn( true );
        when( masterMember.getInstanceId() ).thenReturn( new InstanceId( 2 ) );
        when( masterMember.getStoreId() ).thenReturn( storeId );
        when( members.getMembers() ).thenReturn( Arrays.asList( new ClusterMember( me ), masterMember ) );
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
        when( dependencyResolver.resolveDependency( ClusterMembers.class ) ).thenReturn( members );
        UpdatePuller updatePuller = mock( UpdatePuller.class );
        when( updatePuller.tryPullUpdates() ).thenReturn( true );
        when( dependencyResolver.resolveDependency( UpdatePuller.class ) ).thenReturn( updatePuller );

        ClusterMemberAvailability clusterMemberAvailability = mock( ClusterMemberAvailability.class );
        final TriggerableClusterMemberEvents events = new TriggerableClusterMemberEvents();

        Election election = mock( Election.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, NullLogProvider.getInstance() );

        toTest.init();
        toTest.start();

        final DelegateInvocationHandler<Master> handler = new DelegateInvocationHandler<>( Master.class );

        MasterClientResolver masterClientResolver =  mock( MasterClientResolver.class );
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
        when( masterClientResolver.instantiate( anyString(), anyInt(), any( Monitors.class ),
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
        when( transactionIdStoreMock.getLastCommittedTransaction() ).thenReturn( new TransactionId( 0, 0 ) );
        SwitchToSlave switchToSlave = new SwitchToSlave( new File( "" ), NullLogService.getInstance(),
                mock( FileSystemAbstraction.class ),
                members,
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
                storeSupplierMock(), me, NullLogService.getInstance() );
        haModeSwitcher.init();
        haModeSwitcher.start();
        haModeSwitcher.listeningAt( URI.create( "http://localhost:12345" ) );

        toTest.addHighAvailabilityMemberListener( haModeSwitcher );

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
        Master expected = masterClient;
        Master actual = ref.get();
        // let's test the toString()s since there are too many wrappers of proxies
        String expected1 = expected.toString();
        String actual1 = actual.toString();
        assertEquals( expected1, actual1 );

        toTest.stop();
        toTest.shutdown();
        haModeSwitcher.stop();
        haModeSwitcher.shutdown();
        otherModeSwitcher.stop();
        otherModeSwitcher.shutdown();
    }

    private static final class HAStateChangeListener implements HighAvailabilityMemberListener
    {
        boolean masterIsElected = false;
        boolean masterIsAvailable = false;
        boolean slaveIsAvailable = false;
        boolean instanceStops = false;
        HighAvailabilityMemberChangeEvent lastEvent = null;

        @Override
        public void masterIsElected( HighAvailabilityMemberChangeEvent event )
        {
            masterIsElected = true;
            masterIsAvailable = false;
            slaveIsAvailable = false;
            instanceStops = false;
            lastEvent = event;
        }

        @Override
        public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            masterIsElected = false;
            masterIsAvailable = true;
            slaveIsAvailable = false;
            instanceStops = false;
            lastEvent = event;
        }

        @Override
        public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            masterIsElected = false;
            masterIsAvailable = false;
            slaveIsAvailable = true;
            instanceStops = false;
            lastEvent = event;
        }

        @Override
        public void instanceStops( HighAvailabilityMemberChangeEvent event )
        {
            masterIsElected = false;
            masterIsAvailable = false;
            slaveIsAvailable = false;
            instanceStops = true;
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

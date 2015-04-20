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

import java.net.URI;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.SLAVE;

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
        StringLogger logger = mock( StringLogger.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, logger );

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
        StringLogger logger = mock( StringLogger.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, logger );
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
        StringLogger logger = mock( StringLogger.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, logger );
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
        StringLogger logger = mock( StringLogger.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, logger );
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
        when ( otherMemberMock.getInstanceId() ).thenReturn( other );
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
        StringLogger logger = mock( StringLogger.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, logger );
        toTest.init();
        ClusterMemberListener theListener = listener.iterator().next();
        HAStateChangeListener probe = new HAStateChangeListener();
        toTest.addHighAvailabilityMemberListener( probe );

        // Send it to MASTER
        theListener.coordinatorIsElected( me );
        theListener.memberIsAvailable( MASTER, me, URI.create("ha://whatever"), StoreId.DEFAULT );

        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.MASTER) );

        // When
        theListener.memberIsFailed( new InstanceId( 2 ) );

        // Then
        assertThat( listener.size(), equalTo( 1 ) ); // Sanity check.
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
        assertThat( probe.instanceStops, is( true ) );
        verify(guard, times(2)).deny( any( AvailabilityGuard.AvailabilityRequirement.class) );
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
        when ( otherMemberMock.getInstanceId() ).thenReturn( other );
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
        StringLogger logger = mock( StringLogger.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, logger );
        toTest.init();
        ClusterMemberListener theListener = listener.iterator().next();
        HAStateChangeListener probe = new HAStateChangeListener();
        toTest.addHighAvailabilityMemberListener( probe );

        // Send it to MASTER
        theListener.memberIsAvailable( MASTER, other, URI.create( "ha://whatever" ), StoreId.DEFAULT );
        theListener.memberIsAvailable( SLAVE, me, URI.create( "ha://whatever2" ), StoreId.DEFAULT );

        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.SLAVE) );

        // When
        theListener.memberIsFailed( new InstanceId( 2 ) );

        // Then
        assertThat( listener.size(), equalTo( 1 ) ); // Sanity check.
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
        assertThat( probe.instanceStops, is( true ) );
        verify(guard, times(2)).deny( any( AvailabilityGuard.AvailabilityRequirement.class) );
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
        when ( otherMemberMock.getInstanceId() ).thenReturn( other );
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
        StringLogger logger = mock( StringLogger.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, logger );
        toTest.init();
        ClusterMemberListener theListener = listener.iterator().next();
        HAStateChangeListener probe = new HAStateChangeListener();
        toTest.addHighAvailabilityMemberListener( probe );

        // Send it to MASTER
        theListener.coordinatorIsElected( me );

        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_MASTER) );

        // When
        theListener.memberIsFailed( new InstanceId( 2 ) );

        // Then
        assertThat( listener.size(), equalTo( 1 ) ); // Sanity check.
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
        assertThat( probe.instanceStops, is( true ) );
        verify(guard, times(1)).deny( any( AvailabilityGuard.AvailabilityRequirement.class) );
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
        when ( otherMemberMock.getInstanceId() ).thenReturn( other );
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
        StringLogger logger = mock( StringLogger.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, logger );
        toTest.init();
        ClusterMemberListener theListener = listener.iterator().next();
        HAStateChangeListener probe = new HAStateChangeListener();
        toTest.addHighAvailabilityMemberListener( probe );

        // Send it to MASTER
        theListener.memberIsAvailable( MASTER, other, URI.create("ha://whatever"), StoreId.DEFAULT );

        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.TO_SLAVE) );

        // When
        theListener.memberIsFailed( new InstanceId( 2 ) );

        // Then
        assertThat( listener.size(), equalTo( 1 ) ); // Sanity check.
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );
        assertThat( probe.instanceStops, is( true ) );
        verify(guard, times(1)).deny( any( AvailabilityGuard.AvailabilityRequirement.class) );
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
        StringLogger logger = mock( StringLogger.class );
        HighAvailabilityMemberStateMachine toTest =
                new HighAvailabilityMemberStateMachine( context, guard, members, events, election, logger );

        toTest.init();

        ClusterMemberListener theListener = listener.iterator().next();

        // When
        theListener.coordinatorIsElected( me );

        // Then
        assertThat( toTest.getCurrentState(), equalTo( HighAvailabilityMemberState.PENDING ) );

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
}

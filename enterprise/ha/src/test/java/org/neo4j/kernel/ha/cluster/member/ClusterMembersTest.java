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
package org.neo4j.kernel.ha.cluster.member;

import java.net.URI;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.Heartbeat;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;

import static java.net.URI.create;
import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.SLAVE;
import static org.neo4j.kernel.ha.cluster.member.ClusterMemberMatcher.sameMemberAs;

public class ClusterMembersTest
{
    private static InstanceId clusterId1 = new InstanceId( 1 );
    private static InstanceId clusterId2 = new InstanceId( 2 );
    private static InstanceId clusterId3 = new InstanceId( 3 );
    private static URI clusterUri1 = create( "cluster://server1" );
    private static URI clusterUri2 = create( "cluster://server2" );
    private static URI clusterUri3 = create( "cluster://server3" );
    private static URI haUri1 = create( "ha://server1?serverId="+clusterId1.toIntegerIndex() );

    @Test
    public void shouldRegisterItselfOnListeners() throws Exception
    {
        // given
        Cluster cluster = mock( Cluster.class );
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        // when
        new ClusterMembers( cluster, heartbeat, clusterMemberEvents, null );

        // then
        verify( cluster ).addClusterListener( Mockito.<ClusterListener>any() );
        verify( heartbeat ).addHeartbeatListener( Mockito.<HeartbeatListener>any() );
        verify( clusterMemberEvents ).addClusterMemberListener( Mockito.<ClusterMemberListener>any() );
    }

    @Test
    public void shouldContainMemberListAfterEnteringCluster() throws Exception
    {
        // given
        Cluster cluster = mock( Cluster.class );
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, null );

        // when
        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );
        listener.getValue().enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );

        // then
        assertThat( members.getMembers(), hasItems(
                sameMemberAs( new ClusterMember( clusterId1 ) ),
                sameMemberAs( new ClusterMember( clusterId2 ) ),
                sameMemberAs( new ClusterMember( clusterId3 ) ) ));
    }

    @Test
    public void joinedMemberShowsInList() throws Exception
    {
        // given
        Cluster cluster = mock( Cluster.class );
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, null );

        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );

        listener.getValue().enteredCluster( clusterConfiguration( clusterUri1, clusterUri2 ) );

        // when
        listener.getValue().joinedCluster( clusterId3, clusterUri3 );

        // then
        assertThat( members.getMembers(), hasItems(
                sameMemberAs( new ClusterMember( clusterId1 ) ),
                sameMemberAs( new ClusterMember( clusterId2 ) ),
                sameMemberAs( new ClusterMember( clusterId3 ) ) ) );
    }

    @Test
    public void iCanGetToMyself() throws Exception
    {
        // given
        Cluster cluster = mock( Cluster.class );
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, clusterId1 );

        // when

        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );

        listener.getValue().enteredCluster( clusterConfiguration( clusterUri1, clusterUri2 ) );

        ClusterMember me = members.getSelf();
        assertNotNull( me );
        assertEquals( 1, me.getInstanceId().toIntegerIndex() );
        assertEquals( clusterId1, me.getInstanceId() );
    }

    @Test
    public void leftMemberDisappearsFromList() throws Exception
    {
        // given
        Cluster cluster = mock( Cluster.class );
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, null );

        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );

        listener.getValue().enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );

        // when
        listener.getValue().leftCluster( clusterId3, clusterUri3 );

        // then
        assertThat(
                members.getMembers(),
                not( hasItems( sameMemberAs( new ClusterMember( clusterId3 ) ) ) ));
    }

    @Test
    public void availableMasterShowsProperInformation() throws Exception
    {
        // given
        Cluster cluster = mock(Cluster.class);
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, null );

        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );
        listener.getValue().enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );

        ArgumentCaptor<ClusterMemberListener> clusterMemberListener = ArgumentCaptor.forClass( ClusterMemberListener.class );
        verify( clusterMemberEvents ).addClusterMemberListener( clusterMemberListener.capture() );

        // when
        clusterMemberListener.getValue().memberIsAvailable( MASTER, clusterId1, haUri1, StoreId.DEFAULT );

        // then
        assertThat(
                members.getMembers(),
                hasItem( sameMemberAs( new ClusterMember( clusterId1 ).availableAs(
                        MASTER, haUri1, StoreId.DEFAULT ) ) ) );
    }

    @Test
    public void availableSlaveShowsProperInformation() throws Exception
    {
        // given
        Cluster cluster = mock(Cluster.class);
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, null );

        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );
        listener.getValue().enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );

        ArgumentCaptor<ClusterMemberListener> clusterMemberListener = ArgumentCaptor.forClass( ClusterMemberListener.class );
        verify( clusterMemberEvents ).addClusterMemberListener( clusterMemberListener.capture() );

        // when
        clusterMemberListener.getValue().memberIsAvailable( SLAVE, clusterId1, haUri1, StoreId.DEFAULT );

        // then
        assertThat(
                members.getMembers(),
                hasItem( sameMemberAs( new ClusterMember(
                        clusterId1 ).availableAs( SLAVE, haUri1, StoreId.DEFAULT ) ) ) );
    }

    @Test
    public void membersShowsAsUnavailableWhenNewMasterElectedBeforeTheyBecomeAvailable() throws Exception
    {
        // given
        Cluster cluster = mock(Cluster.class);
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, null );

        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );
        listener.getValue().enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );

        ArgumentCaptor<ClusterMemberListener> clusterMemberListener = ArgumentCaptor.forClass( ClusterMemberListener.class );
        verify( clusterMemberEvents ).addClusterMemberListener( clusterMemberListener.capture() );
        clusterMemberListener.getValue().memberIsAvailable( SLAVE, clusterId1, haUri1, StoreId.DEFAULT );

        // when
        clusterMemberListener.getValue().coordinatorIsElected( clusterId2 );

        // then
        assertThat(
                members.getMembers(),
                hasItem( sameMemberAs( new ClusterMember( clusterId1 ) ) ) );
    }

    @Test
    public void failedMemberShowsAsSuch() throws Exception
    {
        // given
        Cluster cluster = mock(Cluster.class);
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, null );

        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );
        listener.getValue().enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );

        ArgumentCaptor<HeartbeatListener> heartBeatListener = ArgumentCaptor.forClass( HeartbeatListener.class );
        verify( heartbeat ).addHeartbeatListener( heartBeatListener.capture() );

        // when
        heartBeatListener.getValue().failed( clusterId1);

        // then
        assertThat(
                members.getMembers(),
                hasItem( sameMemberAs( new ClusterMember(
                        clusterId1 ).failed() ) ) );
    }

    @Test
    public void failedThenAliveMemberShowsAsAlive() throws Exception
    {
        // given
        Cluster cluster = mock(Cluster.class);
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, null );

        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );
        listener.getValue().enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );

        ArgumentCaptor<HeartbeatListener> heartBeatListener = ArgumentCaptor.forClass( HeartbeatListener.class );
        verify( heartbeat ).addHeartbeatListener( heartBeatListener.capture() );

        // when
        heartBeatListener.getValue().failed( clusterId1 );
        heartBeatListener.getValue().alive( clusterId1 );

        // then
        assertThat(
                members.getMembers(),
                hasItem( sameMemberAs( new ClusterMember( clusterId1 ) ) ) );
    }

    @Test
    public void missingMasterUnavailabilityEventDoesNotClobberState() throws Exception
    {
        // given
        Cluster cluster = mock(Cluster.class);
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, clusterId1 );

        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );
        listener.getValue().enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );

        ArgumentCaptor<ClusterMemberListener> clusterMemberListener = ArgumentCaptor.forClass( ClusterMemberListener.class );
        verify( clusterMemberEvents ).addClusterMemberListener( clusterMemberListener.capture() );

        // when
        // first we are available as slaves
        clusterMemberListener.getValue().memberIsAvailable( SLAVE, clusterId1, haUri1, StoreId.DEFAULT );
        // and then for some reason as master, without an unavailable message in between
        clusterMemberListener.getValue().memberIsAvailable( MASTER, clusterId1, haUri1, StoreId.DEFAULT );

        // then
        assertThat( members.getSelf().getHARole(), equalTo( MASTER ) );
    }

    @Test
    public void missingSlaveUnavailabilityEventDoesNotClobberState() throws Exception
    {
        // given
        Cluster cluster = mock(Cluster.class);
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, clusterId1 );

        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );
        listener.getValue().enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );

        ArgumentCaptor<ClusterMemberListener> clusterMemberListener = ArgumentCaptor.forClass( ClusterMemberListener.class );
        verify( clusterMemberEvents ).addClusterMemberListener( clusterMemberListener.capture() );

        // when
        // first we are available as master
        clusterMemberListener.getValue().memberIsAvailable( MASTER, clusterId1, haUri1, StoreId.DEFAULT );
        // and then for some reason as slave, without an unavailable message in between
        clusterMemberListener.getValue().memberIsAvailable( SLAVE, clusterId1, haUri1, StoreId.DEFAULT );

        // then
        assertThat( members.getSelf().getHARole(), equalTo( SLAVE ) );
    }

    @Test
    public void missingMasterUnavailabilityEventForOtherInstanceStillRemovesBackupRole() throws Exception
    {
        // given
        Cluster cluster = mock(Cluster.class);
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, clusterId1 );
        // initialized with the members of the cluster
        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );
        listener.getValue().enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );

        ArgumentCaptor<ClusterMemberListener> clusterMemberListener = ArgumentCaptor.forClass( ClusterMemberListener.class );
        verify( clusterMemberEvents ).addClusterMemberListener( clusterMemberListener.capture() );

        // instance 2 is available as MASTER and BACKUP
        clusterMemberListener.getValue().memberIsAvailable(
                OnlineBackupKernelExtension.BACKUP, clusterId2, clusterUri2, StoreId.DEFAULT );
        clusterMemberListener.getValue().memberIsAvailable( MASTER, clusterId2, clusterUri2, StoreId.DEFAULT );

        // when - instance 2 becomes available as SLAVE
        clusterMemberListener.getValue().memberIsAvailable( SLAVE, clusterId2, clusterUri2, StoreId.DEFAULT );

        // then - instance 2 should be available ONLY as SLAVE
        for ( ClusterMember clusterMember : members.getMembers() )
        {
            if ( clusterMember.getInstanceId().equals( clusterId2 ) )
            {
                assertThat( count( clusterMember.getRoles() ), equalTo( 1l ) );
                assertThat( Iterables.single( clusterMember.getRoles() ), equalTo( SLAVE ) );
                break; // that's the only member we care about
            }
        }
    }

    @Test
    public void receivingInstanceFailureEventRemovesAllRolesForIt() throws Exception
    {
        // given
        Cluster cluster = mock(Cluster.class);
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock(ClusterMemberEvents.class);

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, clusterId1 );
        // initialized with the members of the cluster
        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );
        listener.getValue().enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );

        ArgumentCaptor<ClusterMemberListener> clusterMemberListener = ArgumentCaptor.forClass( ClusterMemberListener.class );
        verify( clusterMemberEvents ).addClusterMemberListener( clusterMemberListener.capture() );

        // instance 2 is available as MASTER and BACKUP
        clusterMemberListener.getValue().memberIsAvailable(
                OnlineBackupKernelExtension.BACKUP, clusterId2, clusterUri2, StoreId.DEFAULT );
        clusterMemberListener.getValue().memberIsAvailable( MASTER, clusterId2, clusterUri2, StoreId.DEFAULT );

        // when - instance 2 becomes failed
        clusterMemberListener.getValue().memberIsFailed( clusterId2 );

        // then - instance 2 should not be available as any roles
        for ( ClusterMember clusterMember : members.getMembers() )
        {
            if ( clusterMember.getInstanceId().equals( clusterId2 ) )
            {
                assertThat( count( clusterMember.getRoles() ), equalTo( 0l ) );
                break; // that's the only member we care about
            }
        }
    }

    @Test
    public void membersPresentAtJoinTimeShouldHaveInitiallyKnownFlagSetToTrue()
    {
        // Given
        Cluster cluster = mock( Cluster.class );
        final ClusterListener[] listenerSlot = new ClusterListener[1];
        doAnswer( new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocation ) throws Throwable
            {
                listenerSlot[0] = ((ClusterListener) invocation.getArguments()[0]);
                return null;
            }
        } ).when( cluster ).addClusterListener( any( ClusterListener.class ) );
        Heartbeat heartbeat = mock( Heartbeat.class );
        ClusterMemberEvents clusterMemberEvents = mock( ClusterMemberEvents.class );

        ClusterMembers members = new ClusterMembers( cluster, heartbeat, clusterMemberEvents, clusterId1 );
        ClusterListener clusterListener = listenerSlot[0];

        // When
        clusterListener.enteredCluster( clusterConfiguration( clusterUri1, clusterUri2 ) );
        clusterListener.joinedCluster( clusterId3, clusterUri3 );

        // Then
        assertThat( count( members.getMembers() ), equalTo( 3L ) );
        for ( ClusterMember member : members.getMembers() )
        {
            if ( member.getInstanceId().equals( clusterId1 ) || member.getInstanceId().equals( clusterId2 ) )
            {
                assertTrue( member.isInitiallyKnown() );
            }
            else if ( member.getInstanceId().equals( clusterId3 ) )
            {
                assertFalse( member.isInitiallyKnown() );
            }
            else
            {
                fail( "Unexpected member with id: " + member.getInstanceId() );
            }
        }
    }

    private ClusterConfiguration clusterConfiguration( URI... uris )
    {
        ClusterConfiguration toReturn = new ClusterConfiguration( "neo4j.ha", StringLogger.SYSTEM, asList( uris ) );
        toReturn.joined( clusterId1, clusterUri1 );
        toReturn.joined( clusterId2, clusterUri2 );
        if ( uris.length == 3 )
        {
            toReturn.joined( clusterId3, clusterUri3 );
        }
        return toReturn;
    }
}

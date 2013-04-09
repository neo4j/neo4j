/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static java.net.URI.create;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.SLAVE;
import static org.neo4j.kernel.ha.cluster.member.ClusterMemberMatcher.sameMemberAs;

import java.net.URI;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.Heartbeat;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;

public class ClusterMembersTest
{
    private static InstanceId clusterId1 = new InstanceId( 1 );
    private static InstanceId clusterId2 = new InstanceId( 2 );
    private static InstanceId clusterId3 = new InstanceId( 3 );
    private static URI clusterUri1 = create( "cluster://server1" );
    private static URI clusterUri2 = create( "cluster://server2" );
    private static URI clusterUri3 = create( "cluster://server3" );
    private static URI haUri1 = create( "ha://server1?serverId="+clusterId1.toIntegerIndex() );
    private static URI haUri2 = create( "ha://server2?serverId="+clusterId2.toIntegerIndex() );
    private static URI haUri3 = create( "ha://server3?serverId="+clusterId3.toIntegerIndex() );

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
        assertThat( members.getMembers(), CoreMatchers.<ClusterMember>hasItems(
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
        assertThat( members.getMembers(), CoreMatchers.<ClusterMember>hasItems(
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
        assertEquals( 1, me.getInstanceId() );
        assertEquals( clusterId1, me.getMemberId() );
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
        listener.getValue().leftCluster( clusterId3 );

        // then
        assertThat(
                members.getMembers(),
                CoreMatchers.not( CoreMatchers.<ClusterMember>hasItems(
                sameMemberAs( new ClusterMember( clusterId3 ) ) ) ));
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
        clusterMemberListener.getValue().memberIsAvailable( MASTER, clusterId1, haUri1 );

        // then
        assertThat(
                members.getMembers(),
                CoreMatchers.<ClusterMember>hasItem( sameMemberAs( new ClusterMember( clusterId1 ).availableAs(
                        MASTER, haUri1 ) ) ) );
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
        clusterMemberListener.getValue().memberIsAvailable( SLAVE, clusterId1, haUri1 );

        // then
        assertThat(
                members.getMembers(),
                CoreMatchers.<ClusterMember>hasItem( sameMemberAs( new ClusterMember(
                        clusterId1 ).availableAs( SLAVE, haUri1 ) ) ) );
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
        clusterMemberListener.getValue().memberIsAvailable( SLAVE, clusterId1, haUri1 );

        // when
        clusterMemberListener.getValue().coordinatorIsElected( clusterId2 );

        // then
        assertThat(
                members.getMembers(),
                CoreMatchers.<ClusterMember>hasItem( sameMemberAs( new ClusterMember( clusterId1 ) ) ) );
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
                CoreMatchers.<ClusterMember>hasItem( sameMemberAs( new ClusterMember(
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
                CoreMatchers.<ClusterMember>hasItem( sameMemberAs( new ClusterMember( clusterId1 ) ) ) );
    }

    private ClusterConfiguration clusterConfiguration( URI... uris )
    {
        ClusterConfiguration toReturn = new ClusterConfiguration( "neo4j.ha", asList( uris ) );
        toReturn.joined( clusterId1, clusterUri1 );
        toReturn.joined( clusterId2, clusterUri2 );
        if ( uris.length == 3 )
        {
            toReturn.joined( clusterId3, clusterUri3 );
        }
        return toReturn;
    }
}

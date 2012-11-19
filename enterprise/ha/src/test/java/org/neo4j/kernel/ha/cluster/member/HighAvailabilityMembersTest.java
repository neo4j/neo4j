/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.ha.cluster.member.ClusterMemberMatcher.containsMembers;
import static org.neo4j.kernel.ha.cluster.member.ClusterMemberMatcher.containsOnlyMembers;
import static org.neo4j.kernel.ha.cluster.member.ClusterMemberMatcher.member;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;
import org.mockito.Mockito;
import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.ClusterMonitor;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.kernel.ha.cluster.HighAvailability;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;

public class HighAvailabilityMembersTest
{
    @Test
    public void shouldRegisterItselfOnListeners() throws Exception
    {
        // given
        ClusterMonitor clusterMonitor = mock( ClusterMonitor.class );
        HighAvailability highAvailability = mock( HighAvailability.class );

        // when
        new HighAvailabilityMembers( clusterMonitor, highAvailability );

        // then
        verify( clusterMonitor ).addBindingListener( Mockito.<BindingListener>any() );
        verify( clusterMonitor ).addClusterListener( Mockito.<ClusterListener>any() );
        verify( clusterMonitor ).addHeartbeatListener( Mockito.<HeartbeatListener>any() );
        verify( highAvailability ).addHighAvailabilityMemberListener( Mockito.<HighAvailabilityMemberListener>any() );
    }
    
    @Test
    public void shouldContainMemberListAfterEnteringCluster() throws Exception
    {
        // given
        MockedClusterMonitor clusterMonitor = new MockedClusterMonitor();
        MockedHighAvailability highAvailability = new MockedHighAvailability();
        HighAvailabilityMembers members = new HighAvailabilityMembers( clusterMonitor, highAvailability );

        // when
        clusterMonitor.enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );

        // then
        assertThat( members.getMembers(), containsOnlyMembers(
                member( clusterUri1 ).available( false ).alive( true ),
                member( clusterUri2 ).available( false ).alive( true ),
                member( clusterUri3 ).available( false ).alive( true ) ) );
    }
    
    @Test
    public void joinedMemberShowsInList() throws Exception
    {
        // given
        MockedClusterMonitor clusterMonitor = new MockedClusterMonitor();
        MockedHighAvailability highAvailability = new MockedHighAvailability();
        HighAvailabilityMembers members = new HighAvailabilityMembers( clusterMonitor, highAvailability );
        clusterMonitor.enteredCluster( clusterConfiguration( clusterUri1, clusterUri2 ) );

        // when
        clusterMonitor.joinedCluster( clusterUri3 );

        // then
        assertThat( members.getMembers(), containsOnlyMembers(
                member( clusterUri1 ).available( false ),
                member( clusterUri2 ).available( false ),
                member( clusterUri3 ).available( false ) ) );
    }
    
    @Test
    public void leftMemberDissappearsFromList() throws Exception
    {
        // given
        MockedClusterMonitor clusterMonitor = new MockedClusterMonitor();
        MockedHighAvailability highAvailability = new MockedHighAvailability();
        HighAvailabilityMembers members = new HighAvailabilityMembers( clusterMonitor, highAvailability );
        clusterMonitor.enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );
        
        // when
        clusterMonitor.leftCluster( clusterUri2 );
        
        // then
        assertThat( members.getMembers(), containsOnlyMembers(
                member( clusterUri1 ).available( false ),
                member( clusterUri3 ).available( false ) ) );
    }
    
    @Test
    public void availableMasterShowsProperInformation() throws Exception
    {
        // given
        MockedClusterMonitor clusterMonitor = new MockedClusterMonitor();
        MockedHighAvailability highAvailability = new MockedHighAvailability();
        HighAvailabilityMembers members = new HighAvailabilityMembers( clusterMonitor, highAvailability );
        clusterMonitor.listeningAt( clusterUri1 );
        clusterMonitor.enteredCluster( clusterConfiguration( clusterUri1, clusterUri2 ) );

        // when
        highAvailability.masterIsElectedAndAvailable( clusterUri1, haUri1 );

        // then
        assertThat( members.getMembers(), containsMembers(
                member( clusterUri1 ).available( true ).alive( true ).haRole( "MASTER" ).uris( clusterUri1, haUri1 ) ) );
    }

    @Test
    public void availableSlaveShowsProperInformation() throws Exception
    {
        // given
        MockedClusterMonitor clusterMonitor = new MockedClusterMonitor();
        MockedHighAvailability highAvailability = new MockedHighAvailability();
        HighAvailabilityMembers members = new HighAvailabilityMembers( clusterMonitor, highAvailability );
        clusterMonitor.listeningAt( clusterUri1 );
        clusterMonitor.enteredCluster( clusterConfiguration( clusterUri1, clusterUri2 ) );
        highAvailability.masterIsElectedAndAvailable( clusterUri1, haUri1 );

        // when
        highAvailability.slaveIsAvailable( clusterUri2, haUri2 );

        // then
        assertThat( members.getMembers(), containsMembers(
                member( clusterUri2 ).available( true ).alive( true ).haRole( "SLAVE" ).uris( clusterUri2, haUri2 ) ) );
    }
    
    @Test
    public void membersShowsAsUnavailableWhenNewMasterElectedBeforeTheyBecomeAvailable() throws Exception
    {
        // given
        MockedClusterMonitor clusterMonitor = new MockedClusterMonitor();
        MockedHighAvailability highAvailability = new MockedHighAvailability();
        HighAvailabilityMembers members = new HighAvailabilityMembers( clusterMonitor, highAvailability );
        clusterMonitor.listeningAt( clusterUri1 );
        clusterMonitor.enteredCluster( clusterConfiguration( clusterUri1, clusterUri2, clusterUri3 ) );
        highAvailability.masterIsElectedAndAvailable( clusterUri1, haUri1 );
        highAvailability.slaveIsAvailable( clusterUri2, haUri2 );
        highAvailability.slaveIsAvailable( clusterUri3, haUri3 );

        // when
        highAvailability.masterIsElected( clusterUri2, haUri2 );

        // then
        assertThat( members.getMembers(), containsMembers(
                member( clusterUri1 ).available( false ).alive( true ),
                member( clusterUri2 ).available( false ).alive( true ),
                member( clusterUri3 ).available( false ).alive( true ) ) );
    }
    
    @Test
    public void failedMemberShowsAsSuch() throws Exception
    {
        // given
        MockedClusterMonitor clusterMonitor = new MockedClusterMonitor();
        MockedHighAvailability highAvailability = new MockedHighAvailability();
        HighAvailabilityMembers members = new HighAvailabilityMembers( clusterMonitor, highAvailability );
        clusterMonitor.listeningAt( clusterUri1 );
        clusterMonitor.enteredCluster( clusterConfiguration( clusterUri1, clusterUri2 ) );
        highAvailability.masterIsElectedAndAvailable( clusterUri1, haUri1 );
        highAvailability.slaveIsAvailable( clusterUri2, haUri2 );

        // when
        clusterMonitor.failed( clusterUri2 );

        // then
        assertThat( members.getMembers(), containsMembers(
                member( clusterUri1 ).available( true ).haRole( "MASTER" ),
                member( clusterUri2 ).available( true ).alive( false ) ) );
    }
    
    @Test
    public void failedThenAliveMemberShowsAsAlive() throws Exception
    {
        // given
        MockedClusterMonitor clusterMonitor = new MockedClusterMonitor();
        MockedHighAvailability highAvailability = new MockedHighAvailability();
        HighAvailabilityMembers members = new HighAvailabilityMembers( clusterMonitor, highAvailability );
        clusterMonitor.listeningAt( clusterUri1 );
        clusterMonitor.enteredCluster( clusterConfiguration( clusterUri1, clusterUri2 ) );
        highAvailability.masterIsElectedAndAvailable( clusterUri1, haUri1 );
        highAvailability.slaveIsAvailable( clusterUri2, haUri2 );
        clusterMonitor.failed( clusterUri2 );

        // when
        clusterMonitor.alive( clusterUri2 );

        // then
        assertThat( members.getMembers(), containsMembers(
                member( clusterUri1 ).available( true ).alive( true ).haRole( "MASTER" ),
                member( clusterUri2 ).available( true ).alive( true ).haRole( "SLAVE" ) ) );
    }
    
    private static URI clusterUri1 = uri( "cluster://server1" );
    private static URI clusterUri2 = uri( "cluster://server2" );
    private static URI clusterUri3 = uri( "cluster://server3" );
    private static URI haUri1 = uri( "ha://server1?serverId=1" );
    private static URI haUri2 = uri( "ha://server2?serverId=2" );
    private static URI haUri3 = uri( "ha://server3?serverId=3" );
    
    private static URI uri( String string )
    {
        try
        {
            return new URI( string );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private ClusterConfiguration clusterConfiguration( URI... uris )
    {
        return new ClusterConfiguration( "neo4j.ha", asList( uris ) );
    }
}

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
package org.neo4j.causalclustering.core.consensus.membership;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.neo4j.causalclustering.core.consensus.DirectNetworking;
import org.neo4j.causalclustering.core.consensus.RaftTestFixture;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Inbound;
import org.neo4j.causalclustering.messaging.Message;
import org.neo4j.causalclustering.messaging.Outbound;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.core.consensus.RaftMachine.Timeouts.ELECTION;
import static org.neo4j.causalclustering.core.consensus.RaftMachine.Timeouts.HEARTBEAT;
import static org.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static org.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

@RunWith( MockitoJUnitRunner.class )
public class RaftGroupMembershipTest
{
    @Mock
    private Outbound<MemberId, Message> outbound;

    @Mock
    private Inbound inbound;

    @Test
    public void shouldNotFormGroupWithoutAnyBootstrapping()
    {
        // given
        DirectNetworking net = new DirectNetworking();

        final MemberId[] ids = {member( 0 ), member( 1 ), member( 2 )};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, ids );

        fixture.members().setTargetMembershipSet( new RaftTestGroup( ids ).getMembers() );
        fixture.members().invokeTimeout( ELECTION );

        // when
        net.processMessages();

        // then
        assertThat( fixture.members(), hasCurrentMembers( new RaftTestGroup( new int[0] ) ) );
        assertEquals( fixture.messageLog(), 0, fixture.members().withRole( LEADER ).size() );
        assertEquals( fixture.messageLog(), 3, fixture.members().withRole( FOLLOWER ).size() );
    }

    @Test
    public void shouldAddSingleInstanceToExistingRaftGroup() throws Exception
    {
        // given
        DirectNetworking net = new DirectNetworking();

        final MemberId leader = member( 0 );
        final MemberId stable1 = member( 1 );
        final MemberId stable2 = member( 2 );
        final MemberId toBeAdded = member( 3 );

        final MemberId[] initialMembers = {leader, stable1, stable2};
        final MemberId[] finalMembers = {leader, stable1, stable2, toBeAdded};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, finalMembers );
        fixture.bootstrap( initialMembers );

        fixture.members().withId( leader ).timerService().invoke( ELECTION );
        net.processMessages();

        // when
        fixture.members().withId( leader ).raftInstance()
                .setTargetMembershipSet( new RaftTestGroup( finalMembers ).getMembers() );
        net.processMessages();

        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();

        // then
        assertThat( fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestGroup( finalMembers ) ) );
        assertEquals( fixture.messageLog(),1, fixture.members().withRole( LEADER ).size() );
        assertEquals( fixture.messageLog(),3, fixture.members().withRole( FOLLOWER ).size() );
    }

    @Test
    public void shouldAddMultipleInstancesToExistingRaftGroup() throws Exception
    {
        // given
        DirectNetworking net = new DirectNetworking();

        final MemberId leader = member( 0 );
        final MemberId stable1 = member( 1 );
        final MemberId stable2 = member( 2 );
        final MemberId toBeAdded1 = member( 3 );
        final MemberId toBeAdded2 = member( 4 );
        final MemberId toBeAdded3 = member( 5 );

        final MemberId[] initialMembers = {leader, stable1, stable2};
        final MemberId[] finalMembers = {leader, stable1, stable2, toBeAdded1, toBeAdded2, toBeAdded3};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, finalMembers );
        fixture.bootstrap( initialMembers );

        fixture.members().withId( leader ).timerService().invoke( ELECTION );
        net.processMessages();

        // when
        fixture.members().setTargetMembershipSet( new RaftTestGroup( finalMembers ).getMembers() );
        net.processMessages();

        // We need a heartbeat for every member we add. It is necessary to have the new members report their state
        // so their membership change can be processed. We can probably do better here.
        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();
        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();
        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();

        // then
        assertThat( fixture.messageLog(), fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestGroup( finalMembers ) ) );
        assertEquals( fixture.messageLog(), 1, fixture.members().withRole( LEADER ).size() );
        assertEquals( fixture.messageLog(), 5, fixture.members().withRole( FOLLOWER ).size() );
    }

    @Test
    public void shouldRemoveSingleInstanceFromExistingRaftGroup() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final MemberId leader = member( 0 );
        final MemberId stable = member( 1 );
        final MemberId toBeRemoved = member( 2 );

        final MemberId[] initialMembers = {leader, stable, toBeRemoved};
        final MemberId[] finalMembers = {leader, stable};

        RaftTestFixture fixture = new RaftTestFixture( net, 2, initialMembers );
        fixture.bootstrap( initialMembers );

        fixture.members().withId( leader ).timerService().invoke( ELECTION );

        // when
        fixture.members().setTargetMembershipSet( new RaftTestGroup( finalMembers ).getMembers() );
        net.processMessages();

        // then
        assertThat( fixture.messageLog(), fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestGroup( finalMembers ) ) );
        assertEquals( fixture.messageLog(), 1, fixture.members().withIds( finalMembers ).withRole( LEADER ).size() );
        assertEquals( fixture.messageLog(), 1, fixture.members().withIds( finalMembers ).withRole( FOLLOWER ).size() );
    }

    @Test
    public void shouldRemoveMultipleInstancesFromExistingRaftGroup() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final MemberId leader = member( 0 );
        final MemberId stable = member( 1 );
        final MemberId toBeRemoved1 = member( 2 );
        final MemberId toBeRemoved2 = member( 3 );
        final MemberId toBeRemoved3 = member( 4 );

        final MemberId[] initialMembers = {leader, stable, toBeRemoved1, toBeRemoved2, toBeRemoved3};
        final MemberId[] finalMembers = {leader, stable};

        RaftTestFixture fixture = new RaftTestFixture( net, 2, initialMembers );
        fixture.bootstrap( initialMembers );

        fixture.members().withId( leader ).timerService().invoke( ELECTION );
        net.processMessages();

        // when
        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet( new RaftTestGroup( finalMembers ).getMembers() );
        net.processMessages();

        // then
        assertThat( fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestGroup( finalMembers ) ) );
        assertEquals( fixture.messageLog(), 1, fixture.members().withIds( finalMembers ).withRole( LEADER ).size() );
        assertEquals( fixture.messageLog(), 1, fixture.members().withIds( finalMembers ).withRole( FOLLOWER ).size() );
    }

    @Test
    public void shouldHandleMixedChangeToExistingRaftGroup() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final MemberId leader = member( 0 );
        final MemberId stable = member( 1 );
        final MemberId toBeRemoved1 = member( 2 );
        final MemberId toBeRemoved2 = member( 3 );
        final MemberId toBeAdded1 = member( 4 );
        final MemberId toBeAdded2 = member( 5 );

        final MemberId[] everyone = {leader, stable, toBeRemoved1, toBeRemoved2, toBeAdded1, toBeAdded2};

        final MemberId[] initialMembers = {leader, stable, toBeRemoved1, toBeRemoved2};
        final MemberId[] finalMembers = {leader, stable, toBeAdded1, toBeAdded2};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, everyone );
        fixture.bootstrap( initialMembers );

        fixture.members().withId( leader ).timerService().invoke( ELECTION );
        net.processMessages();

        // when
        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet(
                new RaftTestGroup( finalMembers ).getMembers() );
        net.processMessages();

        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();
        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();
        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();

        // then
        assertThat( fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestGroup( finalMembers ) ) );
        assertEquals( fixture.messageLog(), 1, fixture.members().withIds( finalMembers ).withRole( LEADER ).size() );
        assertEquals( fixture.messageLog(), 3, fixture.members().withIds( finalMembers ).withRole( FOLLOWER ).size() );
    }

    @Test
    public void shouldRemoveLeaderFromExistingRaftGroupAndActivelyTransferLeadership() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final MemberId leader = member( 0 );
        final MemberId stable1 = member( 1 );
        final MemberId stable2 = member( 2 );

        final MemberId[] initialMembers = {leader, stable1, stable2};
        final MemberId[] finalMembers = {stable1, stable2};

        RaftTestFixture fixture = new RaftTestFixture( net, 2, initialMembers );
        fixture.bootstrap( initialMembers );
        fixture.members().withId( leader ).timerService().invoke( ELECTION );
        net.processMessages();

        // when
        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet( new RaftTestGroup( finalMembers ).getMembers() );
        net.processMessages();

        fixture.members().withId( stable1 ).timerService().invoke( ELECTION );
        net.processMessages();

        // then
        assertThat( fixture.messageLog(), fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestGroup( finalMembers ) ) );
        assertTrue( fixture.messageLog(), fixture.members().withId( stable1 ).raftInstance().isLeader() ||
                fixture.members().withId( stable2 ).raftInstance().isLeader() );
    }

    @Test
    public void shouldRemoveLeaderAndAddItBackIn() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final MemberId leader1 = member( 0 );
        final MemberId leader2 = member( 1 );
        final MemberId stable1 = member( 2 );
        final MemberId stable2 = member( 3 );

        final MemberId[] allMembers = {leader1, leader2, stable1, stable2};
        final MemberId[] fewerMembers = {leader2, stable1, stable2};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, allMembers );
        fixture.bootstrap( allMembers );

        // when
        fixture.members().withId( leader1 ).timerService().invoke( ELECTION );
        net.processMessages();

        fixture.members().withId( leader1 ).raftInstance().setTargetMembershipSet( new RaftTestGroup( fewerMembers )
                .getMembers() );
        net.processMessages();

        fixture.members().withId( leader2 ).timerService().invoke( ELECTION );
        net.processMessages();

        fixture.members().withId( leader2 ).raftInstance().setTargetMembershipSet( new RaftTestGroup( allMembers )
                .getMembers() );
        net.processMessages();

        fixture.members().withId( leader2 ).timerService().invoke( HEARTBEAT );
        net.processMessages();

        // then
        assertTrue( fixture.messageLog(), fixture.members().withId( leader2 ).raftInstance().isLeader() );
        assertThat( fixture.messageLog(), fixture.members().withIds( allMembers ), hasCurrentMembers( new RaftTestGroup( allMembers ) ) );
    }

    @Test
    public void shouldRemoveFollowerAndAddItBackIn() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final MemberId leader = member( 0 );
        final MemberId unstable = member( 1 );
        final MemberId stable1 = member( 2 );
        final MemberId stable2 = member( 3 );

        final MemberId[] allMembers = {leader, unstable, stable1, stable2};
        final MemberId[] fewerMembers = {leader, stable1, stable2};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, allMembers );
        fixture.bootstrap( allMembers );

        // when
        fixture.members().withId( leader ).timerService().invoke( ELECTION );
        net.processMessages();

        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet( new RaftTestGroup( fewerMembers ).getMembers() );
        net.processMessages();

        assertTrue( fixture.members().withId( leader ).raftInstance().isLeader() );
        assertThat( fixture.members().withIds( fewerMembers ), hasCurrentMembers( new RaftTestGroup( fewerMembers ) ) );

        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet( new RaftTestGroup( allMembers ).getMembers() );
        net.processMessages();

        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();

        // then
        assertTrue( fixture.messageLog(), fixture.members().withId( leader ).raftInstance().isLeader() );
        assertThat( fixture.messageLog(), fixture.members().withIds( allMembers ), hasCurrentMembers( new RaftTestGroup( allMembers ) ) );
    }

    @Test
    public void shouldElectNewLeaderWhenOldOneAbruptlyLeaves() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final MemberId leader1 = member( 0 );
        final MemberId leader2 = member( 1 );
        final MemberId stable = member( 2 );

        final MemberId[] initialMembers = {leader1, leader2, stable};

        RaftTestFixture fixture = new RaftTestFixture( net, 2, initialMembers );
        fixture.bootstrap( initialMembers );

        fixture.members().withId( leader1 ).timerService().invoke( ELECTION );
        net.processMessages();

        // when
        net.disconnect( leader1 );
        fixture.members().withId( leader2 ).timerService().invoke( ELECTION );
        net.processMessages();

        // then
        assertTrue( fixture.messageLog(), fixture.members().withId( leader2 ).raftInstance().isLeader() );
        assertFalse( fixture.messageLog(), fixture.members().withId( stable ).raftInstance().isLeader() );
        assertEquals( fixture.messageLog(), 1, fixture.members().withIds( leader2, stable ).withRole( LEADER ).size() );
        assertEquals( fixture.messageLog(), 1, fixture.members().withIds( leader2, stable ).withRole( FOLLOWER ).size() );
    }

    private Matcher<? super RaftTestFixture.Members> hasCurrentMembers( final RaftTestGroup raftGroup )
    {
        return new TypeSafeMatcher<RaftTestFixture.Members>()
        {
            @Override
            protected boolean matchesSafely( RaftTestFixture.Members members )
            {
                for ( RaftTestFixture.MemberFixture finalMember : members )
                {
                    if ( !raftGroup.equals( new RaftTestGroup( finalMember.raftInstance().replicationMembers() ) ) )
                    {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Raft group: " ).appendValue( raftGroup );
            }
        };
    }
}

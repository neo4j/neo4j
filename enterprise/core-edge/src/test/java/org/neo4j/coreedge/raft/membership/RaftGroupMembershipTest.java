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
package org.neo4j.coreedge.raft.membership;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.neo4j.coreedge.raft.DirectNetworking;
import org.neo4j.coreedge.raft.RaftTestFixture;
import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.server.RaftTestMember;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.coreedge.raft.RaftInstance.Timeouts.ELECTION;
import static org.neo4j.coreedge.raft.RaftInstance.Timeouts.HEARTBEAT;
import static org.neo4j.coreedge.raft.roles.Role.FOLLOWER;
import static org.neo4j.coreedge.raft.roles.Role.LEADER;

@RunWith(MockitoJUnitRunner.class)
public class RaftGroupMembershipTest
{
    @Mock
    private Outbound<RaftTestMember> outbound;

    @Mock
    private Inbound inbound;

    @Test
    public void shouldNotFormGroupWithoutAnyBootstrapping() throws Exception
    {
        // given
        DirectNetworking net = new DirectNetworking();

        final long[] ids = {0, 1, 2};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, ids );

        fixture.members().setTargetMembershipSet( new RaftTestGroup( ids ).getMembers() );
        fixture.members().invokeTimeout( ELECTION );

        // when
        net.processMessages();

        // then
        assertThat( fixture.members(), hasCurrentMembers( new RaftTestGroup() ) );
        assertEquals( 0, fixture.members().withRole( LEADER ).size() );
        assertEquals( 3, fixture.members().withRole( FOLLOWER ).size() );
    }

    @Test
    public void shouldAddSingleInstanceToExistingRaftGroup() throws Exception
    {
        // given
        DirectNetworking net = new DirectNetworking();

        final long leader = 0;
        final long stable1 = 1;
        final long stable2 = 2;
        final long toBeAdded = 3;

        final long[] initialMembers = {leader, stable1, stable2};
        final long[] finalMembers = {leader, stable1, stable2, toBeAdded};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, finalMembers );

        fixture.members().withId( leader ).raftInstance().bootstrapWithInitialMembers( new RaftTestGroup(
                initialMembers ) );

        fixture.members().withId( leader ).timeoutService().invokeTimeout( ELECTION );
        net.processMessages();

        // when
        fixture.members().withId( leader ).raftInstance()
                .setTargetMembershipSet( new RaftTestGroup( finalMembers ).getMembers() );
        net.processMessages();

        fixture.members().withId( leader ).timeoutService().invokeTimeout( HEARTBEAT );
        net.processMessages();

        // then
        assertThat( fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestGroup( finalMembers ) ) );
        assertEquals( 1, fixture.members().withRole( LEADER ).size() );
        assertEquals( 3, fixture.members().withRole( FOLLOWER ).size() );
    }

    @Test
    public void shouldAddMultipleInstancesToExistingRaftGroup() throws Exception
    {
        // given
        DirectNetworking net = new DirectNetworking();

        final long leader = 0;
        final long stable1 = 1;
        final long stable2 = 2;
        final long toBeAdded1 = 3;
        final long toBeAdded2 = 4;
        final long toBeAdded3 = 5;

        final long[] initialMembers = {leader, stable1, stable2};
        final long[] finalMembers = {leader, stable1, stable2, toBeAdded1, toBeAdded2, toBeAdded3};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, finalMembers );

        fixture.members().withId( leader ).raftInstance().bootstrapWithInitialMembers( new RaftTestGroup( initialMembers ) );
        fixture.members().withId( leader ).timeoutService().invokeTimeout( ELECTION );
        net.processMessages();

        // when
        fixture.members().setTargetMembershipSet( new RaftTestGroup( finalMembers ).getMembers() );
        net.processMessages();

        // We need a heartbeat for every member we add. It is necessary to have the new members report their state
        // so their membership change can be processed. We can probably do better here.
        fixture.members().withId( leader ).timeoutService().invokeTimeout( HEARTBEAT );
        net.processMessages();
        fixture.members().withId( leader ).timeoutService().invokeTimeout( HEARTBEAT );
        net.processMessages();
        fixture.members().withId( leader ).timeoutService().invokeTimeout( HEARTBEAT );
        net.processMessages();

        // then
        assertThat( fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestGroup( finalMembers ) ) );
        assertEquals( 1, fixture.members().withRole( LEADER ).size() );
        assertEquals( 5, fixture.members().withRole( FOLLOWER ).size() );
    }

    @Test
    public void shouldRemoveSingleInstanceFromExistingRaftGroup() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final long leader = 0;
        final long stable = 1;
        final long toBeRemoved = 2;

        final long[] initialMembers = {leader, stable, toBeRemoved};
        final long[] finalMembers = {leader, stable};

        RaftTestFixture fixture = new RaftTestFixture( net, 2, initialMembers );

        fixture.members().withId( leader ).raftInstance().bootstrapWithInitialMembers( new RaftTestGroup( initialMembers ) );
        fixture.members().withId( leader ).timeoutService().invokeTimeout( ELECTION );

        // when
        fixture.members().setTargetMembershipSet( new RaftTestGroup( finalMembers ).getMembers() );
        net.processMessages();

        // then
        assertThat( fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestGroup( finalMembers ) ) );
        assertEquals( 1, fixture.members().withIds( finalMembers ).withRole( LEADER ).size() );
        assertEquals( 1, fixture.members().withIds( finalMembers ).withRole( FOLLOWER ).size() );
    }

    @Test
    public void shouldRemoveMultipleInstancesFromExistingRaftGroup() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final long leader = 0;
        final long stable = 1;
        final long toBeRemoved1 = 2;
        final long toBeRemoved2 = 3;
        final long toBeRemoved3 = 4;

        final long[] initialMembers = {leader, stable, toBeRemoved1, toBeRemoved2, toBeRemoved3};
        final long[] finalMembers = {leader, stable};

        RaftTestFixture fixture = new RaftTestFixture( net, 2, initialMembers );

        fixture.members().withId( leader ).raftInstance().bootstrapWithInitialMembers( new RaftTestGroup( initialMembers ) );
        fixture.members().withId( leader ).timeoutService().invokeTimeout( ELECTION );
        net.processMessages();

        // when
        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet( new RaftTestGroup( finalMembers ).getMembers() );
        net.processMessages();

        // then
        assertThat( fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestGroup( finalMembers ) ) );
        assertEquals( 1, fixture.members().withIds( finalMembers ).withRole( LEADER ).size() );
        assertEquals( 1, fixture.members().withIds( finalMembers ).withRole( FOLLOWER ).size() );
    }

    @Test
    public void shouldHandleMixedChangeToExistingRaftGroup() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final long leader = 0;
        final long stable = 1;
        final long toBeRemoved1 = 2;
        final long toBeRemoved2 = 3;
        final long toBeAdded1 = 4;
        final long toBeAdded2 = 5;

        final long[] everyone = {leader, stable, toBeRemoved1, toBeRemoved2, toBeAdded1, toBeAdded2};

        final long[] initialMembers = {leader, stable, toBeRemoved1, toBeRemoved2};
        final long[] finalMembers = {leader, stable, toBeAdded1, toBeAdded2};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, everyone );

        fixture.members().withId( leader ).raftInstance().bootstrapWithInitialMembers( new RaftTestGroup( initialMembers ) );
        fixture.members().withId( leader ).timeoutService().invokeTimeout( ELECTION );
        net.processMessages();

        // when
        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet( new RaftTestGroup( finalMembers )
                .getMembers() );
        net.processMessages();

        fixture.members().withId( leader ).timeoutService().invokeTimeout( HEARTBEAT );
        net.processMessages();
        fixture.members().withId( leader ).timeoutService().invokeTimeout( HEARTBEAT );
        net.processMessages();
        fixture.members().withId( leader ).timeoutService().invokeTimeout( HEARTBEAT );
        net.processMessages();

        // then
        assertThat( fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestGroup( finalMembers ) ) );
        assertEquals( 1, fixture.members().withIds( finalMembers ).withRole( LEADER ).size() );
        assertEquals( 3, fixture.members().withIds( finalMembers ).withRole( FOLLOWER ).size() );
    }

    @Test
    public void shouldRemoveLeaderFromExistingRaftGroupAndActivelyTransferLeadership() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final long leader = 0;
        final long stable1 = 1;
        final long stable2 = 2;

        final long[] initialMembers = {leader, stable1, stable2};
        final long[] finalMembers = {stable1, stable2};

        RaftTestFixture fixture = new RaftTestFixture( net, 2, initialMembers );

        fixture.members().withId( leader ).raftInstance().bootstrapWithInitialMembers( new RaftTestGroup( initialMembers ) );
        fixture.members().withId( leader ).timeoutService().invokeTimeout( ELECTION );
        net.processMessages();

        // when
        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet( new RaftTestGroup( finalMembers ).getMembers() );
        net.processMessages();

        fixture.members().withId( stable1 ).timeoutService().invokeTimeout( ELECTION );
        net.processMessages();

        // then
        assertThat( fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestGroup( finalMembers ) ) );
        assertTrue( fixture.members().withId( stable1 ).raftInstance().isLeader() ||
                fixture.members().withId( stable2 ).raftInstance().isLeader() );
    }

    @Test
    public void shouldRemoveLeaderAndAddItBackIn() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final long leader1 = 0;
        final long leader2 = 1;
        final long stable1 = 2;
        final long stable2 = 3;

        final long[] allMembers = {leader1, leader2, stable1, stable2};
        final long[] fewerMembers = {leader2, stable1, stable2};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, allMembers );

        // when
        fixture.members().withId( leader1 ).raftInstance().bootstrapWithInitialMembers( new RaftTestGroup( allMembers ) );
        fixture.members().withId( leader1 ).timeoutService().invokeTimeout( ELECTION );
        net.processMessages();

        fixture.members().withId( leader1 ).raftInstance().setTargetMembershipSet( new RaftTestGroup( fewerMembers )
                .getMembers() );
        net.processMessages();

        fixture.members().withId( leader2 ).timeoutService().invokeTimeout( ELECTION );
        net.processMessages();

        fixture.members().withId( leader2 ).raftInstance().setTargetMembershipSet( new RaftTestGroup( allMembers )
                .getMembers() );
        net.processMessages();

        fixture.members().withId( leader2 ).timeoutService().invokeTimeout( HEARTBEAT );
        net.processMessages();

        // then
        assertTrue( fixture.members().withId( leader2 ).raftInstance().isLeader() );
        assertThat( fixture.members().withIds( allMembers ), hasCurrentMembers( new RaftTestGroup( allMembers ) ) );
    }

    @Test
    public void shouldRemoveFollowerAndAddItBackIn() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final long leader = 0;
        final long unstable = 1;
        final long stable1 = 2;
        final long stable2 = 3;

        final long[] allMembers = {leader, unstable, stable1, stable2};
        final long[] fewerMembers = {leader, stable1, stable2};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, allMembers );

        // when
        fixture.members().withId( leader ).raftInstance().bootstrapWithInitialMembers( new RaftTestGroup( allMembers ) );
        fixture.members().withId( leader ).timeoutService().invokeTimeout( ELECTION );
        net.processMessages();

        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet( new RaftTestGroup( fewerMembers ).getMembers() );
        net.processMessages();

        assertTrue( fixture.members().withId( leader ).raftInstance().isLeader() );
        assertThat( fixture.members().withIds( fewerMembers ), hasCurrentMembers( new RaftTestGroup( fewerMembers ) ) );

        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet( new RaftTestGroup( allMembers ).getMembers() );
        net.processMessages();

        fixture.members().withId( leader ).timeoutService().invokeTimeout( HEARTBEAT );
        net.processMessages();

        // then
        assertTrue( fixture.members().withId( leader ).raftInstance().isLeader() );
        assertThat( fixture.members().withIds( allMembers ), hasCurrentMembers( new RaftTestGroup( allMembers ) ) );
    }

    @Test
    public void shouldElectNewLeaderWhenOldOneAbruptlyLeaves() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final long leader1 = 0;
        final long leader2 = 1;
        final long stable = 2;

        final long[] initialMembers = {leader1, leader2, stable};

        RaftTestFixture fixture = new RaftTestFixture( net, 2, initialMembers );

        fixture.members().withId( leader1 ).raftInstance().bootstrapWithInitialMembers( new RaftTestGroup( initialMembers ) );

        fixture.members().withId( leader1 ).timeoutService().invokeTimeout( ELECTION );
        net.processMessages();

        // when
        net.disconnect( leader1 );
        fixture.members().withId( leader2 ).timeoutService().invokeTimeout( ELECTION );
        net.processMessages();

        // then
        assertTrue( fixture.members().withId( leader2 ).raftInstance().isLeader() );
        assertFalse( fixture.members().withId( stable ).raftInstance().isLeader() );
        assertEquals( 1, fixture.members().withIds( leader2, stable ).withRole( LEADER ).size() );
        assertEquals( 1, fixture.members().withIds( leader2, stable ).withRole( FOLLOWER ).size() );
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

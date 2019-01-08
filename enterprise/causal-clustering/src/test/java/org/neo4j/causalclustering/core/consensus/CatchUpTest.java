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
package org.neo4j.causalclustering.core.consensus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.RaftMessages.NewEntry.Request;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import org.neo4j.causalclustering.core.consensus.shipping.RaftLogShipper;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.identity.MemberId;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.causalclustering.core.consensus.log.RaftLogHelper.readLogEntry;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

public class CatchUpTest
{
    @Test
    public void happyClusterPropagatesUpdates() throws Throwable
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final MemberId leader = member( 0 );
        final MemberId[] allMembers = {leader, member( 1 ), member( 2 )};

        final RaftTestFixture fixture = new RaftTestFixture( net, 3, allMembers );
        fixture.bootstrap( allMembers );
        final MemberId leaderMember = fixture.members().withId( leader ).member();

        // when
        fixture.members().withId( leader ).timerService().invoke( RaftMachine.Timeouts.ELECTION );
        net.processMessages();
        fixture.members().withId( leader ).raftInstance().handle( new Request( leaderMember, valueOf( 42 ) ) );
        net.processMessages();

        // then
        for ( MemberId aMember : allMembers )
        {
            assertThat( fixture.messageLog(), integerValues( fixture.members().withId( aMember ).raftLog() ), hasItems( 42 ) );
        }
    }

    @Test
    public void newMemberWithNoLogShouldCatchUpFromPeers() throws Throwable
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final MemberId leaderId = member( 0 );
        final MemberId sleepyId = member( 2 );

        final MemberId[] awakeMembers = {leaderId, member( 1 )};
        final MemberId[] allMembers = {leaderId, member( 1 ), sleepyId};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, allMembers );
        fixture.bootstrap( allMembers );
        fixture.members().withId( leaderId ).raftInstance().installCoreState(
                new RaftCoreState( new MembershipEntry( 0, new HashSet<>( Arrays.asList( allMembers ) ) ) ));

        fixture.members().withId( leaderId ).timerService().invoke( RaftMachine.Timeouts.ELECTION );
        net.processMessages();

        final MemberId leader = fixture.members().withId( leaderId ).member();

        net.disconnect( sleepyId );

        // when
        fixture.members().withId( leaderId ).raftInstance().handle( new Request( leader, valueOf( 10 ) ) );
        fixture.members().withId( leaderId ).raftInstance().handle( new Request( leader, valueOf( 20 ) ) );
        fixture.members().withId( leaderId ).raftInstance().handle( new Request( leader, valueOf( 30 ) ) );
        fixture.members().withId( leaderId ).raftInstance().handle( new Request( leader, valueOf( 40 ) ) );
        net.processMessages();

        // then
        for ( MemberId awakeMember : awakeMembers )
        {
            assertThat( integerValues( fixture.members().withId( awakeMember ).raftLog() ),
                    hasItems( 10, 20, 30, 40 ) );
        }

        assertThat( integerValues( fixture.members().withId( sleepyId ).raftLog() ), empty() );

        // when
        net.reconnect( sleepyId );
        fixture.members().invokeTimeout( RaftLogShipper.Timeouts.RESEND );
        net.processMessages();

        // then
        assertThat( fixture.messageLog(), integerValues( fixture.members().withId( sleepyId ).raftLog() ), hasItems( 10, 20, 30, 40 ) );
    }

    private List<Integer> integerValues( ReadableRaftLog log ) throws IOException
    {
        List<Integer> actual = new ArrayList<>();
        for ( long logIndex = 0; logIndex <= log.appendIndex(); logIndex++ )
        {
            ReplicatedContent content = readLogEntry( log, logIndex ).content();
            if ( content instanceof ReplicatedInteger )
            {
                ReplicatedInteger integer = (ReplicatedInteger) content;
                actual.add( integer.get() );
            }
        }
        return actual;
    }
}

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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.consensus.schedule.OnDemandTimerService;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import org.neo4j.causalclustering.logging.BetterMessageLogger;
import org.neo4j.causalclustering.logging.MessageLogger;
import org.neo4j.causalclustering.messaging.Inbound;
import org.neo4j.causalclustering.messaging.LoggingInbound;
import org.neo4j.causalclustering.messaging.LoggingOutbound;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.lang.String.format;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class RaftTestFixture
{
    private Members members = new Members();
    // Does not need to be closed
    private StringWriter writer = new StringWriter();

    public RaftTestFixture( DirectNetworking net, int expectedClusterSize, MemberId... ids )
    {
        for ( MemberId id : ids )
        {
            MemberFixture fixtureMember = new MemberFixture();

            FakeClock clock = Clocks.fakeClock();
            fixtureMember.timerService = new OnDemandTimerService( clock );

            fixtureMember.raftLog = new InMemoryRaftLog();
            fixtureMember.member = id;

            MessageLogger<MemberId> messageLogger =
                    new BetterMessageLogger<>( id, new PrintWriter( writer ), Clocks.systemClock() );
            Inbound<RaftMessages.RaftMessage> inbound =
                    new LoggingInbound<>( net.new Inbound<>( fixtureMember.member ), messageLogger, fixtureMember.member );
            Outbound<MemberId,RaftMessages.RaftMessage> outbound = new LoggingOutbound<>( net.new Outbound( id ), fixtureMember.member,
                    messageLogger );

            fixtureMember.raftMachine = new RaftMachineBuilder( fixtureMember.member, expectedClusterSize,
                    RaftTestMemberSetBuilder.INSTANCE )
                    .inbound( inbound )
                    .outbound( outbound )
                    .raftLog( fixtureMember.raftLog )
                    .clock( clock )
                    .timerService( fixtureMember.timerService )
                    .build();

            members.put( fixtureMember );
        }
    }

    public Members members()
    {
        return members;
    }

    public void bootstrap( MemberId[] members ) throws IOException
    {
        for ( MemberFixture member : members() )
        {
            member.raftLog().append( new RaftLogEntry( 0, new MemberIdSet( asSet( members ) ) ) );
            member.raftInstance().installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( members ) ) ) );
            member.raftInstance().postRecoveryActions();
        }
    }

    public String messageLog()
    {
        return writer.toString();
    }

    public static class Members implements Iterable<MemberFixture>
    {
        private Map<MemberId, MemberFixture> memberMap = new HashMap<>();

        private MemberFixture put( MemberFixture value )
        {
            return memberMap.put( value.member, value );
        }

        public MemberFixture withId( MemberId id )
        {
            return memberMap.get( id );
        }

        public Members withIds( MemberId... ids )
        {
            Members filteredMembers = new Members();
            for ( MemberId id : ids )
            {
                if ( memberMap.containsKey( id ) )
                {
                    filteredMembers.put( memberMap.get( id ) );
                }
            }
            return filteredMembers;
        }

        public Members withRole( Role role )
        {
            Members filteredMembers = new Members();

            for ( Map.Entry<MemberId,MemberFixture> entry : memberMap.entrySet() )
            {
                if ( entry.getValue().raftInstance().currentRole() == role )
                {
                    filteredMembers.put( entry.getValue() );
                }
            }
            return filteredMembers;
        }

        public void setTargetMembershipSet( Set<MemberId> targetMembershipSet )
        {
            for ( MemberFixture memberFixture : memberMap.values() )
            {
                memberFixture.raftMachine.setTargetMembershipSet( targetMembershipSet );
            }
        }

        public void invokeTimeout( TimerService.TimerName name )
        {
            for ( MemberFixture memberFixture : memberMap.values() )
            {
                memberFixture.timerService.invoke( name );
            }
        }

        @Override
        public Iterator<MemberFixture> iterator()
        {
            return memberMap.values().iterator();
        }

        public int size()
        {
            return memberMap.size();
        }

        @Override
        public String toString()
        {
            return format( "Members%s", memberMap );
        }
    }

    public class MemberFixture
    {
        private MemberId member;
        private RaftMachine raftMachine;
        private OnDemandTimerService timerService;
        private RaftLog raftLog;

        public MemberId member()
        {
            return member;
        }

        public RaftMachine raftInstance()
        {
            return raftMachine;
        }

        public OnDemandTimerService timerService()
        {
            return timerService;
        }

        public RaftLog raftLog()
        {
            return raftLog;
        }

        @Override
        public String toString()
        {
            return "FixtureMember{" +
                    "raftInstance=" + raftMachine +
                    ", timeoutService=" + timerService +
                    ", raftLog=" + raftLog +
                    '}';
        }
    }
}

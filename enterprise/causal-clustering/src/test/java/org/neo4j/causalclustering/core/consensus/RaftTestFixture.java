/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus;

import java.io.IOException;
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
import org.neo4j.causalclustering.core.consensus.schedule.ControlledRenewableTimeoutService;
import org.neo4j.causalclustering.core.consensus.schedule.RenewableTimeoutService;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import org.neo4j.causalclustering.logging.NullMessageLogger;
import org.neo4j.causalclustering.messaging.Inbound;
import org.neo4j.causalclustering.messaging.LoggingOutbound;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.lang.String.format;

import static org.neo4j.helpers.collection.Iterators.asSet;

public class RaftTestFixture
{
    private Members members = new Members();

    public RaftTestFixture( DirectNetworking net, int expectedClusterSize, MemberId... ids )
    {
        for ( MemberId id : ids )
        {
            MemberFixture fixtureMember = new MemberFixture();

            FakeClock clock = Clocks.fakeClock();
            fixtureMember.timeoutService = new ControlledRenewableTimeoutService( clock );

            fixtureMember.raftLog = new InMemoryRaftLog();
            fixtureMember.member = id;

            Inbound inbound = net.new Inbound( fixtureMember.member );
            Outbound<MemberId,RaftMessages.RaftMessage> outbound = new LoggingOutbound<>( net.new Outbound( id ), fixtureMember.member,
                    new NullMessageLogger<>() );

            fixtureMember.raftMachine = new RaftMachineBuilder( fixtureMember.member, expectedClusterSize,
                    RaftTestMemberSetBuilder.INSTANCE )
                    .inbound( inbound )
                    .outbound( outbound )
                    .raftLog( fixtureMember.raftLog )
                    .clock( clock )
                    .timeoutService( fixtureMember.timeoutService )
                    .build();

            members.put( fixtureMember );
        }
    }

    public Members members()
    {
        return members;
    }

    public void bootstrap( MemberId[] members ) throws RaftMachine.BootstrapException, IOException
    {
        for ( MemberFixture member : members() )
        {
            member.raftLog().append( new RaftLogEntry(0, new MemberIdSet(asSet( members ))) );
            member.raftInstance().installCoreState( new RaftCoreState( new MembershipEntry( 0,  asSet( members )) ) );
            member.raftInstance().startTimers();
        }
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

        public void invokeTimeout( RenewableTimeoutService.TimeoutName name )
        {
            for ( MemberFixture memberFixture : memberMap.values() )
            {
                memberFixture.timeoutService.invokeTimeout( name );
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
        private ControlledRenewableTimeoutService timeoutService;
        private RaftLog raftLog;

        public MemberId member()
        {
            return member;
        }

        public RaftMachine raftInstance()
        {
            return raftMachine;
        }

        public ControlledRenewableTimeoutService timeoutService()
        {
            return timeoutService;
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
                    ", timeoutService=" + timeoutService +
                    ", raftLog=" + raftLog +
                    '}';
        }
    }
}

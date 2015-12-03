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
package org.neo4j.coreedge.raft;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.membership.RaftTestGroup;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.server.RaftTestMemberSetBuilder;
import org.neo4j.coreedge.raft.net.LoggingOutbound;
import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.server.logging.MessageLogger;
import org.neo4j.coreedge.server.logging.NullMessageLogger;
import org.neo4j.helpers.Clock;

import static java.lang.String.format;
import static org.neo4j.coreedge.server.RaftTestMember.member;

public class RaftTestFixture
{
    private Members members = new Members();

    public RaftTestFixture( DirectNetworking net, int expectedClusterSize, long... ids )
    {
        this( Clock.SYSTEM_CLOCK, net, new NullMessageLogger<>(), expectedClusterSize, ids );
    }

    public RaftTestFixture( Clock clock, DirectNetworking net, int expectedClusterSize, long... ids )
    {
        this( clock, net, new NullMessageLogger<>(), expectedClusterSize, ids );
    }

    public RaftTestFixture( Clock clock, DirectNetworking net, MessageLogger<RaftTestMember> logger,
                            int expectedClusterSize, long... ids )
    {
        for ( long id : ids )
        {
            MemberFixture fixtureMember = new MemberFixture();

            fixtureMember.timeoutService = new ControlledRenewableTimeoutService();

            fixtureMember.raftLog = new InMemoryRaftLog();
            fixtureMember.member = member( id );

            Inbound inbound = net.new Inbound( id );
            Outbound<RaftTestMember> outbound = new LoggingOutbound<>( net.new Outbound( id ), fixtureMember.member, new NullMessageLogger<>() );

            fixtureMember.raftInstance = new RaftInstanceBuilder<>( fixtureMember.member, expectedClusterSize,
                    RaftTestMemberSetBuilder.INSTANCE )
                    .inbound( inbound )
                    .outbound( outbound )
                    .raftLog ( fixtureMember.raftLog )
                    .timeoutService( fixtureMember.timeoutService )
                    .build();

            members.put( id, fixtureMember );
        }
    }

    public Members members()
    {
        return members;
    }

    public static class Members implements Iterable<MemberFixture>
    {
        private Map<Long, MemberFixture> memberMap = new HashMap<>();

        private MemberFixture put( Long key, MemberFixture value )
        {
            return memberMap.put( key, value );
        }

        public MemberFixture withId( long id )
        {
            return memberMap.get( id );
        }

        public Members withIds( long... ids )
        {
            Members filteredMembers = new Members();
            for ( long id : ids )
            {
                if ( memberMap.containsKey( id ) )
                {
                    filteredMembers.put( id, memberMap.get( id ) );
                }
            }
            return filteredMembers;
        }

        public Members withRole( Role role )
        {
            Members filteredMembers = new Members();

            for ( Map.Entry<Long, MemberFixture> entry : memberMap.entrySet() )
            {
                if ( entry.getValue().raftInstance().currentRole() == role )
                {
                    filteredMembers.put( entry.getKey(), entry.getValue() );
                }
            }
            return filteredMembers;
        }

        public void setTargetMembershipSet( Set<RaftTestMember> targetMembershipSet )
        {
            for ( MemberFixture memberFixture : memberMap.values() )
            {
                memberFixture.raftInstance.setTargetMembershipSet( targetMembershipSet );
            }
        }

        public void invokeTimeout( RenewableTimeoutService.TimeoutName name )
        {
            for ( MemberFixture memberFixture : memberMap.values() )
            {
                memberFixture.timeoutService.invokeTimeout( name );
            }
        }

        public void bootstrapWithInitialMembers( RaftTestGroup raftTestGroup ) throws RaftInstance.BootstrapException
        {
            for ( MemberFixture memberFixture : memberMap.values() )
            {
                memberFixture.raftInstance.bootstrapWithInitialMembers( raftTestGroup );
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
        private RaftTestMember member;
        private RaftInstance<RaftTestMember> raftInstance;
        private ControlledRenewableTimeoutService timeoutService;
        private RaftLog raftLog;

        public RaftTestMember member()
        {
            return member;
        }

        public RaftInstance<RaftTestMember> raftInstance()
        {
            return raftInstance;
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
                    "raftInstance=" + raftInstance +
                    ", timeoutService=" + timeoutService +
                    ", raftLog=" + raftLog +
                    '}';
        }
    }
}

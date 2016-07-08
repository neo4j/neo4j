/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.raft.net.LoggingOutbound;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.RaftTestMemberSetBuilder;
import org.neo4j.coreedge.server.logging.NullMessageLogger;

import static java.lang.String.format;

public class RaftTestFixture
{
    private Members members = new Members();

    public RaftTestFixture( DirectNetworking net, int expectedClusterSize, CoreMember... ids )
    {
        for ( CoreMember id : ids )
        {
            MemberFixture fixtureMember = new MemberFixture();

            fixtureMember.timeoutService = new ControlledRenewableTimeoutService();

            fixtureMember.raftLog = new InMemoryRaftLog();
            fixtureMember.member = id;

            Inbound inbound = net.new Inbound( fixtureMember.member );
            Outbound<CoreMember,RaftMessages.RaftMessage> outbound = new LoggingOutbound<>( net.new Outbound( id ), fixtureMember.member,
                    new NullMessageLogger<>() );

            fixtureMember.raftInstance = new RaftInstanceBuilder( fixtureMember.member, expectedClusterSize,
                    RaftTestMemberSetBuilder.INSTANCE )
                    .inbound( inbound )
                    .outbound( outbound )
                    .raftLog( fixtureMember.raftLog )
                    .timeoutService( fixtureMember.timeoutService )
                    .build();

            members.put( fixtureMember );
        }
    }

    public Members members()
    {
        return members;
    }

    public static class Members implements Iterable<MemberFixture>
    {
        private Map<CoreMember, MemberFixture> memberMap = new HashMap<>();

        private MemberFixture put( MemberFixture value )
        {
            return memberMap.put( value.member, value );
        }

        public MemberFixture withId( CoreMember id )
        {
            return memberMap.get( id );
        }

        public Members withIds( CoreMember... ids )
        {
            Members filteredMembers = new Members();
            for ( CoreMember id : ids )
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

            for ( Map.Entry<CoreMember,MemberFixture> entry : memberMap.entrySet() )
            {
                if ( entry.getValue().raftInstance().currentRole() == role )
                {
                    filteredMembers.put( entry.getValue() );
                }
            }
            return filteredMembers;
        }

        public void setTargetMembershipSet( Set<CoreMember> targetMembershipSet )
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
        private CoreMember member;
        private RaftInstance raftInstance;
        private ControlledRenewableTimeoutService timeoutService;
        private RaftLog raftLog;

        public CoreMember member()
        {
            return member;
        }

        public RaftInstance raftInstance()
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

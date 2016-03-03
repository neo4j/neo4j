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
package org.neo4j.coreedge.raft.elections;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.coreedge.raft.DelayedRenewableTimeoutService;
import org.neo4j.coreedge.raft.RaftInstance;
import org.neo4j.coreedge.raft.RaftInstanceBuilder;
import org.neo4j.coreedge.raft.RaftTestNetwork;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.membership.RaftTestGroup;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.server.RaftTestMemberSetBuilder;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.NullLogProvider;

public class Fixture
{
    final Set<RaftTestMember> members = new HashSet<>();
    final Set<RaftInstance<RaftTestMember>> rafts = new HashSet<>();
    final RaftTestNetwork<RaftTestMember> net;
    final List<DelayedRenewableTimeoutService> timeoutServices = new ArrayList<>();

    public Fixture( Set<Long> memberIds, RaftTestNetwork<RaftTestMember> net, long electionTimeout, long heartbeatInterval ) throws Throwable
    {
        this.net = net;

        for ( Long id : memberIds )
        {
            RaftTestMember member = RaftTestMember.member( id );

            RaftTestNetwork.Inbound inbound = net.new Inbound( member );
            RaftTestNetwork.Outbound outbound = net.new Outbound( member );

            members.add( member );

            DelayedRenewableTimeoutService timeoutService = createTimeoutService();

            RaftInstance<RaftTestMember> raftInstance =
                    new RaftInstanceBuilder<>( member, memberIds.size(), RaftTestMemberSetBuilder.INSTANCE )
                            .electionTimeout( electionTimeout )
                            .heartbeatInterval( heartbeatInterval )
                            .inbound( inbound )
                            .outbound( outbound )
                            .timeoutService( timeoutService )
                            .raftLog( new InMemoryRaftLog() )
                            .build();

            rafts.add( raftInstance );
        }
    }

    private DelayedRenewableTimeoutService createTimeoutService() throws Throwable
    {
        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService(
                Clock.SYSTEM_CLOCK, NullLogProvider.getInstance() );

        timeoutServices.add( timeoutService );

        timeoutService.init();
        timeoutService.start();

        return timeoutService;
    }

    public void boot() throws RaftInstance.BootstrapException
    {
        net.start();
        Iterables.first( rafts ).bootstrapWithInitialMembers( new RaftTestGroup( members ) );
    }

    public void teardown() throws InterruptedException
    {
        net.stop();
        for ( DelayedRenewableTimeoutService timeoutService : timeoutServices )
        {
            try
            {
                timeoutService.stop();
            }
            catch ( Throwable e )
            {
                e.printStackTrace();
            }
        }
        for ( RaftInstance<RaftTestMember> raft : rafts )
        {
            raft.logShippingManager().destroy();
        }
    }
}

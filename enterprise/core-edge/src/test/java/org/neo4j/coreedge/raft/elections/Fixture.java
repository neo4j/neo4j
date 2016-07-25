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

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.coreedge.raft.DelayedRenewableTimeoutService;
import org.neo4j.coreedge.raft.RaftInstance;
import org.neo4j.coreedge.raft.RaftInstance.BootstrapException;
import org.neo4j.coreedge.raft.RaftInstanceBuilder;
import org.neo4j.coreedge.raft.RaftStateMachine;
import org.neo4j.coreedge.raft.RaftTestNetwork;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.membership.RaftTestGroup;
import org.neo4j.coreedge.server.MemberId;
import org.neo4j.coreedge.server.RaftTestMemberSetBuilder;
import org.neo4j.function.Predicates;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.NullLogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Fixture
{
    private final Set<MemberId> members = new HashSet<>();
    private final Set<BootstrapWaiter> bootstrapWaiters = new HashSet<>();
    private final List<DelayedRenewableTimeoutService> timeoutServices = new ArrayList<>();
    final Set<RaftInstance> rafts = new HashSet<>();
    final RaftTestNetwork net;

    Fixture( Set<MemberId> memberIds, RaftTestNetwork net, long electionTimeout, long heartbeatInterval )
    {
        this.net = net;

        for ( MemberId member : memberIds )
        {
            RaftTestNetwork.Inbound inbound = net.new Inbound( member );
            RaftTestNetwork.Outbound outbound = net.new Outbound( member );

            members.add( member );

            DelayedRenewableTimeoutService timeoutService = createTimeoutService();

            BootstrapWaiter waiter = new BootstrapWaiter();
            bootstrapWaiters.add( waiter );

            RaftInstance raftInstance =
                    new RaftInstanceBuilder( member, memberIds.size(), RaftTestMemberSetBuilder.INSTANCE )
                            .electionTimeout( electionTimeout )
                            .heartbeatInterval( heartbeatInterval )
                            .inbound( inbound )
                            .outbound( outbound )
                            .timeoutService( timeoutService )
                            .raftLog( new InMemoryRaftLog() )
                            .stateMachine( waiter )
                            .build();

            rafts.add( raftInstance );
        }
    }

    private DelayedRenewableTimeoutService createTimeoutService()
    {
        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService(
                Clock.systemUTC(), NullLogProvider.getInstance() );

        timeoutServices.add( timeoutService );

        timeoutService.init();
        timeoutService.start();

        return timeoutService;
    }

    void boot() throws BootstrapException, TimeoutException, InterruptedException
    {
        net.start();
        Iterables.first( rafts ).bootstrapWithInitialMembers( new RaftTestGroup( members ) );
        awaitBootstrapped();
    }

    public void tearDown()
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
        for ( RaftInstance raft : rafts )
        {
            raft.logShippingManager().stop();
        }
    }

    /**
     * This class simply waits for a single entry to have been committed,
     * which should be the initial member set entry.
     *
     * If all members of the cluster have committed such an entry, it's possible for any member
     * to perform elections. We need to meet this condition before we start disconnecting members.
     */
    private static class BootstrapWaiter implements RaftStateMachine
    {
        private AtomicBoolean bootstrapped = new AtomicBoolean( false );

        @Override
        public void notifyCommitted( long commitIndex )
        {
            if ( commitIndex >= 0 )
            {
                bootstrapped.set( true );
            }
        }

        @Override
        public void notifyNeedFreshSnapshot()
        {
        }

        @Override
        public void downloadSnapshot( MemberId from )
        {
        }

    }

    private void awaitBootstrapped() throws InterruptedException, TimeoutException
    {
        Predicates.await( () -> {
            for ( BootstrapWaiter bootstrapWaiter : bootstrapWaiters )
            {
                if ( !bootstrapWaiter.bootstrapped.get() )
                {
                    return false;
                }
            }
            return true;
        }, 30, SECONDS, 100, MILLISECONDS );
    }
}

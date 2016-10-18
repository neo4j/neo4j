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
package org.neo4j.causalclustering.core.consensus.election;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.RaftMachine.BootstrapException;
import org.neo4j.causalclustering.core.consensus.RaftMachineBuilder;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import org.neo4j.causalclustering.core.consensus.schedule.DelayedRenewableTimeoutService;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import org.neo4j.causalclustering.messaging.TestNetwork;
import org.neo4j.function.Predicates;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.helpers.collection.Iterables.asSet;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class Fixture
{
    private final Set<MemberId> members = new HashSet<>();
    private final Set<BootstrapWaiter> bootstrapWaiters = new HashSet<>();
    private final List<DelayedRenewableTimeoutService> timeoutServices = new ArrayList<>();
    final Set<RaftFixture> rafts = new HashSet<>();
    final TestNetwork net;

    Fixture( Set<MemberId> memberIds, TestNetwork net, long electionTimeout, long heartbeatInterval )
    {
        this.net = net;

        for ( MemberId member : memberIds )
        {
            TestNetwork.Inbound inbound = net.new Inbound( member );
            TestNetwork.Outbound outbound = net.new Outbound( member );

            members.add( member );

            DelayedRenewableTimeoutService timeoutService = createTimeoutService();

            BootstrapWaiter waiter = new BootstrapWaiter();
            bootstrapWaiters.add( waiter );

            InMemoryRaftLog raftLog = new InMemoryRaftLog();
            RaftMachine raftMachine =
                    new RaftMachineBuilder( member, memberIds.size(), RaftTestMemberSetBuilder.INSTANCE )
                            .electionTimeout( electionTimeout )
                            .heartbeatInterval( heartbeatInterval )
                            .inbound( inbound )
                            .outbound( outbound )
                            .timeoutService( timeoutService )
                            .raftLog( raftLog )
                            .commitListener( waiter )
                            .build();

            rafts.add( new RaftFixture(raftMachine, raftLog) );
        }
    }

    private DelayedRenewableTimeoutService createTimeoutService()
    {
        DelayedRenewableTimeoutService timeoutService = new DelayedRenewableTimeoutService(
                Clocks.systemClock(), getInstance() );

        timeoutServices.add( timeoutService );

        timeoutService.init();
        timeoutService.start();

        return timeoutService;
    }

    void boot() throws BootstrapException, TimeoutException, InterruptedException, IOException
    {
        for ( RaftFixture raft : rafts )
        {
            raft.raftLog().append( new RaftLogEntry(0, new MemberIdSet(asSet( members ))) );
            raft.raftMachine().installCoreState( new RaftCoreState( new MembershipEntry( 0, members  ) ) );
        }
        net.start();
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
        for ( RaftFixture raft : rafts )
        {
            raft.raftMachine().logShippingManager().stop();
        }
    }

    /**
     * This class simply waits for a single entry to have been committed,
     * which should be the initial member set entry.
     *
     * If all members of the cluster have committed such an entry, it's possible for any member
     * to perform elections. We need to meet this condition before we start disconnecting members.
     */
    private static class BootstrapWaiter implements RaftMachineBuilder.CommitListener
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

    class RaftFixture
    {

        private final RaftMachine raftMachine;
        private final InMemoryRaftLog raftLog;

        public RaftFixture( RaftMachine raftMachine, InMemoryRaftLog raftLog )
        {
            this.raftMachine = raftMachine;
            this.raftLog = raftLog;
        }

        public RaftMachine raftMachine()
        {
            return raftMachine;
        }

        public InMemoryRaftLog raftLog()
        {
            return raftLog;
        }
    }
}

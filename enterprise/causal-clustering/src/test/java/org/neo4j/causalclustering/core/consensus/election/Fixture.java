/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.election;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.RaftMachineBuilder;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import org.neo4j.causalclustering.messaging.TestNetwork;
import org.neo4j.function.Predicates;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.helpers.collection.Iterables.asSet;

public class Fixture
{
    private final Set<MemberId> members = new HashSet<>();
    private final Set<BootstrapWaiter> bootstrapWaiters = new HashSet<>();
    private final List<TimerService> timerServices = new ArrayList<>();
    private final JobScheduler scheduler = new CentralJobScheduler();
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

            TimerService timerService = createTimerService();

            BootstrapWaiter waiter = new BootstrapWaiter();
            bootstrapWaiters.add( waiter );

            InMemoryRaftLog raftLog = new InMemoryRaftLog();
            RaftMachine raftMachine =
                    new RaftMachineBuilder( member, memberIds.size(), RaftTestMemberSetBuilder.INSTANCE )
                            .electionTimeout( electionTimeout )
                            .heartbeatInterval( heartbeatInterval )
                            .inbound( inbound )
                            .outbound( outbound )
                            .timerService( timerService )
                            .raftLog( raftLog )
                            .commitListener( waiter )
                            .build();

            rafts.add( new RaftFixture( raftMachine, raftLog ) );
        }
    }

    private TimerService createTimerService()
    {
        TimerService timerService = new TimerService( scheduler, NullLogProvider.getInstance() );
        timerServices.add( timerService );
        return timerService;
    }

    void boot() throws Throwable
    {
        scheduler.init();
        scheduler.start();
        for ( RaftFixture raft : rafts )
        {
            raft.raftLog().append( new RaftLogEntry( 0, new MemberIdSet( asSet( members ) ) ) );
            raft.raftMachine().installCoreState( new RaftCoreState( new MembershipEntry( 0, members ) ) );
            raft.raftMachine.postRecoveryActions();
        }
        net.start();
        awaitBootstrapped();
    }

    public void tearDown() throws Throwable
    {
        net.stop();
        for ( RaftFixture raft : rafts )
        {
            raft.raftMachine().logShippingManager().stop();
        }
        scheduler.stop();
        scheduler.shutdown();
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

    private void awaitBootstrapped() throws TimeoutException
    {
        Predicates.await( () ->
        {
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

        RaftFixture( RaftMachine raftMachine, InMemoryRaftLog raftLog )
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

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
package org.neo4j.coreedge.server.core;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.catchup.CatchupServer;
import org.neo4j.coreedge.raft.DelayedRenewableTimeoutService;
import org.neo4j.coreedge.raft.RaftInstance;
import org.neo4j.coreedge.raft.RaftServer;
import org.neo4j.coreedge.raft.membership.MembershipWaiter;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdGeneratorFactory;
import org.neo4j.coreedge.raft.state.CoreState;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CoreServerStartupProcess
{
    public static LifeSupport createLifeSupport( DataSourceManager dataSourceManager,
            ReplicatedIdGeneratorFactory idGeneratorFactory, RaftInstance raft, CoreState coreState,
            RaftServer raftServer, CatchupServer catchupServer,
            DelayedRenewableTimeoutService raftTimeoutService, MembershipWaiter membershipWaiter,
            long joinCatchupTimeout, LogProvider logProvider )
    {
        LifeSupport services = new LifeSupport();
        services.add( dataSourceManager );
        services.add( idGeneratorFactory );
        services.add( coreState );
        services.add( raftServer );
        services.add( catchupServer );
        services.add( raftTimeoutService );
        services.add( new MembershipWaiterLifecycle( membershipWaiter, joinCatchupTimeout, raft, logProvider ) );

        return services;
    }

    private static class MembershipWaiterLifecycle extends LifecycleAdapter
    {
        private final MembershipWaiter membershipWaiter;
        private final Long joinCatchupTimeout;
        private final RaftInstance raft;
        private final Log log;

        private MembershipWaiterLifecycle( MembershipWaiter membershipWaiter, Long joinCatchupTimeout,
                                           RaftInstance raft, LogProvider logProvider )
        {
            this.membershipWaiter = membershipWaiter;
            this.joinCatchupTimeout = joinCatchupTimeout;
            this.raft = raft;
            this.log = logProvider.getLog( getClass() );
        }

        @Override
        public void start() throws Throwable
        {
            CompletableFuture<Boolean> caughtUp = membershipWaiter.waitUntilCaughtUpMember( raft.state() );

            try
            {
                caughtUp.get( joinCatchupTimeout, MILLISECONDS );
            }
            catch ( ExecutionException e )
            {
                log.error( "Server failed to join cluster", e.getCause() );
                throw e.getCause();
            }
            catch ( InterruptedException | TimeoutException e )
            {
                String message =
                        format( "Server failed to join cluster within catchup time limit [%d ms]", joinCatchupTimeout );
                log.error( message, e );
                throw new RuntimeException( message, e );
            }
            finally
            {
                caughtUp.cancel( true );
            }
        }
    }
}

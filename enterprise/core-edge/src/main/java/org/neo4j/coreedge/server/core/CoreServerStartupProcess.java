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
package org.neo4j.coreedge.server.core;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.raft.membership.MembershipWaiter;
import org.neo4j.coreedge.raft.RaftInstance;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.RaftServer;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdGeneratorFactory;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.raft.DelayedRenewableTimeoutService;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifecycleException;

import org.neo4j.coreedge.catchup.CatchupServer;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CoreServerStartupProcess implements Lifecycle
{
    private final LifeSupport services = new LifeSupport();

    public CoreServerStartupProcess( LocalDatabase localDatabase, DataSourceManager dataSourceManager,
                                     ReplicatedIdGeneratorFactory idGeneratorFactory,
                                     RaftInstance<CoreMember> raft, RaftLog raftLog, RaftServer<CoreMember> raftServer,
                                     CatchupServer catchupServer,
                                     DelayedRenewableTimeoutService raftTimeoutService,
                                     MembershipWaiter<CoreMember> membershipWaiter,
                                     long joinCatchupTimeout )
    {
        services.add( new LifecycleAdapter() {
            @Override
            public void start() throws Throwable
            {
                localDatabase.deleteStore();
            }
        });
        services.add( dataSourceManager );
        services.add( idGeneratorFactory );
        services.add( new LifecycleAdapter( ) {
            @Override
            public void start() throws Throwable
            {
                raftLog.replay();
            }
        } );
        services.add( raftServer );
        services.add( raftTimeoutService );
        services.add( catchupServer );
        services.add( new MembershipWaiterLifecycle<>(membershipWaiter, joinCatchupTimeout, raft ) );
    }

    @Override
    public void init() throws LifecycleException
    {
        services.init();
    }

    @Override
    public void start() throws LifecycleException
    {
        services.start();
    }

    @Override
    public void stop() throws LifecycleException
    {
        services.stop();
    }

    @Override
    public void shutdown() throws LifecycleException
    {
        services.shutdown();
    }

    private static class MembershipWaiterLifecycle<MEMBER> extends LifecycleAdapter
    {
        private final MembershipWaiter<MEMBER> membershipWaiter;
        private final Long joinCatchupTimeout;
        private final RaftInstance<MEMBER> raft;

        private MembershipWaiterLifecycle( MembershipWaiter<MEMBER> membershipWaiter, Long joinCatchupTimeout, RaftInstance<MEMBER> raft )
        {
            this.membershipWaiter = membershipWaiter;
            this.joinCatchupTimeout = joinCatchupTimeout;
            this.raft = raft;
        }

        @Override
        public void start() throws Throwable
        {
            CompletableFuture<Boolean> caughtUp = membershipWaiter.waitUntilCaughtUpMember( raft.state() );

            try
            {
                caughtUp.get( joinCatchupTimeout, MILLISECONDS );
            }
            catch ( InterruptedException | ExecutionException | TimeoutException e )
            {
                throw new RuntimeException( format( "Server failed to join cluster within catchup time limit [%d ms]",
                        joinCatchupTimeout ), e );
            }
            finally
            {
                caughtUp.cancel( true );
            }
        }
    }
}

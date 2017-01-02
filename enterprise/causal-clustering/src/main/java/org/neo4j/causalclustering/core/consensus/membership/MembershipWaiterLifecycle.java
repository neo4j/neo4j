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
package org.neo4j.causalclustering.core.consensus.membership;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class MembershipWaiterLifecycle extends LifecycleAdapter
{
    private final MembershipWaiter membershipWaiter;
    private final Long joinCatchupTimeout;
    private final RaftMachine raft;
    private final Log log;

    public MembershipWaiterLifecycle( MembershipWaiter membershipWaiter, Long joinCatchupTimeout,
                                      RaftMachine raft, LogProvider logProvider )
    {
        this.membershipWaiter = membershipWaiter;
        this.joinCatchupTimeout = joinCatchupTimeout;
        this.raft = raft;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        CompletableFuture<Boolean> caughtUp = membershipWaiter.waitUntilCaughtUpMember( raft );

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

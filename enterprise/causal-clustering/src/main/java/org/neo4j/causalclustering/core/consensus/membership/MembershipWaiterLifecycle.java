/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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

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
package org.neo4j.causalclustering.core.consensus.election;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.core.consensus.LeaderListener;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.collection.Iterables;

public class ElectionUtil
{
    private ElectionUtil()
    {
    }

    public static MemberId waitForLeaderAgreement( Iterable<RaftMachine> validRafts, long maxTimeMillis ) throws
            InterruptedException, TimeoutException
    {
        long viewCount = Iterables.count( validRafts );

        Map<MemberId, MemberId> leaderViews = new HashMap<>();
        CompletableFuture<MemberId> futureAgreedLeader = new CompletableFuture<>();

        Collection<Runnable> destructors = new ArrayList<>();
        for ( RaftMachine raft : validRafts )
        {
            destructors.add( leaderViewUpdatingListener( raft, validRafts, leaderViews, viewCount, futureAgreedLeader ) );
        }

        try
        {
            try
            {
                return futureAgreedLeader.get( maxTimeMillis, TimeUnit.MILLISECONDS );
            }
            catch ( ExecutionException e )
            {
                throw new RuntimeException( e );
            }
        }
        finally
        {
            destructors.forEach( Runnable::run );
        }
    }

    private static Runnable leaderViewUpdatingListener( RaftMachine raft, Iterable<RaftMachine>
            validRafts, Map<MemberId,MemberId> leaderViews, long viewCount, CompletableFuture<MemberId>
            futureAgreedLeader )
    {
        LeaderListener listener = newLeader ->
        {
            synchronized ( leaderViews )
            {
                leaderViews.put( raft.identity(), newLeader.memberId() );

                boolean leaderIsValid = false;
                for ( RaftMachine validRaft : validRafts )
                {
                    if ( validRaft.identity().equals( newLeader.memberId() ) )
                    {
                        leaderIsValid = true;
                    }
                }

                if ( newLeader.memberId() != null && leaderIsValid && allAgreeOnLeader( leaderViews, viewCount, newLeader.memberId() ) )
                {
                    futureAgreedLeader.complete( newLeader.memberId() );
                }
            }
        };

        raft.registerListener( listener );
        return () -> raft.unregisterListener( listener );
    }

    private static <T> boolean allAgreeOnLeader( Map<T,T> leaderViews, long viewCount, T leader )
    {
        if ( leaderViews.size() != viewCount )
        {
            return false;
        }

        for ( T leaderView : leaderViews.values() )
        {
            if ( !leader.equals( leaderView ) )
            {
                return false;
            }
        }

        return true;
    }
}

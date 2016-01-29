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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.helpers.collection.FilteringIterable;

/**
 * In this scenario we disconnect the current leader and measure how long time it
 * takes until the remaining members agree on a new leader.
 */
public class DisconnectLeaderScenario
{
    private final Fixture fixture;
    private final long electionTimeout;
    private final List<Long> electionResults = new ArrayList<>();

    public DisconnectLeaderScenario( Fixture fixture, long electionTimeout )
    {
        this.fixture = fixture;
        this.electionTimeout = electionTimeout;
    }

    public void run( long iterations ) throws InterruptedException, TimeoutException
    {
        for ( int i = 0; i < iterations; i++ )
        {
            long electionTime = oneIteration();
            electionResults.add( electionTime );
            Thread.sleep( ThreadLocalRandom.current().nextLong( electionTimeout ) );
        }
    }

    private long oneIteration() throws InterruptedException, TimeoutException
    {
        RaftTestMember oldLeader = ElectionUtil.waitForLeaderAgreement( fixture.rafts, 10 * electionTimeout );
        long startTime = System.currentTimeMillis();

        fixture.net.disconnect( oldLeader );
        RaftTestMember newLeader = ElectionUtil.waitForLeaderAgreement( new FilteringIterable<>( fixture.rafts, raft -> !raft.identity().equals( oldLeader ) ), 10 * electionTimeout );
        assert !newLeader.equals( oldLeader ); // this should be guaranteed by the waitForLeaderAgreement call

        long endTime = System.currentTimeMillis();

        fixture.net.reconnect( oldLeader );

        return endTime - startTime;
    }

    private boolean hadOneOrMoreCollisions( long result )
    {
        /* This is just a simple heuristic to classify the results into colliding and
         * non-colliding groups. It is not entirely accurate and doesn't have to be. */
        return result > (electionTimeout * 2);
    }

    public class Result
    {
        double nonCollidingAverage;
        double collidingAverage;
        double collisionRate;
        long collisionCount;

        @Override
        public String toString()
        {
            return String.format( "Result{nonCollidingAverage=%s, collidingAverage=%s, collisionRate=%s, collisionCount=%d}",
                    nonCollidingAverage, collidingAverage, collisionRate, collisionCount );
        }
    }

    public Result result()
    {
        Result result = new Result();

        long collidingRuns = 0;
        long collidingSum = 0;

        long nonCollidingRuns = 0;
        long nonCollidingSum = 0;

        for ( long electionTime : electionResults )
        {
            if ( hadOneOrMoreCollisions( electionTime ) )
            {
                collidingRuns++;
                collidingSum += electionTime;
            }
            else
            {
                nonCollidingRuns++;
                nonCollidingSum += electionTime;
            }
        }

        result.collidingAverage = collidingSum / (double) collidingRuns;
        result.nonCollidingAverage = nonCollidingSum / (double) nonCollidingRuns;
        result.collisionRate = collidingRuns / (double) electionResults.size();
        result.collisionCount = collidingRuns;

        return result;
    }
}

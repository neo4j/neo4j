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

import org.junit.Test;

import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.coreedge.raft.RaftStateMachine;
import org.neo4j.coreedge.raft.RaftTestNetwork;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.server.edge.CoreServerSelectionStrategy;
import org.neo4j.function.Predicates;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.neo4j.helpers.collection.Iterators.asSet;

/**
 * A test suite that is used for measuring the election performance and
 * guarding against regressions in this area. The outcome assertions are very
 * relaxed so that false positives are avoided in CI and adjustments of the
 * limits should be made by looking at statistics and reasoning about what
 * type of performance should be expected, taking all parameters into account.
 *
 * Major regressions that severely affect the election performance and the
 * ability to perform an election at all should be caught by this test. Very
 * rare false positives should not be used as an indication for increasing the
 * limits.
 */
public class ElectionPerformanceIT
{
    /**
     * This class simply waits for a single entry to have been committed for each member,
     * which should be the initial member set entry, making it possible for every member
     * to perform elections. We need this before we start disconnecting members.
     */
    private class BootstrapWaiter implements RaftStateMachine<RaftTestMember>
    {
        private AtomicLong count = new AtomicLong();

        @Override
        public void notifyCommitted( long commitIndex )
        {
            count.incrementAndGet();
        }

        @Override
        public void notifyNeedFreshSnapshot( RaftTestMember myself, CoreServerSelectionStrategy strategy )
        {
        }

        private void await( long awaitedCount ) throws InterruptedException, TimeoutException
        {
            Predicates.await( () -> count.get() >= awaitedCount, 30, SECONDS, 100, MILLISECONDS );
        }
    }

    @Test
    public void electionPerformance_NormalConditions() throws Throwable
    {
        /* This test runs with with few iterations. Hence it does not have the power to catch
         * regressions efficiently. Its purpose is mainly to run elections using real-world
         * parameters and catch very obvious regressions while not contributing overly much to the
         * regression test suites total runtime. */

        // given parameters
        final long networkLatency = 15L;
        final long electionTimeout = 500L;
        final long heartbeatInterval = 250L;
        final int iterations = 10;

        RaftTestNetwork<RaftTestMember> net = new RaftTestNetwork<>( ( i, o ) -> networkLatency );
        Set<Long> members = asSet( 0L, 1L, 2L );
        BootstrapWaiter bootstrapWaiter = new BootstrapWaiter();
        Fixture fixture = new Fixture( members, net, electionTimeout, heartbeatInterval, bootstrapWaiter );
        DisconnectLeaderScenario scenario = new DisconnectLeaderScenario( fixture, electionTimeout );

        try
        {
            // when running scenario
            fixture.boot();
            bootstrapWaiter.await( members.size() );
            scenario.run( iterations, 10 * electionTimeout );
        }
        finally
        {
            fixture.teardown();
        }

        DisconnectLeaderScenario.Result result = scenario.result();

        /* These bounds have been experimentally established and should have a very low
         * likelihood for false positives without an actual major regression. If this test fails
         * then the recommended action is to run the test manually and interpret the results
         * to guide further action. Perhaps the power of the test has to be improved, but
         * the intention here is not to catch anything but the most major of regressions. */

        assertThat( result.nonCollidingAverage, lessThan( 2.0 * electionTimeout ) );
        if ( result.collisionCount > 3 )
        {
            assertThat( result.collidingAverage, lessThan( 6.0 * electionTimeout ) );
        }
        assertThat( result.timeoutCount, is( 0L ) );
    }

    @Test
    public void electionPerformance_RapidConditions() throws Throwable
    {
        // given parameters
        final long networkLatency = 1L;
        final long electionTimeout = 30L;
        final long heartbeatInterval = 15L;
        final int iterations = 100;

        RaftTestNetwork<RaftTestMember> net = new RaftTestNetwork<>( ( i, o ) -> networkLatency );
        Set<Long> members = asSet( 0L, 1L, 2L );
        BootstrapWaiter bootstrapWaiter = new BootstrapWaiter();
        Fixture fixture = new Fixture( members, net, electionTimeout, heartbeatInterval, bootstrapWaiter );
        DisconnectLeaderScenario scenario = new DisconnectLeaderScenario( fixture, electionTimeout );

        try
        {
            // when running scenario
            fixture.boot();
            bootstrapWaiter.await( members.size() );
            scenario.run( iterations, 10 * electionTimeout );
        }
        finally
        {
            fixture.teardown();
        }

        DisconnectLeaderScenario.Result result = scenario.result();

        /* These bounds have been experimentally established and should have a very low
         * likelihood for false positives without an actual major regression. If this test fails
         * then the recommended action is to run the test manually and interpret the results
         * to guide further action. Perhaps the power of the test has to be improved, but
         * the intention here is not to catch anything but the most major of regressions. */

        assertThat( result.nonCollidingAverage, lessThan( 2.0 * electionTimeout ) );

        // because of the high number of iterations, it is possible to assert on the collision rate
        assertThat( result.collisionRate, lessThan( 0.50d ) );

        if ( result.collisionCount > 10 )
        {
            assertThat( result.collidingAverage, lessThan( 5.0*electionTimeout ) );
        }
        assertThat( result.timeoutCount, lessThanOrEqualTo( 1L ) ); // for GC or whatever reason
    }
}

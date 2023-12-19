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

import org.junit.Ignore;
import org.junit.Test;

import java.util.Set;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.TestNetwork;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
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
    @Ignore( "This belongs better in a benchmarking suite." )
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

        TestNetwork net = new TestNetwork<>( ( i, o ) -> networkLatency );
        Set<MemberId> members = asSet( member( 0 ), member( 1 ), member( 2 ) );
        Fixture fixture = new Fixture( members, net, electionTimeout, heartbeatInterval );
        DisconnectLeaderScenario scenario = new DisconnectLeaderScenario( fixture, electionTimeout );

        try
        {
            // when running scenario
            fixture.boot();
            scenario.run( iterations, 10 * electionTimeout );
        }
        finally
        {
            fixture.tearDown();
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

    @Ignore( "This belongs better in a benchmarking suite." )
    @Test
    public void electionPerformance_RapidConditions() throws Throwable
    {
        // given parameters
        final long networkLatency = 1L;
        final long electionTimeout = 30L;
        final long heartbeatInterval = 15L;
        final int iterations = 100;

        TestNetwork net = new TestNetwork<>( ( i, o ) -> networkLatency );
        Set<MemberId> members = asSet( member( 0 ), member( 1 ), member( 2 ) );
        Fixture fixture = new Fixture( members, net, electionTimeout, heartbeatInterval );
        DisconnectLeaderScenario scenario = new DisconnectLeaderScenario( fixture, electionTimeout );

        try
        {
            // when running scenario
            fixture.boot();
            scenario.run( iterations, 10 * electionTimeout );
        }
        finally
        {
            fixture.tearDown();
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
            assertThat( result.collidingAverage, lessThan( 5.0 * electionTimeout ) );
        }
        assertThat( result.timeoutCount, lessThanOrEqualTo( 1L ) ); // for GC or whatever reason
    }
}

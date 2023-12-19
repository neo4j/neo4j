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
package org.neo4j.causalclustering.messaging.address;

import org.junit.Test;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

public class UnknownAddressMonitorTest
{
    @Test
    public void shouldLogFirstFailure()
    {
        // given
        Log log = mock( Log.class );
        UnknownAddressMonitor logger = new UnknownAddressMonitor( log, testClock(), 100 );

        // when
        MemberId to = member( 0 );
        logger.logAttemptToSendToMemberWithNoKnownAddress( to );

        // then
        verify( log ).info( format( "No address found for %s, probably because the member has been shut down.", to ) );
    }

    private FakeClock testClock()
    {
        return Clocks.fakeClock( 1_000_000, MILLISECONDS );
    }

    @Test
    public void shouldThrottleLogging()
    {
        // given
        Log log = mock( Log.class );
        FakeClock clock = testClock();
        UnknownAddressMonitor logger = new UnknownAddressMonitor( log, clock, 1000 );
        MemberId to = member( 0 );

        // when
        logger.logAttemptToSendToMemberWithNoKnownAddress( to );
        clock.forward( 1, MILLISECONDS );
        logger.logAttemptToSendToMemberWithNoKnownAddress( to );

        // then
        verify( log, times( 1 ) )
                .info( format( "No address found for %s, probably because the member has been shut " + "down.", to ) );
    }

    @Test
    public void shouldResumeLoggingAfterQuietPeriod()
    {
        // given
        Log log = mock( Log.class );
        FakeClock clock = testClock();
        UnknownAddressMonitor logger = new UnknownAddressMonitor( log, clock, 1000 );
        MemberId to = member( 0 );

        // when
        logger.logAttemptToSendToMemberWithNoKnownAddress( to );
        clock.forward( 20001, MILLISECONDS );
        logger.logAttemptToSendToMemberWithNoKnownAddress( to );
        clock.forward( 80001, MILLISECONDS );
        logger.logAttemptToSendToMemberWithNoKnownAddress( to );

        // then
        verify( log, times( 3 ) )
                .info( format( "No address found for %s, probably because the member has been shut " + "down.", to ) );
    }
}

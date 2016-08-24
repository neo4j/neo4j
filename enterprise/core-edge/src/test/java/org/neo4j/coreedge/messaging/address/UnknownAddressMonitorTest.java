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
package org.neo4j.coreedge.messaging.address;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.logging.Log;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.coreedge.identity.RaftTestMember.member;

public class UnknownAddressMonitorTest
{
    @Test
    public void shouldLogFirstFailure() throws Exception
    {
        // given
        Log log = mock( Log.class );
        UnknownAddressMonitor logger = new UnknownAddressMonitor( log, Clocks.fakeClock(), 10000 );

        // when
        logger.logAttemptToSendToMemberWithNoKnownAddress( member( 0 ) );

        // then
        verify( log ).info( anyString(), eq( member( 0 ) ) );
    }

    @Test
    public void shouldThrottleLogging() throws Exception
    {
        // given
        Log log = mock( Log.class );
        FakeClock clock = Clocks.fakeClock();
        UnknownAddressMonitor logger = new UnknownAddressMonitor( log, clock, 10000 );

        // when
        logger.logAttemptToSendToMemberWithNoKnownAddress( member( 0 ) );
        clock.forward( 1000, TimeUnit.MILLISECONDS );
        logger.logAttemptToSendToMemberWithNoKnownAddress( member( 0 ) );

        // then
        verify( log, times( 1 ) ).info( anyString(), eq( member( 0 ) ) );
    }

    @Test
    public void shouldResumeLoggingAfterQuietPeriod() throws Exception
    {
        // given
        Log log = mock( Log.class );
        FakeClock clock = Clocks.fakeClock();
        UnknownAddressMonitor logger = new UnknownAddressMonitor( log, clock, 10000 );

        // when
        logger.logAttemptToSendToMemberWithNoKnownAddress( member( 0 ) );
        clock.forward( 11000, TimeUnit.MILLISECONDS );
        logger.logAttemptToSendToMemberWithNoKnownAddress( member( 0 ) );

        // then
        verify( log, times( 2 ) ).info( anyString(), eq( member( 0 ) ) );
    }
}

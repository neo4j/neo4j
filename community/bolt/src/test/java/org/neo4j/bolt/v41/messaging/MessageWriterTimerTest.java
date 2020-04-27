/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v41.messaging;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.stream.Stream;

import org.neo4j.time.SystemNanoClock;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MessageWriterTimerTest
{
    static Stream<Arguments> clocks()
    {
        return Stream.of(
                // The second clock timestamp should always be later than the first
                of( 100L, 1000L ),
                of( 0L, 0L ),
                of( 0L, Long.MAX_VALUE ),
                of( Long.MAX_VALUE, Long.MAX_VALUE ),
                of( Long.MAX_VALUE, -100L ),
                of( Long.MAX_VALUE, -1000L )
        );
    }

    @ParameterizedTest
    @MethodSource( "clocks" )
    void shouldNeverTimeoutIfKeepAliveIsSetToLongMax( long first, long second ) throws Throwable
    {
        // Given
        var clock = mock( SystemNanoClock.class );
        var timer = new MessageWriterTimer( clock, Duration.ofNanos( Long.MAX_VALUE ) );
        when( clock.nanos() ).thenReturn( first ).thenReturn( second );

        // When
        timer.reset();

        // Then
        assertThat( timer.isTimedOut() ).isFalse();
    }

    @ParameterizedTest
    @MethodSource( "clocks" )
    void shouldAlwaysTimeoutIfKeepAliveIsSetToNegative( long first, long second ) throws Throwable
    {
        // Given
        var clock = mock( SystemNanoClock.class );
        var timer = new MessageWriterTimer( clock, Duration.ofNanos( -500 ) );
        when( clock.nanos() ).thenReturn( first ).thenReturn( second );

        // When
        timer.reset();

        // Then
        assertThat( timer.isTimedOut() ).isTrue();
    }
}

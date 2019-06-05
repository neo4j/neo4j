/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log.pruning;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.logging.AssertableLogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EntryTimespanThresholdTest
{
    private final File file = mock( File.class );
    private final LogFileInformation source = mock( LogFileInformation.class );
    private final long version = 4;
    private Clock clock = Clock.fixed( Instant.ofEpochMilli( 1000 ), ZoneOffset.UTC );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    void shouldReturnFalseWhenTimeIsEqualOrAfterTheLowerLimit() throws IOException
    {
        // given
        final EntryTimespanThreshold threshold = new EntryTimespanThreshold( logProvider, clock, MILLISECONDS, 200 );

        when( source.getFirstStartRecordTimestamp( version ) ).thenReturn( 800L );

        // when
        threshold.init();
        final boolean result = threshold.reached( file, version, source );

        // then
        assertFalse( result );
    }

    @Test
    void shouldReturnReturnWhenTimeIsBeforeTheLowerLimit() throws IOException
    {
        // given
        final EntryTimespanThreshold threshold = new EntryTimespanThreshold( logProvider, clock, MILLISECONDS, 100 );

        when( source.getFirstStartRecordTimestamp( version ) ).thenReturn( 800L );

        // when
        threshold.init();
        final boolean result = threshold.reached( file, version, source );

        // then
        assertTrue( result );
    }

    @Test
    void thresholdNotReachedWhenTheLogCannotBeRead() throws IOException
    {
        // given
        final EntryTimespanThreshold threshold = new EntryTimespanThreshold( logProvider, clock, MILLISECONDS, 100 );

        final IOException ex = new IOException(  );
        when( source.getFirstStartRecordTimestamp( version ) ).thenThrow( ex );

        // when
        threshold.init();
        assertFalse( threshold.reached( file, version, source ) );
        logProvider.rawMessageMatcher().assertContains( "Fail to get timestamp info from transaction log file" );
    }
}

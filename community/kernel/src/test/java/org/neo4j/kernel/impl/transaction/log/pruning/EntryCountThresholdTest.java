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

import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.logging.AssertableLogProvider;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EntryCountThresholdTest
{
    private final LogFileInformation info = mock( LogFileInformation.class );
    private final File file = mock( File.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    void shouldReportThresholdReachedWhenThresholdIsReached() throws Exception
    {
        long version = 10L;

        when( info.getFirstEntryId( version + 1 ) ).thenReturn( 1L );
        when( info.getLastEntryId() ).thenReturn( 2L );

        EntryCountThreshold threshold = createThreshold( 1 );
        boolean reached = threshold.reached( file, version, info );

        assertTrue( reached );
    }

    @Test
    void shouldReportThresholdNotReachedWhenThresholdIsNotReached() throws Exception
    {
        long version = 10L;

        when( info.getFirstEntryId( version ) ).thenReturn( 1L );
        when( info.getFirstEntryId( version + 1 ) ).thenReturn( 1L );

        when( info.getLastEntryId() ).thenReturn( 1L );

        EntryCountThreshold threshold = createThreshold( 1 );

        assertFalse( threshold.reached( file, version, info ) );
    }

    @Test
    void shouldProperlyHandleCaseWithOneEntryPerLogFile() throws Exception
    {
        // Given 2 files with one entry each
        when( info.getFirstEntryId( 1L ) ).thenReturn( 1L );
        when( info.getFirstEntryId( 2L ) ).thenReturn( 2L );
        when( info.getFirstEntryId( 3L ) ).thenReturn( 3L );

        when( info.getLastEntryId() ).thenReturn( 3L );

        // When the threshold is 1 entries
        EntryCountThreshold threshold = createThreshold( 1 );

        // Then the last file should be kept around
        assertFalse( threshold.reached( file, 2L, info ) );
        assertTrue( threshold.reached( file, 1L, info ) );
    }

    @Test
    void shouldWorkWhenCalledMultipleTimesKeeping2Files() throws Exception
    {
        when( info.getFirstEntryId( 1L ) ).thenReturn( 1L );
        when( info.getFirstEntryId( 2L ) ).thenReturn( 5L );
        when( info.getFirstEntryId( 3L ) ).thenReturn( 15L );
        when( info.getFirstEntryId( 4L ) ).thenReturn( 18L );
        when( info.getLastEntryId() ).thenReturn( 18L );

        EntryCountThreshold threshold = createThreshold( 8 );

        assertTrue( threshold.reached( file, 1L, info ) );

        assertFalse( threshold.reached( file, 2L, info ) );

        assertFalse( threshold.reached( file, 3L, info ) );
    }

    @Test
    void shouldWorkWhenCalledMultipleTimesKeeping3Files() throws Exception
    {
        when( info.getFirstEntryId( 1L ) ).thenReturn( 1L );
        when( info.getFirstEntryId( 2L ) ).thenReturn( 5L );
        when( info.getFirstEntryId( 3L ) ).thenReturn( 15L );
        when( info.getFirstEntryId( 4L ) ).thenReturn( 18L );
        when( info.getLastEntryId() ).thenReturn( 18L );

        EntryCountThreshold threshold = createThreshold( 15 );

        assertFalse( threshold.reached( file, 1L, info ) );

        assertFalse( threshold.reached( file, 2L, info ) );

        assertFalse( threshold.reached( file, 3L, info ) );
    }

    @Test
    void shouldWorkWhenCalledMultipleTimesKeeping1FileOnBoundary() throws Exception
    {
        when( info.getFirstEntryId( 1L ) ).thenReturn( 1L );
        when( info.getFirstEntryId( 2L ) ).thenReturn( 5L );
        when( info.getFirstEntryId( 3L ) ).thenReturn( 15L );
        when( info.getFirstEntryId( 4L ) ).thenReturn( 18L );
        when( info.getLastEntryId() ).thenReturn( 18L );

        EntryCountThreshold threshold = createThreshold( 3 );

        assertTrue( threshold.reached( file, 1L, info ) );
        assertTrue( threshold.reached( file, 2L, info ) );
        assertFalse( threshold.reached( file, 3L, info ) );
    }

    @Test
    void shouldSkipEmptyLogsBetweenLogsThatWillBeKept() throws Exception
    {
        // Given
        // 1, 3 and 4 are empty. 2 has 5 transactions, 5 has 8, 6 is the current version
        when( info.getFirstEntryId( 1L ) ).thenReturn( 1L );
        when( info.getFirstEntryId( 2L ) ).thenReturn( 1L );
        when( info.getFirstEntryId( 3L ) ).thenReturn( 5L );
        when( info.getFirstEntryId( 4L ) ).thenReturn( 5L );
        when( info.getFirstEntryId( 5L ) ).thenReturn( 5L );
        when( info.getFirstEntryId( 6L ) ).thenReturn( 13L );
        when( info.getLastEntryId() ).thenReturn( 13L );

        // The threshold is 9, which is one more than what version 5 has, which means 2 should be kept
        EntryCountThreshold threshold = createThreshold( 9 );

        assertFalse( threshold.reached( file, 5L, info ) );
        assertFalse( threshold.reached( file, 4L, info ) );
        assertFalse( threshold.reached( file, 3L, info ) );
        assertFalse( threshold.reached( file, 2L, info ) );
        assertTrue( threshold.reached( file, 1L, info ) );
    }

    @Test
    void shouldDeleteNonEmptyLogThatIsAfterASeriesOfEmptyLogs() throws Exception
    {
        // Given
        // 1, 3 and 4 are empty. 2 has 5 transactions, 5 has 8, 6 is the current version
        when( info.getFirstEntryId( 1L ) ).thenReturn( 1L );
        when( info.getFirstEntryId( 2L ) ).thenReturn( 1L );
        when( info.getFirstEntryId( 3L ) ).thenReturn( 5L );
        when( info.getFirstEntryId( 4L ) ).thenReturn( 5L );
        when( info.getFirstEntryId( 5L ) ).thenReturn( 5L );
        when( info.getFirstEntryId( 6L ) ).thenReturn( 13L );
        when( info.getLastEntryId() ).thenReturn( 13L );

        // The threshold is 8, which is exactly what version 5 has, which means 2 should be deleted
        EntryCountThreshold threshold = createThreshold( 8 );

        assertFalse( threshold.reached( file, 5L, info ) );
        assertTrue( threshold.reached( file, 4L, info ) );
        assertTrue( threshold.reached( file, 3L, info ) );
        assertTrue( threshold.reached( file, 2L, info ) );
        assertTrue( threshold.reached( file, 1L, info ) );
    }

    @Test
    void thresholdNotReachedWhenNextEntryIdNotFound() throws IOException
    {
        when( info.getFirstEntryId( 2L ) ).thenReturn( -1L );
        EntryCountThreshold threshold = createThreshold( 0 );

        assertFalse( threshold.reached( file, 1, info ) );
        logProvider.assertLogStringContains( "Fail to get id of the first entry in the next transaction log file. Requested version: 2" );
    }

    @Test
    void thresholdNotReachedWhenFailToGetNextEntryId() throws IOException
    {
        when( info.getFirstEntryId( 2L ) ).thenThrow( new IOException( "Exception." ) );
        EntryCountThreshold threshold = createThreshold( 0 );

        assertFalse( threshold.reached( file, 1, info ) );
        logProvider.assertLogStringContains( "Error on attempt to get entry ids from transaction log files. Checked version: 1" );
    }

    private EntryCountThreshold createThreshold( int maxTxCount )
    {
        return new EntryCountThreshold( logProvider, maxTxCount );
    }
}

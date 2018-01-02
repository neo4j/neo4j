/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Test;

import java.io.File;

import org.neo4j.kernel.impl.transaction.log.IllegalLogFormatException;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransactionCountThresholdTest
{
    private LogFileInformation info = mock( LogFileInformation.class );
    private File file = mock( File.class );

    @Test
    public void shouldReportThresholdReachedWhenThresholdIsReached() throws Exception
    {
        long version = 10l;

        when( info.getFirstCommittedTxId( version + 1 ) ).thenReturn( 1l );
        when( info.getLastCommittedTxId() ).thenReturn( 2l );

        TransactionCountThreshold threshold = new TransactionCountThreshold( 1 );
        boolean reached = threshold.reached( file, version, info );

        assertTrue( reached );
    }

    @Test
    public void shouldReportThresholdNotReachedWhenThresholdIsNotReached() throws Exception
    {
        long version = 10l;

        when( info.getFirstCommittedTxId( version + 1 ) ).thenReturn( 1l );
        when( info.getLastCommittedTxId() ).thenReturn( 2l );

        TransactionCountThreshold threshold = new TransactionCountThreshold( 2 );
        boolean reached = threshold.reached( file, version, info );

        assertFalse( reached );
    }

    @Test
    public void shouldReturnTrueWhenLogFormatVersionIsOlderThanTheRequiredOne() throws Exception
    {
        long version = 10l;

        when( info.getFirstCommittedTxId( version + 1 ) ).thenThrow( new IllegalLogFormatException( 9l, 8l ) );

        TransactionCountThreshold threshold = new TransactionCountThreshold( 2 );
        boolean reached = threshold.reached( file, version, info );

        assertTrue( reached );
    }

    @Test
    public void shouldThrowExceptionWhenLogFormatVersionIsNewerThanTheRequiredOne() throws Exception
    {
        long version = 10l;

        when( info.getFirstCommittedTxId( version + 1 ) ).thenThrow( new IllegalLogFormatException( 9l, 11l ) );

        TransactionCountThreshold threshold = new TransactionCountThreshold( 2 );
        try
        {
            threshold.reached( file, version, info );
            fail( "should have thrown IllegalLogFormatException" );
        }
        catch ( RuntimeException e )
        {
            assertTrue( e.getCause() instanceof IllegalLogFormatException );
            assertTrue( ((IllegalLogFormatException) e.getCause()).wasNewerLogVersion() );
        }
    }
    
    @Test
    public void shouldWorkWhenCalledMultipleTimesKeeping2Files() throws Exception
    {
        when( info.getFirstCommittedTxId( 1l ) ).thenReturn( 1l );
        when( info.getFirstCommittedTxId( 2l ) ).thenReturn( 5l );
        when( info.getFirstCommittedTxId( 3l ) ).thenReturn( 15l );
        when( info.getFirstCommittedTxId( 4l ) ).thenReturn( 18l );
        when( info.getLastCommittedTxId() ).thenReturn( 18l );

        TransactionCountThreshold threshold = new TransactionCountThreshold( 8 );

        assertTrue( threshold.reached( file, 1l, info ) );

        assertFalse( threshold.reached( file, 2l, info ) );

        assertFalse( threshold.reached( file, 3l, info ) );
    }

    @Test
    public void shouldWorkWhenCalledMultipleTimesKeeping3Files() throws Exception
    {
        when( info.getFirstCommittedTxId( 1l ) ).thenReturn( 1l );
        when( info.getFirstCommittedTxId( 2l ) ).thenReturn( 5l );
        when( info.getFirstCommittedTxId( 3l ) ).thenReturn( 15l );
        when( info.getFirstCommittedTxId( 4l ) ).thenReturn( 18l );
        when( info.getLastCommittedTxId() ).thenReturn( 18l );

        TransactionCountThreshold threshold = new TransactionCountThreshold( 15 );

        assertFalse( threshold.reached( file, 1l, info ) );

        assertFalse( threshold.reached( file, 2l, info ) );

        assertFalse( threshold.reached( file, 3l, info ) );
    }

    @Test
    public void shouldWorkWhenCalledMultipleTimesKeeping1FileOnBoundary() throws Exception
    {
        when( info.getFirstCommittedTxId( 1l ) ).thenReturn( 1l );
        when( info.getFirstCommittedTxId( 2l ) ).thenReturn( 5l );
        when( info.getFirstCommittedTxId( 3l ) ).thenReturn( 15l );
        when( info.getFirstCommittedTxId( 4l ) ).thenReturn( 18l );
        when( info.getLastCommittedTxId() ).thenReturn( 18l );

        TransactionCountThreshold threshold = new TransactionCountThreshold( 3 );

        assertTrue( threshold.reached( file, 1l, info ) );

        assertTrue( threshold.reached( file, 2l, info ) );

        assertFalse( threshold.reached( file, 3l, info ) );
    }

    @Test
    public void shouldSkipEmptyLogsBetweenLogsThatWillBeKept() throws Exception
    {
        // Given
        // 1, 3 and 4 are empty. 2 has 5 transactions, 5 has 8, 6 is the current version
        when( info.getFirstCommittedTxId( 1l ) ).thenReturn( 1l );
        when( info.getFirstCommittedTxId( 2l ) ).thenReturn( 1l );
        when( info.getFirstCommittedTxId( 3l ) ).thenReturn( 5l );
        when( info.getFirstCommittedTxId( 4l ) ).thenReturn( 5l );
        when( info.getFirstCommittedTxId( 5l ) ).thenReturn( 5l );
        when( info.getFirstCommittedTxId( 6l ) ).thenReturn( 13l );
        when( info.getLastCommittedTxId() ).thenReturn( 13l );

        // The threshold is 9, which is one more than what version 5 has, which means 2 should be kept
        TransactionCountThreshold threshold = new TransactionCountThreshold( 9 );

        assertFalse( threshold.reached( file, 5l, info ) );
        assertFalse( threshold.reached( file, 4l, info ) );
        assertFalse( threshold.reached( file, 3l, info ) );
        assertFalse( threshold.reached( file, 2l, info ) );
        assertTrue( threshold.reached( file, 1l, info ) );
    }

    @Test
    public void shouldDeleteNonEmptyLogThatIsAfterASeriesOfEmptyLogs() throws Exception
    {
        // Given
        // 1, 3 and 4 are empty. 2 has 5 transactions, 5 has 8, 6 is the current version
        when( info.getFirstCommittedTxId( 1l ) ).thenReturn( 1l );
        when( info.getFirstCommittedTxId( 2l ) ).thenReturn( 1l );
        when( info.getFirstCommittedTxId( 3l ) ).thenReturn( 5l );
        when( info.getFirstCommittedTxId( 4l ) ).thenReturn( 5l );
        when( info.getFirstCommittedTxId( 5l ) ).thenReturn( 5l );
        when( info.getFirstCommittedTxId( 6l ) ).thenReturn( 13l );
        when( info.getLastCommittedTxId() ).thenReturn( 13l );

        // The threshold is 8, which is exactly what version 5 has, which means 2 should be deleted
        TransactionCountThreshold threshold = new TransactionCountThreshold( 8 );

        assertFalse( threshold.reached( file, 5l, info ) );
        assertTrue( threshold.reached( file, 4l, info ) );
        assertTrue( threshold.reached( file, 3l, info ) );
        assertTrue( threshold.reached( file, 2l, info ) );
        assertTrue( threshold.reached( file, 1l, info ) );
    }
}

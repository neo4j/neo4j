/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework.log.pruning;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Test;
import org.neo4j.kernel.impl.transaction.xaframework.IllegalLogFormatException;
import org.neo4j.kernel.impl.transaction.xaframework.LogFileInformation;

public class TransactionCountThresholdTest
{
    private LogFileInformation info = mock( LogFileInformation.class );
    private File file = mock( File.class );

    @Test
    public void shouldReportThresholdReachedWhenThresholdIsReached() throws Exception
    {
        long version = 10l;

        when( info.getFirstCommittedTxId( version ) ).thenReturn( 1l );
        when( info.getLastCommittedTxId() ).thenReturn( 1l );

        TransactionCountThreshold threshold = new TransactionCountThreshold( 1 );
        boolean reached = threshold.reached( file, version, info );

        assertTrue( reached );
    }

    @Test
    public void shouldReportThresholdNotReachedWhenThresholdIsNotReached() throws Exception
    {
        long version = 10l;

        when( info.getFirstCommittedTxId( version ) ).thenReturn( 1l );
        when( info.getLastCommittedTxId() ).thenReturn( 1l );

        TransactionCountThreshold threshold = new TransactionCountThreshold( 2 );
        boolean reached = threshold.reached( file, version, info );

        assertFalse( reached );
    }

    @Test
    public void shouldReturnTrueWhenLogFormatVersionIsOlderThanTheRequiredOne() throws Exception
    {
        long version = 10l;

        when( info.getFirstCommittedTxId( version ) ).thenThrow( new IllegalLogFormatException( 9l, 8l ) );

        TransactionCountThreshold threshold = new TransactionCountThreshold( 2 );
        boolean reached = threshold.reached( file, version, info );

        assertTrue( reached );
    }

    @Test
    public void shouldThrowExceptionWhenLogFormatVersionIsNewerThanTheRequiredOne() throws Exception
    {
        long version = 10l;

        when( info.getFirstCommittedTxId( version ) ).thenThrow( new IllegalLogFormatException( 9l, 11l ) );

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
}

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
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.FrozenClock;
import org.neo4j.kernel.impl.transaction.log.IllegalLogFormatException;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransactionTimespanThresholdTest
{
    private final File file = mock( File.class );
    private final LogFileInformation source = mock( LogFileInformation.class );
    private final long version = 4;

    @Test
    public void shouldReturnFalseWhenTimeIsEqualOrAfterTheLowerLimit() throws IOException
    {
        // given
        FrozenClock clock = new FrozenClock( 1000l, TimeUnit.MILLISECONDS );
        final TransactionTimespanThreshold threshold =
                new TransactionTimespanThreshold( clock, TimeUnit.MILLISECONDS, 200 );

        when( source.getFirstStartRecordTimestamp( version ) ).thenReturn( 800l );

        // when
        threshold.init();
        final boolean result = threshold.reached( file, version, source );

        // then
        assertFalse( result );
    }

    @Test
    public void shouldReturnReturnWhenTimeIsBeforeTheLowerLimit() throws IOException
    {
        // given
        FrozenClock clock = new FrozenClock( 1000l, TimeUnit.MILLISECONDS );
        final TransactionTimespanThreshold threshold =
                new TransactionTimespanThreshold( clock, TimeUnit.MILLISECONDS, 100 );

        when( source.getFirstStartRecordTimestamp( version ) ).thenReturn( 800l );

        // when
        threshold.init();
        final boolean result = threshold.reached( file, version, source );

        // then
        assertTrue( result );
    }

    @Test
    public void shouldReturnTrueIfTheLogHasAnOlderVersion() throws IOException
    {
        // given
        FrozenClock clock = new FrozenClock( 1000l, TimeUnit.MILLISECONDS );
        final TransactionTimespanThreshold threshold =
                new TransactionTimespanThreshold( clock, TimeUnit.MILLISECONDS, 100 );

        when( source.getFirstStartRecordTimestamp( version ) ).thenThrow( new IllegalLogFormatException( version, 3 ) );

        // when
        threshold.init();
        final boolean result = threshold.reached( file, version, source );

        // then
        assertTrue( result );
    }

    @Test
    public void shouldThrowIfTheLogHasANewerVersion() throws IOException
    {
        // given
        FrozenClock clock = new FrozenClock( 1000l, TimeUnit.MILLISECONDS );
        final TransactionTimespanThreshold threshold =
                new TransactionTimespanThreshold( clock, TimeUnit.MILLISECONDS, 100 );

        final IllegalLogFormatException ex = new IllegalLogFormatException( version, 5 );
        when( source.getFirstStartRecordTimestamp( version ) ).thenThrow( ex );

        // when
        threshold.init();
        try{
            threshold.reached( file, version, source );
            fail("should have thrown");
        } catch (RuntimeException e) {
            // then
            assertEquals( ex, e.getCause() );
        }
    }

    @Test
    public void shouldThrowIfTheLogCannotBeRead() throws IOException
    {
        // given
        FrozenClock clock = new FrozenClock( 1000l, TimeUnit.MILLISECONDS );
        final TransactionTimespanThreshold threshold =
                new TransactionTimespanThreshold( clock, TimeUnit.MILLISECONDS, 100 );

        final IOException ex = new IOException(  );
        when( source.getFirstStartRecordTimestamp( version ) ).thenThrow( ex );

        // when
        threshold.init();
        try{
            threshold.reached( file, version, source );
            fail("should have thrown");
        } catch (RuntimeException e) {
            // then
            assertEquals( ex, e.getCause() );
        }
    }
}

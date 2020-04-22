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

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.PackOutput;
import org.neo4j.bolt.v3.messaging.BoltResponseMessageWriterV3;
import org.neo4j.bolt.v3.messaging.BoltResponseMessageWriterV3Test;
import org.neo4j.logging.internal.NullLogService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BoltResponseMessageWriterV41Test extends BoltResponseMessageWriterV3Test
{

    @Test
    void flushShouldResetTimer() throws Throwable
    {
        // Given
        var delegator = mock( BoltResponseMessageWriterV3.class );
        var timer = mock( MessageWriterTimer.class );
        var writer = newWriter( delegator, timer );

        // When
        writer.flush();

        // Then
        verify( timer ).reset();
    }

    @Test
    void shouldEndRecordFlushIfKeepAliveInvokedDuringWritingARecord() throws Throwable
    {
        // Given
        var delegator = mock( BoltResponseMessageWriterV3.class );
        var timer = timedOutTimer();
        var writer = newWriter( delegator, timer );

        // When
        writer.beginRecord( 0 );
        writer.keepAlive();
        verify( delegator, never() ).flush();

        // Then
        writer.endRecord();
        verify( delegator ).flush();
    }

    @Test
    void shouldOnErrorFlushIfKeepAliveInvokedDuringWritingARecord() throws Throwable
    {
        // Given
        var delegator = mock( BoltResponseMessageWriterV3.class );
        var timer = timedOutTimer();
        var writer = newWriter( delegator, timer );

        // When
        writer.beginRecord( 0 );
        writer.keepAlive();
        verify( delegator, never() ).flush();

        // Then
        writer.onError();
        verify( delegator ).flush();
    }

    @Test
    void shouldWriteKeepAliveAndFlush() throws Throwable
    {
        // Given
        var output = mock( PackOutput.class );
        var delegator = mock( BoltResponseMessageWriterV3.class );
        when( delegator.output() ).thenReturn( output );

        var timer = timedOutTimer();
        var writer = newWriter( delegator, timer );

        // When
        writer.keepAlive();

        // Then
        verify( output ).beginMessage();
        verify( output ).messageSucceeded();
        verify( delegator ).flush();
    }

    @Test
    void initTimerShouldResetTimer() throws Throwable
    {
        // Given
        var delegator = mock( BoltResponseMessageWriterV3.class );
        var timer = mock( MessageWriterTimer.class );
        var writer = newWriter( delegator, timer );

        // When
        writer.initKeepAliveTimer();

        // Then
        verify( timer ).reset();
    }

    protected BoltResponseMessageWriter newWriter( PackOutput output, Neo4jPack.Packer packer )
    {
        var timer = mock( MessageWriterTimer.class );
        return new BoltResponseMessageWriterV41(
                new BoltResponseMessageWriterV3( out -> packer, output, NullLogService.getInstance() ),
                timer );
    }

    protected BoltResponseMessageWriter newWriter( BoltResponseMessageWriterV3 writer, MessageWriterTimer timer )
    {
        return new BoltResponseMessageWriterV41( writer, timer );
    }

    private MessageWriterTimer timedOutTimer()
    {
        var timer = mock( MessageWriterTimer.class );
        when( timer.isTimedOut() ).thenReturn( true );
        return timer;
    }
}

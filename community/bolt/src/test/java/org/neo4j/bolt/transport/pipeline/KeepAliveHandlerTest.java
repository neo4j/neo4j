/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.transport.pipeline;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.bolt.messaging.BoltResponseMessageWriter;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class KeepAliveHandlerTest
{

    private static final long KEEP_ALIVE_TIMEOUT_SECONDS = 1;

    private BoltResponseMessageWriter messageWriter;
    private KeepAliveHandler handler;
    private EmbeddedChannel channel;

    @BeforeEach
    void setupChannel() throws IOException
    {
        messageWriter = mock( BoltResponseMessageWriter.class );
        handler = new KeepAliveHandler( KEEP_ALIVE_TIMEOUT_SECONDS, messageWriter );

        channel = new EmbeddedChannel( handler );
    }

    private void sleepUntilTimeout() throws InterruptedException
    {
        Thread.sleep( KEEP_ALIVE_TIMEOUT_SECONDS + 500 );
    }

    @Test
    void shouldFlushBufferOrSendKeepAliveWhenActive() throws IOException, InterruptedException
    {
        handler.setActive( true );

        sleepUntilTimeout();
        channel.runPendingTasks();

        verify( messageWriter, atLeastOnce() ).flushBufferOrSendKeepAlive();
        verifyNoMoreInteractions( messageWriter );
    }

    @Test
    void shouldIgnoreIdleConnectionsWhenInactive() throws InterruptedException
    {
        sleepUntilTimeout();
        channel.runPendingTasks();

        verifyNoInteractions( messageWriter );
    }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.runtime.BoltConnectionFatality;

class AuthenticationTimeoutHandlerTest {
    private static final Duration TIMEOUT_DURATION = Duration.ofSeconds(1);

    private AuthenticationTimeoutHandler timeoutHandler;

    @BeforeEach
    void prepareChannel() {
        timeoutHandler = new AuthenticationTimeoutHandler(TIMEOUT_DURATION);
    }

    void awaitTimeout() throws InterruptedException {
        Thread.sleep(TIMEOUT_DURATION.toMillis() + 500);
    }

    @Test
    void shouldCloseChannelWhenTimeoutIsExceededByClient() throws InterruptedException {
        var ctx = mock(ChannelHandlerContext.class);
        var channel = mock(Channel.class);

        when(ctx.channel()).thenReturn(channel);
        when(channel.toString()).thenReturn("test");

        var ex = assertThrows(
                BoltConnectionFatality.class,
                () -> timeoutHandler.channelIdle(ctx, IdleStateEvent.READER_IDLE_STATE_EVENT));
        assertEquals(
                "Terminated connection 'test' as the client failed to authenticate within "
                        + TIMEOUT_DURATION.toMillis() + " ms.",
                ex.getMessage());
    }

    @Test
    void shouldCloseChannelWhenTimeoutIsExceededByServer() throws InterruptedException {
        var ctx = mock(ChannelHandlerContext.class);
        var channel = mock(Channel.class);

        when(ctx.channel()).thenReturn(channel);
        when(channel.toString()).thenReturn("test");

        timeoutHandler.setRequestReceived(true);

        var ex = assertThrows(
                BoltConnectionFatality.class,
                () -> timeoutHandler.channelIdle(ctx, IdleStateEvent.FIRST_READER_IDLE_STATE_EVENT));
        assertEquals(
                "Terminated connection '" + channel
                        + "' as the server failed to handle an authentication request within "
                        + TIMEOUT_DURATION.toMillis() + " ms.",
                ex.getMessage());
    }
}

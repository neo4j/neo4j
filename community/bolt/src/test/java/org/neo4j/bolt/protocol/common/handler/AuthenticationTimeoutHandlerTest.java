/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.EventExecutor;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.error.ClientTimeoutException;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;

class AuthenticationTimeoutHandlerTest {
    private static final Duration TIMEOUT_DURATION = Duration.ofSeconds(1);

    private AuthenticationTimeoutHandler timeoutHandler;

    @BeforeEach
    void prepareChannel() {
        timeoutHandler = new AuthenticationTimeoutHandler(TIMEOUT_DURATION);
    }

    @Test
    void shouldCloseChannelWhenTimeoutIsExceededByClient() throws Exception {
        var ctx = mock(ChannelHandlerContext.class);
        var channel = mock(Channel.class);
        var executor = mock(EventExecutor.class);

        ConnectionMockFactory.newFactory().attachToMock(channel);

        when(ctx.executor()).thenReturn(executor);
        when(ctx.channel()).thenReturn(channel);
        when(channel.toString()).thenReturn("test");

        timeoutHandler.handlerAdded(ctx);

        var ex = assertThrows(ClientTimeoutException.class, () -> timeoutHandler.authTimerEnded(ctx));
        assertEquals(
                "Terminated connection 'bolt-test-connection' (test) as the client failed to authenticate within "
                        + TIMEOUT_DURATION.toMillis() + " ms.",
                ex.getMessage());
    }

    @Test
    void shouldCloseChannelWhenTimeoutIsExceededByServer() throws Exception {
        var ctx = mock(ChannelHandlerContext.class);
        var channel = mock(Channel.class);
        var executor = mock(EventExecutor.class);

        ConnectionMockFactory.newFactory().attachToMock(channel);

        when(ctx.executor()).thenReturn(executor);
        when(ctx.channel()).thenReturn(channel);
        when(channel.toString()).thenReturn("test");

        timeoutHandler.handlerAdded(ctx);
        timeoutHandler.setRequestReceived(true);

        var ex = assertThrows(BoltConnectionFatality.class, () -> timeoutHandler.authTimerEnded(ctx));
        assertEquals(
                "Terminated connection 'bolt-test-connection' (test) as the server failed to handle an authentication request within "
                        + TIMEOUT_DURATION.toMillis() + " ms.",
                ex.getMessage());
    }
}

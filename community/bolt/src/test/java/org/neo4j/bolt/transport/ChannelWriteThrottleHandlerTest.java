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
package org.neo4j.bolt.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.embedded.EmbeddedChannel;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.runtime.throttle.ChannelWriteThrottleHandler;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.NullLogProvider;

class ChannelWriteThrottleHandlerTest {
    @Test
    void shouldCloseChannelWhenMaxDurationReached() throws Exception {
        var logProvider = new AssertableLogProvider();
        var channel = new EmbeddedChannel(new ChannelWriteThrottleHandler(10, logProvider));

        // set a low write buffer to trigger `channelWritabilityChanged()` and schedule reaper function.
        DefaultChannelConfig config = (DefaultChannelConfig) channel.config();
        config.setWriteBufferWaterMark(new WriteBufferWaterMark(0, 0));

        var writeFuture = channel.write("Everything is fine!");

        Thread.sleep(100);
        channel.runScheduledPendingTasks();

        Assertions.assertThat(channel.isOpen()).isFalse();

        LogAssertions.assertThat(logProvider)
                .forClass(ChannelWriteThrottleHandler.class)
                .forLevel(AssertableLogProvider.Level.ERROR)
                .containsMessageWithExceptionMatching(
                        "Fatal error occurred when handling a client connection",
                        ex -> ex instanceof TransportThrottleException);
    }

    @Test
    void shouldNotThrowWhenChannelBecomesWritableAgain() throws Exception {
        var throttle = new ChannelWriteThrottleHandler(100, NullLogProvider.getInstance());
        var channel = new EmbeddedChannel(throttle);
        var ctxMock = mock(ChannelHandlerContext.class);
        var channelMock = mock(Channel.class);
        when(ctxMock.channel()).thenReturn(channelMock);
        when(channelMock.isWritable()).thenReturn(true);

        // set a low write buffer to trigger `channelWritabilityChanged()` and schedule reaper function.
        DefaultChannelConfig config = (DefaultChannelConfig) channel.config();
        config.setWriteBufferWaterMark(new WriteBufferWaterMark(0, 0));

        var writeFuture = channel.write("Everything is fine!");

        throttle.channelWritabilityChanged(ctxMock);

        Thread.sleep(200);

        channel.runScheduledPendingTasks();
        assertDoesNotThrow(channel::checkException);
        assertThat(writeFuture.isDone()).isEqualTo(false);
    }
}

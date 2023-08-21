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
package org.neo4j.bolt.runtime.throttle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.logging.LogAssertions.assertThat;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.signal.StateSignal;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.Level;

class ChannelReadThrottleHandlerTest {

    @Test
    public void shouldSwitchAutoRead() {
        var logging = new AssertableLogProvider();
        var channel = new EmbeddedChannel(new ChannelReadThrottleHandler(3, 5, logging));

        assertThat(channel.config().isAutoRead()).isTrue();

        // get the virtual queue to 4 elements - this does not cause any throttling yet as the high water mark has not
        // been passed
        for (var i = 0; i < 4; ++i) {
            channel.writeInbound("Un-throttled #" + i);
            assertThat(channel.config().isAutoRead()).isTrue();
        }

        // make sure that BEGIN (or any other type of message) is ignored by the handler - we only care about
        // END_JOB_PROCESSING as this marks the end of response for a given request
        channel.writeOutbound(StateSignal.BEGIN_JOB_PROCESSING);

        // blow past the high watermark by two messages
        for (var i = 0; i < 2; ++i) {
            channel.writeInbound("Throttled #" + i);
            assertThat(channel.config().isAutoRead()).isFalse();
        }

        // also make sure that the user is notified about this issue in some capacity (this may be stalling their
        // application after all)
        assertThat(logging)
                .forLevel(Level.WARN)
                .forClass(ChannelReadThrottleHandler.class)
                .containsMessageWithArguments(
                        "[%s] Inbound message queue has exceeded high watermark - Disabling message processing",
                        channel.remoteAddress());

        // end two jobs to bring us closer to the low watermark (and past the high watermark) - this should keep the
        // throttle active
        for (var i = 0; i < 2; ++i) {
            channel.writeOutbound(StateSignal.END_JOB_PROCESSING);
            assertThat(channel.config().isAutoRead()).isFalse();
        }

        // blow past the low watermark - this should disable any throttling on the channel as we're now back in the
        // clear
        for (var i = 0; i < 2; ++i) {
            channel.writeOutbound(StateSignal.END_JOB_PROCESSING);
            assertThat(channel.config().isAutoRead()).isTrue();
        }

        // also make sure that the user is notified about this issue in some capacity (this may have been stalling their
        // application after all)
        assertThat(logging)
                .forLevel(Level.INFO)
                .forClass(ChannelReadThrottleHandler.class)
                .containsMessageWithArguments(
                        "[%s] Inbound message queue has reached low watermark - Enabling message processing",
                        channel.remoteAddress());

        // blow past the high watermark again to make sure that reaching the limit once does not stop this functionality
        // from ever working again
        channel.writeInbound(new Object());
        channel.writeInbound(new Object());
        channel.writeInbound(new Object());
        assertThat(channel.config().isAutoRead()).isFalse();
    }
}

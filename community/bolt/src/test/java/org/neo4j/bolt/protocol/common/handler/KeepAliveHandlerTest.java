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

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.signal.StateSignal;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.packstream.signal.FrameSignal;

class KeepAliveHandlerTest {

    private static final long KEEP_ALIVE_TIMEOUT_SECONDS = 1;

    private void sleepUntilTimeout() throws InterruptedException {
        Thread.sleep(KEEP_ALIVE_TIMEOUT_SECONDS + 500);
    }

    @Test
    void shouldSendKeepAliveDuringProcessing() throws InterruptedException {
        var handler = new KeepAliveHandler(false, KEEP_ALIVE_TIMEOUT_SECONDS, NullLogProvider.getInstance());
        var channel = new EmbeddedChannel(handler);

        channel.writeOutbound(StateSignal.BEGIN_JOB_PROCESSING);

        sleepUntilTimeout();
        channel.runPendingTasks();

        var msg = channel.readOutbound();
        assertThat(msg).isEqualTo(StateSignal.BEGIN_JOB_PROCESSING);

        msg = channel.readOutbound();
        assertThat(msg).isEqualTo(FrameSignal.NOOP);
    }

    @Test
    @SuppressWarnings("removal")
    void shouldSendKeepAliveDuringInLegacyMode() throws InterruptedException {
        var handler = new KeepAliveHandler(true, KEEP_ALIVE_TIMEOUT_SECONDS, NullLogProvider.getInstance());
        var channel = new EmbeddedChannel(handler);

        channel.writeOutbound(StateSignal.ENTER_STREAMING);

        sleepUntilTimeout();
        channel.runPendingTasks();

        var msg = channel.readOutbound();
        assertThat(msg).isEqualTo(StateSignal.ENTER_STREAMING);

        msg = channel.readOutbound();
        assertThat(msg).isEqualTo(FrameSignal.NOOP);
    }

    @Test
    void shouldIgnoreIdleConnectionsWhenInactive() throws InterruptedException {
        var handler = new KeepAliveHandler(false, KEEP_ALIVE_TIMEOUT_SECONDS, NullLogProvider.getInstance());
        var channel = new EmbeddedChannel(handler);

        sleepUntilTimeout();
        channel.runPendingTasks();

        var msg = channel.readOutbound();

        assertThat(msg).isNull();
    }

    @Test
    void shouldIgnoreIdleConnectionsWhenInactiveInLegacyMode() throws InterruptedException {
        var handler = new KeepAliveHandler(true, KEEP_ALIVE_TIMEOUT_SECONDS, NullLogProvider.getInstance());
        var channel = new EmbeddedChannel(handler);

        sleepUntilTimeout();
        channel.runPendingTasks();

        var msg = channel.readOutbound();

        assertThat(msg).isNull();
    }
}

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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.util.concurrent.TimeUnit;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;

public class KeepAliveHandler extends IdleStateHandler {
    private final BoltResponseMessageWriter messageWriter;

    // accessed from bolt worker pool
    private volatile boolean active;

    public KeepAliveHandler(long writerIdleTimeSeconds, BoltResponseMessageWriter messageWriter) {
        super(0, writerIdleTimeSeconds, 0, TimeUnit.MILLISECONDS);

        this.messageWriter = messageWriter;
    }

    @Override
    protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
        if (!active) {
            return;
        }

        this.messageWriter.flushBufferOrSendKeepAlive();
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }
}

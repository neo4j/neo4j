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
package org.neo4j.bolt.protocol.common.connector.transport;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.unix.ServerDomainSocketChannel;
import java.util.concurrent.ThreadFactory;
import org.neo4j.annotations.service.ServiceProvider;

/**
 * Provides a transport implementation based on the `KQueue`.
 */
@ServiceProvider
public final class KqueueConnectorTransport implements ConnectorTransport {

    @Override
    public String getName() {
        return "KQueue";
    }

    @Override
    public boolean isAvailable() {
        return KQueue.isAvailable();
    }

    @Override
    public boolean isNative() {
        return true;
    }

    @Override
    public EventLoopGroup createEventLoopGroup(int threadCount, ThreadFactory threadFactory) {
        return new KQueueEventLoopGroup(threadCount, threadFactory);
    }

    @Override
    public Class<? extends ServerSocketChannel> getSocketChannelType() {
        return KQueueServerSocketChannel.class;
    }

    @Override
    public Class<? extends ServerDomainSocketChannel> getDomainSocketChannelType() {
        return KQueueServerDomainSocketChannel.class;
    }
}

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

import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import java.util.concurrent.ThreadFactory;
import org.neo4j.annotations.service.ServiceProvider;

/**
 * Provides a transport implementation based on the Linux `epoll` function.
 */
@ServiceProvider
public final class EpollConnectorTransport implements ConnectorTransport {

    @Override
    public String getName() {
        return "epoll";
    }

    @Override
    public boolean isAvailable() {
        return Epoll.isAvailable();
    }

    @Override
    public boolean isNative() {
        return true;
    }

    @Override
    public EpollEventLoopGroup createEventLoopGroup(int threadCount, ThreadFactory threadFactory) {
        return new EpollEventLoopGroup(threadCount, threadFactory);
    }

    @Override
    public Class<EpollServerSocketChannel> getSocketChannelType() {
        return EpollServerSocketChannel.class;
    }

    @Override
    public Class<EpollServerDomainSocketChannel> getDomainSocketChannelType() {
        return EpollServerDomainSocketChannel.class;
    }
}

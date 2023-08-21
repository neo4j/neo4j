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

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.unix.ServerDomainSocketChannel;
import java.util.concurrent.ThreadFactory;
import org.neo4j.annotations.service.ServiceProvider;

/**
 * Provides a connector transport implementation based on the JDK NIO (New IO) APIs.
 * <p />
 * This implementation should typically be used as a fallback in order to facilitate support for operating systems which
 * lack support for faster network APIs as it is available within all compliant JDK implementations.
 */
@ServiceProvider
public final class NioConnectorTransport implements ConnectorTransport {

    @Override
    public String getName() {
        return "NIO";
    }

    @Override
    public int getPriority() {
        return Integer.MAX_VALUE;
    }

    @Override
    public NioEventLoopGroup createEventLoopGroup(int threadCount, ThreadFactory threadFactory) {
        return new NioEventLoopGroup(threadCount, threadFactory);
    }

    @Override
    public Class<NioServerSocketChannel> getSocketChannelType() {
        return NioServerSocketChannel.class;
    }

    @Override
    public Class<? extends ServerDomainSocketChannel> getDomainSocketChannelType() {
        // netty's JDK implementation does not yet support domain sockets
        return null;
    }

    @Override
    public boolean isNative() {
        return false;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }
}

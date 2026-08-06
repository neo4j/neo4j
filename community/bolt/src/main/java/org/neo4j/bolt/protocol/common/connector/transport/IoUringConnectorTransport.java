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

import io.netty.channel.Channel;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketProtocolFamily;
import io.netty.channel.unix.DomainSocketChannel;
import io.netty.channel.unix.ServerDomainSocketChannel;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringDatagramChannel;
import io.netty.channel.uring.IoUringDomainSocketChannel;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringServerDomainSocketChannel;
import io.netty.channel.uring.IoUringServerSocketChannel;
import io.netty.channel.uring.IoUringSocketChannel;
import org.neo4j.annotations.service.ServiceProvider;

/**
 * Provides a transport implementation based on the Linux `io_uring` function family.
 */
@ServiceProvider
public final class IoUringConnectorTransport implements ConnectorTransport {

    @Override
    public String getName() {
        return "io_uring";
    }

    @Override
    public int getPriority() {
        return -100;
    }

    @Override
    public boolean isAvailable() {
        return IoUring.isAvailable();
    }

    @Override
    public boolean isNative() {
        return true;
    }

    @Override
    public boolean supportsOption(ConnectorOption<?> option) {
        return option == ConnectorOption.TCP_FAST_OPEN || option == ConnectorOption.TCP_FAST_OPEN_CONNECT;
    }

    @Override
    public IoHandlerFactory createIoHandlerFactory() {
        return IoUringIoHandler.newFactory();
    }

    @Override
    public Class<? extends Channel> socketChannelType() {
        return IoUringSocketChannel.class;
    }

    @Override
    public Class<? extends ServerChannel> serverSocketChannelType() {
        return IoUringServerSocketChannel.class;
    }

    @Override
    public Class<? extends DomainSocketChannel> domainSocketChannelType() {
        return IoUringDomainSocketChannel.class;
    }

    @Override
    public Class<? extends ServerDomainSocketChannel> serverDomainSocketChannelType() {
        return IoUringServerDomainSocketChannel.class;
    }

    @Override
    public DatagramChannel createDatagramChannel(SocketProtocolFamily protocolFamily) {
        return new IoUringDatagramChannel(protocolFamily);
    }
}

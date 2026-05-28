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
package org.neo4j.bolt.testing.client;

import io.netty.channel.Channel;
import io.netty.channel.unix.DomainSocketAddress;
import java.net.SocketAddress;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.testing.client.BoltTestConnection.Factory;
import org.neo4j.bolt.testing.messages.BoltWire;

public final class UnixDomainSocketConnection extends AbstractNettyConnection {

    private static final Factory factory = new Factory();

    private final DomainSocketAddress address;

    public UnixDomainSocketConnection(ConnectorTransport transport, BoltWire wire, DomainSocketAddress address) {
        super(transport, wire);

        if (transport.serverDomainSocketChannelType() == null) {
            throw new IllegalStateException(
                    "UNIX domain sockets are not available within the current execution environment");
        }

        this.address = address;
    }

    public static BoltTestConnection.Factory factory() {
        return factory;
    }

    @Override
    protected DomainSocketAddress address() {
        return this.address;
    }

    @Override
    protected Class<? extends Channel> channelType() {
        return this.transport.domainSocketChannelType();
    }

    private static class Factory implements BoltTestConnection.Factory {

        @Override
        public BoltTestConnection create(ConnectorTransport transport, BoltWire wire, SocketAddress address) {
            if (!this.isSupported(transport)) {
                throw new IllegalArgumentException("Cannot initialize unix domain socket connection using transport "
                        + transport.getName() + ": Unsupported");
            }

            if (address instanceof DomainSocketAddress domainSocketAddress) {
                return new UnixDomainSocketConnection(transport, wire, domainSocketAddress);
            }

            throw new IllegalArgumentException("Cannot initialize unix domain socket connection with address of type "
                    + address.getClass().getSimpleName());
        }

        @Override
        public boolean isSupported(ConnectorTransport transport) {
            return transport.domainSocketChannelType() != null;
        }

        @Override
        public String toString() {
            return "Unix Domain Socket";
        }
    }
}
